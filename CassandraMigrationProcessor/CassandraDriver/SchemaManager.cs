using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.CassandraDriver;
/// <summary>
/// Handles schema discovery, creation, and synchronization
/// between source and target Cassandra clusters.
/// </summary>
public static class SchemaManager
{
    private const int SchemaQueryTimeoutMs = 30_000;
    private const int ProbeTimeoutMs = 15_000;
    private const int DefaultMaxRetries = 3;
    private const int RetryBaseDelayMs = 2000;
    private const int ThrottleMaxRetries = 10;

    /// <summary>
    /// Generate and register a CLR mapping per UDT in the given keyspace
    /// on the supplied session. Required so that the driver decodes UDT
    /// cells into typed instances (instead of raw byte[]) and so values
    /// read on one session can be re-bound on another.
    /// </summary>
    public static Task RegisterDynamicUdtMappingsAsync(ISession session, string keyspace)
        => DynamicUdtRegistrar.RegisterAsync(session, keyspace);

    /// <summary>
    /// Synchronises the target schema with the source:
    /// ensure keyspace → check table exists → create or
    /// alter → return source column list.
    /// </summary>
    public static async Task<List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)>>
        SyncSchemaAsync(ISession sourceSession, ISession targetSession,
            string sourceKeyspace, string sourceTable,
            string targetKeyspace, string targetTable)
    {
        await EnsureKeyspaceExistsAsync(targetSession, targetKeyspace);

        await ReplicateUserDefinedTypesAsync(sourceSession, targetSession,
            sourceKeyspace, targetKeyspace);

        await CreateTableFromSourceAsync(sourceSession, targetSession,
            sourceKeyspace, sourceTable, targetKeyspace, targetTable);

        return await GetTableColumnsAsync(sourceSession, sourceKeyspace, sourceTable);
    }

    /// <summary>
    /// Replicate every User-Defined Type from the source keyspace
    /// to the target keyspace.
    /// <para>
    /// <b>Scope:</b> this copies <i>every</i> UDT in the source keyspace,
    /// not just the UDTs referenced by the table currently being migrated.
    /// This is intentional — it keeps the implementation simple, guarantees
    /// nested UDT references resolve, and avoids surprises when subsequent
    /// tables in the same keyspace are added to the job. Customers who want
    /// to copy only a subset of UDTs (or none at all) should pre-create
    /// the schema on the target and run the job with
    /// <see cref="Models.Job.SkipSchemaSync"/> = <c>true</c>.
    /// </para>
    /// <para>
    /// UDTs are created in dependency order (a UDT that references another
    /// UDT in the same keyspace is created after its dependency) and use
    /// <c>CREATE TYPE IF NOT EXISTS</c> so that pre-existing target UDTs
    /// are left alone.
    /// </para>
    /// </summary>
    public static async Task ReplicateUserDefinedTypesAsync(ISession sourceSession, ISession targetSession,
        string sourceKeyspace, string targetKeyspace)
    {
        MigrationUtilities.ValidateCqlIdentifier(sourceKeyspace);
        MigrationUtilities.ValidateCqlIdentifier(targetKeyspace);

        var udts = await GetUserDefinedTypesAsync(sourceSession, sourceKeyspace);
        if (udts.Count == 0) return;

        var ordered = TopologicallySortUdts(udts);

        foreach (var udt in ordered)
        {
            MigrationUtilities.ValidateCqlIdentifier(udt.TypeName);

            var fieldDefs = new List<string>(udt.FieldNames.Count);
            for (int i = 0; i < udt.FieldNames.Count; i++)
            {
                MigrationUtilities.ValidateCqlIdentifier(udt.FieldNames[i]);
                fieldDefs.Add($"\"{udt.FieldNames[i]}\" {udt.FieldTypes[i]}");
            }

            string cql =
                $"CREATE TYPE IF NOT EXISTS \"{targetKeyspace}\".\"{udt.TypeName}\" (" +
                string.Join(", ", fieldDefs) + ")";

            await ExecuteWithTimeoutRetryAsync(() =>
                targetSession.ExecuteAsync(new SimpleStatement(cql)));
        }
    }

    /// <summary>
    /// Read every User-Defined Type defined in the given keyspace
    /// from <c>system_schema.types</c>. Returns type name plus the
    /// field name/type pairs in declaration order.
    /// </summary>
    public static async Task<List<UserDefinedTypeDef>> GetUserDefinedTypesAsync(ISession session, string keyspace)
    {
        var statement = new SimpleStatement(
            "SELECT type_name, field_names, field_types " +
            "FROM system_schema.types WHERE keyspace_name = ?",
            keyspace);
        statement.SetReadTimeoutMillis(SchemaQueryTimeoutMs);

        var resultSet = await ExecuteWithTimeoutRetryAsync(() => session.ExecuteAsync(statement));

        var udts = new List<UserDefinedTypeDef>();
        foreach (var row in resultSet)
        {
            var typeName = row.GetValue<string>("type_name");
            var fieldNames = row.GetValue<IEnumerable<string>>("field_names")?.ToList()
                             ?? new List<string>();
            var fieldTypes = row.GetValue<IEnumerable<string>>("field_types")?.ToList()
                             ?? new List<string>();
            udts.Add(new UserDefinedTypeDef(typeName, fieldNames, fieldTypes));
        }
        return udts;
    }

    /// <summary>
    /// Topologically sort UDTs so that any UDT referenced by another
    /// UDT in the same keyspace appears earlier in the returned list.
    /// References are detected by case-insensitive word-boundary
    /// matching against the field-type text. If a dependency cycle
    /// is detected the remaining UDTs are appended in their original
    /// order (the create attempt will surface the cycle as a Cassandra
    /// error rather than silently dropping any UDT).
    /// </summary>
    internal static List<UserDefinedTypeDef> TopologicallySortUdts(List<UserDefinedTypeDef> udts)
    {
        var byName = new Dictionary<string, UserDefinedTypeDef>(StringComparer.OrdinalIgnoreCase);
        foreach (var u in udts) byName[u.TypeName] = u;

        var dependsOn = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var u in udts)
        {
            var deps = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var ft in u.FieldTypes)
            {
                foreach (var other in udts)
                {
                    if (string.Equals(other.TypeName, u.TypeName, StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (ContainsTypeName(ft, other.TypeName))
                        deps.Add(other.TypeName);
                }
            }
            dependsOn[u.TypeName] = deps;
        }

        var resolved = new List<UserDefinedTypeDef>();
        var resolvedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        bool progress = true;
        while (progress && resolved.Count < udts.Count)
        {
            progress = false;
            foreach (var u in udts)
            {
                if (resolvedNames.Contains(u.TypeName)) continue;
                if (dependsOn[u.TypeName].All(d => resolvedNames.Contains(d)))
                {
                    resolved.Add(u);
                    resolvedNames.Add(u.TypeName);
                    progress = true;
                }
            }
        }
        if (resolved.Count < udts.Count)
        {
            foreach (var u in udts)
                if (!resolvedNames.Contains(u.TypeName))
                    resolved.Add(u);
        }
        return resolved;
    }

    private static bool ContainsTypeName(string fieldType, string typeName)
    {
        if (string.IsNullOrEmpty(fieldType) || string.IsNullOrEmpty(typeName)) return false;
        int idx = 0;
        while (idx <= fieldType.Length - typeName.Length)
        {
            int found = fieldType.IndexOf(typeName, idx, StringComparison.OrdinalIgnoreCase);
            if (found < 0) return false;
            bool leftOk = found == 0 || !IsIdentifierChar(fieldType[found - 1]);
            int after = found + typeName.Length;
            bool rightOk = after >= fieldType.Length || !IsIdentifierChar(fieldType[after]);
            if (leftOk && rightOk) return true;
            idx = found + 1;
        }
        return false;
    }

    private static bool IsIdentifierChar(char c) => char.IsLetterOrDigit(c) || c == '_';

    /// <summary>Source-side definition of a User-Defined Type.</summary>
    public sealed record UserDefinedTypeDef(string TypeName, List<string> FieldNames, List<string> FieldTypes);

    /// <summary>
    /// Check if a keyspace exists.
    /// </summary>
    public static async Task<bool> KeyspaceExistsAsync(ISession session, string keyspace)
    {
        var keyspaces = await CassandraQueries.ListKeyspacesAsync(session);
        return keyspaces.Contains(keyspace, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Ensure target keyspace exists. Creates with
    /// SimpleStrategy replication if missing.
    /// </summary>
    public static async Task EnsureKeyspaceExistsAsync(ISession session, string keyspace, int replicationFactor = 1)
    {
        MigrationUtilities.ValidateCqlIdentifier(keyspace);
        if (!await KeyspaceExistsAsync(session, keyspace))
        {
            await session.ExecuteAsync(new SimpleStatement(
                $"CREATE KEYSPACE IF NOT EXISTS \"{keyspace}\" " +
                $"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replicationFactor}}}"));
        }
    }

    /// <summary>
    /// Check if a table exists and is accessible.
    /// Probes actual data (not just metadata) because
    /// Cosmos DB can return metadata for ghost tables
    /// that 404 on data reads.
    /// </summary>
    public static async Task<bool> TableExistsAsync(ISession session, string keyspace, string table)
    {
        MigrationUtilities.ValidateCqlIdentifier(keyspace);
        MigrationUtilities.ValidateCqlIdentifier(table);
        var tables = await CassandraQueries.ListTablesAsync(session, keyspace);
        if (!tables.Contains(table, StringComparer.OrdinalIgnoreCase))
            return false;

        for (int attempt = 1; attempt <= ThrottleMaxRetries; attempt++)
        {
            try
            {
                var probe = new SimpleStatement(
                    $"SELECT * FROM \"{keyspace}\".\"{table}\" LIMIT 1");
                probe.SetPageSize(1);
                probe.SetAutoPage(false);
                probe.SetReadTimeoutMillis(ProbeTimeoutMs);
                await session.ExecuteAsync(probe);
                return true;
            }
            catch (Exception ex)
            {
                if (ExceptionClassifier.IsThrottle(ex) && attempt < ThrottleMaxRetries)
                {
                    int delaySec = Math.Min(attempt * 3, 30);
                    await Task.Delay(delaySec * 1000);
                    continue;
                }

                if (ExceptionClassifier.IsNotFound(ex))
                    return false;

                throw;
            }
        }
        return false;
    }

    /// <summary>
    /// Get column metadata for a table.
    /// Returns list of (columnName, cqlType, kind,
    /// clusteringOrder, position).
    /// kind = "partition_key", "clustering",
    ///        "regular", "static"
    /// clusteringOrder = "asc", "desc", or "none"
    /// position = ordinal within key group
    /// </summary>
    public static async Task<List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)>>
        GetTableColumnsAsync(ISession session, string keyspace, string table)
    {
        var statement = new SimpleStatement(
            "SELECT column_name, type, kind, clustering_order, position " +
            "FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
            keyspace, table);
        statement.SetReadTimeoutMillis(SchemaQueryTimeoutMs);

        var resultSet = await ExecuteWithTimeoutRetryAsync(() => session.ExecuteAsync(statement));

        return resultSet.Select(r => (Name: r.GetValue<string>("column_name"), Type: r.GetValue<string>("type"),
            Kind: r.GetValue<string>("kind"),
            ClusteringOrder: r.GetValue<string>("clustering_order") ?? "none",
            Position: r.GetValue<int>("position")
        )).ToList();
    }

    /// <summary>
    /// Create a table on the target using the source schema.
    /// </summary>
    public static async Task CreateTableFromSourceAsync(ISession sourceSession, ISession targetSession,
        string sourceKeyspace,
        string sourceTable,
        string targetKeyspace,
        string targetTable)
    {
        MigrationUtilities.ValidateCqlIdentifier(sourceKeyspace);
        MigrationUtilities.ValidateCqlIdentifier(sourceTable);
        MigrationUtilities.ValidateCqlIdentifier(targetKeyspace);
        MigrationUtilities.ValidateCqlIdentifier(targetTable);
        var columns = await GetTableColumnsAsync(sourceSession, sourceKeyspace, sourceTable);
        if (columns.Count == 0)
            throw new InvalidOperationException($"Source table {sourceKeyspace}.{sourceTable} has no columns or does not exist.");

        if (await TableExistsAsync(targetSession, targetKeyspace, targetTable))
        {
            var targetCols = await GetTableColumnsAsync(targetSession, targetKeyspace, targetTable);
            var srcClustering = columns
                .Where(c => c.Kind == "clustering")
                .OrderBy(c => c.Position).ToList();
            var tgtClustering = targetCols
                .Where(c => c.Kind == "clustering")
                .OrderBy(c => c.Position).ToList();

            bool clusteringMismatch = false;
            if (srcClustering.Count != tgtClustering.Count)
                clusteringMismatch = true;
            else
            {
                for (int i = 0; i < srcClustering.Count; i++)
                {
                    if (!srcClustering[i].ClusteringOrder.Equals(tgtClustering[i].ClusteringOrder,
                            StringComparison.OrdinalIgnoreCase))
                    {
                        clusteringMismatch = true;
                        break;
                    }
                }
            }

            if (clusteringMismatch)
            {
                await targetSession.ExecuteAsync(new SimpleStatement(
                    $"DROP TABLE \"{targetKeyspace}\".\"{targetTable}\""));
            }
            else
            {
                await AlterTableAddMissingColumnsAsync(targetSession, targetKeyspace, targetTable, columns,
                    targetCols);
                return;
            }
        }

        var partitionKeys = columns
            .Where(c => c.Kind == "partition_key")
            .OrderBy(c => c.Position)
            .Select(c => $"\"{c.Name}\"")
            .ToList();

        var clusteringKeys = columns
            .Where(c => c.Kind == "clustering")
            .OrderBy(c => c.Position)
            .Select(c => $"\"{c.Name}\"")
            .ToList();

        var colDefs = columns
            .Select(c => c.Kind == "static"
                ? $"  \"{c.Name}\" {c.Type} static"
                : $"  \"{c.Name}\" {c.Type}")
            .ToList();

        string pkClause;
        if (clusteringKeys.Count > 0)
        {
            pkClause = $"({string.Join(", ", partitionKeys)}), {string.Join(", ", clusteringKeys)}";
        }
        else
        {
            pkClause = string.Join(", ", partitionKeys);
        }

        string clusteringOrder = BuildClusteringOrderClause(columns);

        string cql =
            $"CREATE TABLE IF NOT EXISTS \"{targetKeyspace}\".\"{targetTable}\" (\n" +
            $"{string.Join(",\n", colDefs)},\n  PRIMARY KEY ({pkClause})\n)" +
            clusteringOrder;

        await targetSession.ExecuteAsync(new SimpleStatement(cql));
    }

    /// <summary>
    /// Compare source and target columns. For any
    /// regular/static column in source that is missing
    /// from target, execute ALTER TABLE … ADD.
    /// Primary key columns cannot be added after creation.
    /// </summary>
    public static async Task AlterTableAddMissingColumnsAsync(ISession targetSession, string targetKeyspace,
        string targetTable,
        List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> sourceColumns,
        List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> targetColumns)
    {
        MigrationUtilities.ValidateCqlIdentifier(targetKeyspace);
        MigrationUtilities.ValidateCqlIdentifier(targetTable);
        var targetColNames = new HashSet<string>(targetColumns.Select(c => c.Name),
            StringComparer.OrdinalIgnoreCase);

        var missingCols = sourceColumns
            .Where(c => (c.Kind == "regular" || c.Kind == "static") && !targetColNames.Contains(c.Name))
            .ToList();

        if (missingCols.Count == 0) return;

        var failedCols = new List<(string Name, Exception Ex)>();

        foreach (var col in missingCols)
        {
            string staticClause = col.Kind == "static" ? " static" : "";
            string alterCql =
                $"ALTER TABLE \"{targetKeyspace}\".\"{targetTable}\" " +
                $"ADD \"{col.Name}\" {col.Type}{staticClause}";
            try
            {
                await targetSession.ExecuteAsync(new SimpleStatement(alterCql));
            }
            catch (Exception ex)
            {
                failedCols.Add((col.Name, ex));
            }
        }

        if (failedCols.Count > 0)
        {
            var names = string.Join(", ", failedCols.Select(f => f.Name));
            throw new InvalidOperationException($"ALTER TABLE failed for {failedCols.Count} column(s): {names}. Target schema may be incomplete.");
        }
    }

    /// <summary>
    /// Build a WITH CLUSTERING ORDER BY clause from
    /// column metadata. Returns empty string if no
    /// clustering columns or all are default (ASC).
    /// </summary>
    private static string BuildClusteringOrderClause(List<(string Name, string Type,
        string Kind, string ClusteringOrder,
        int Position)> columns)
    {
        var clusteringCols = columns
            .Where(c => c.Kind == "clustering")
            .OrderBy(c => c.Position)
            .ToList();

        if (clusteringCols.Count == 0)
            return string.Empty;

        bool hasNonDefault = clusteringCols
            .Any(c => c.ClusteringOrder.Equals("desc", StringComparison.OrdinalIgnoreCase));
        if (!hasNonDefault)
            return string.Empty;

        var orderParts = clusteringCols
            .Select(c => $"\"{c.Name}\" {c.ClusteringOrder.ToUpperInvariant()}")
            .ToList();

        return $" WITH CLUSTERING ORDER BY ({string.Join(", ", orderParts)})";
    }

    private static async Task<T> ExecuteWithTimeoutRetryAsync<T>(Func<Task<T>> operation,
        int maxRetries = DefaultMaxRetries,
        int baseDelayMs = RetryBaseDelayMs)
    {
        Exception? lastException = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (Exception ex) when (attempt < maxRetries
                && ExceptionClassifier.IsTransient(ex))
            {
                lastException = ex;
                await Task.Delay(attempt * baseDelayMs);
            }
        }
        throw lastException ?? new TimeoutException("Operation timed out after all retries");
    }
}
