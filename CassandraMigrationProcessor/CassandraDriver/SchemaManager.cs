using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.CassandraDriver;
/// <summary>
/// Handles schema discovery, creation, and synchronization
/// between source and target Cassandra clusters.
/// </summary>
public static class SchemaManager
{
    private const int ProbeTimeoutMs = 15_000;
    private const int ThrottleMaxRetries = 10;

    /// <summary>
    /// Synchronises the target schema with the source:
    /// ensure keyspace → check table exists → create or
    /// alter → return source column list.
    /// </summary>
    public static async Task
        SyncSchemaAsync(ISession sourceSession, ISession targetSession,
            string sourceKeyspace, string sourceTable,
            string targetKeyspace, string targetTable,
            MigrationLog? log = null)
    {
        await EnsureKeyspaceExistsAsync(targetSession, targetKeyspace, log: log);

        // Discover source columns first so that UDT replication can be
        // scoped to only the UDTs actually referenced by this table.
        var sourceColumns = await GetTableColumnsAsync(sourceSession, sourceKeyspace, sourceTable);

        var allUdts = await GetUserDefinedTypesAsync(sourceSession, sourceKeyspace);
        var requiredUdts = FilterUdtsReferencedByTable(allUdts, sourceColumns.Select(c => c.Type));
        if (requiredUdts.Count > 0)
        {
            await ReplicateUserDefinedTypesAsync(sourceSession, targetSession,
                sourceKeyspace, targetKeyspace, requiredUdts, log);
        }

        await CreateTableFromSourceAsync(sourceSession, targetSession,
            sourceKeyspace, sourceTable, targetKeyspace, targetTable, log);
    }

    /// <summary>
    /// Replicate the supplied User-Defined Types (or every UDT in the source
    /// keyspace if <paramref name="udtsToReplicate"/> is <c>null</c>) from the
    /// source keyspace to the target keyspace.
    /// <para>
    /// UDTs are created in dependency order (a UDT that references another
    /// UDT in the same keyspace is created after its dependency) and use
    /// <c>CREATE TYPE IF NOT EXISTS</c> so that pre-existing target UDTs
    /// are left alone.
    /// </para>
    /// <para>
    /// Callers that need to bypass UDT replication entirely can run the job
    /// with <see cref="Models.Job.SkipSchemaSync"/> = <c>true</c> and
    /// pre-provision the target schema themselves.
    /// </para>
    /// </summary>
    public static async Task ReplicateUserDefinedTypesAsync(ISession sourceSession, ISession targetSession,
        string sourceKeyspace, string targetKeyspace,
        IReadOnlyList<UserDefinedTypeDef>? udtsToReplicate = null,
        MigrationLog? log = null)
    {
        CqlIdentifier.Validate(sourceKeyspace);
        CqlIdentifier.Validate(targetKeyspace);

        udtsToReplicate ??= await GetUserDefinedTypesAsync(sourceSession, sourceKeyspace);
        if (udtsToReplicate.Count == 0) return;

        var ordered = TopologicallySortUdts(udtsToReplicate.ToList());

        foreach (var udt in ordered)
        {
            CqlIdentifier.Validate(udt.TypeName);

            var fieldDefs = new List<string>(udt.FieldNames.Count);
            for (int i = 0; i < udt.FieldNames.Count; i++)
            {
                CqlIdentifier.Validate(udt.FieldNames[i]);
                fieldDefs.Add($"\"{udt.FieldNames[i]}\" {udt.FieldTypes[i]}");
            }

            string cql =
                $"CREATE TYPE IF NOT EXISTS \"{targetKeyspace}\".\"{udt.TypeName}\" (" +
                string.Join(", ", fieldDefs) + ")";

            log?.WriteLine($"DDL on target: {cql}", LogType.Info);
            await RetryExecutor.ExecuteWithTimeoutRetryAsync(() =>
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
        statement.SetReadTimeoutMillis(MigrationDefaults.SchemaQueryTimeoutMs);

        var resultSet = await RetryExecutor.ExecuteWithTimeoutRetryAsync(() => session.ExecuteAsync(statement));

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
    /// Reduce <paramref name="allUdts"/> to the subset transitively
    /// referenced by <paramref name="tableColumnTypes"/>.
    /// <para>
    /// A UDT is included if its name appears (with CQL identifier
    /// boundaries) in any column type, or in the field types of another
    /// UDT that is itself included. The matcher correctly handles nested
    /// forms such as <c>frozen&lt;list&lt;frozen&lt;my_udt&gt;&gt;&gt;</c>,
    /// <c>map&lt;text, frozen&lt;my_udt&gt;&gt;</c> and
    /// <c>tuple&lt;…, my_udt, …&gt;</c>.
    /// </para>
    /// </summary>
    public static List<UserDefinedTypeDef> FilterUdtsReferencedByTable(
        IReadOnlyList<UserDefinedTypeDef> allUdts,
        IEnumerable<string> tableColumnTypes)
    {
        if (allUdts == null || allUdts.Count == 0) return new List<UserDefinedTypeDef>();

        var byName = new Dictionary<string, UserDefinedTypeDef>(StringComparer.OrdinalIgnoreCase);
        foreach (var u in allUdts) byName[u.TypeName] = u;

        var reached = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var queue = new Queue<string>();

        foreach (var columnType in tableColumnTypes ?? Enumerable.Empty<string>())
        {
            if (string.IsNullOrEmpty(columnType)) continue;
            foreach (var u in allUdts)
            {
                if (ContainsTypeName(columnType, u.TypeName) && reached.Add(u.TypeName))
                    queue.Enqueue(u.TypeName);
            }
        }

        while (queue.Count > 0)
        {
            var name = queue.Dequeue();
            if (!byName.TryGetValue(name, out var udt)) continue;
            foreach (var ft in udt.FieldTypes)
            {
                foreach (var other in allUdts)
                {
                    if (string.Equals(other.TypeName, name, StringComparison.OrdinalIgnoreCase)) continue;
                    if (ContainsTypeName(ft, other.TypeName) && reached.Add(other.TypeName))
                        queue.Enqueue(other.TypeName);
                }
            }
        }

        return allUdts.Where(u => reached.Contains(u.TypeName)).ToList();
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
    /// Ensure target keyspace exists. Creates with SimpleStrategy
    /// replication (RF=1) if missing. Refuses to auto-create on
    /// multi-datacenter clusters: SimpleStrategy is unsafe across DCs
    /// and an operator-chosen NetworkTopologyStrategy is required.
    /// In that case, the caller must pre-create the keyspace with the
    /// desired per-DC replication factors before running the job.
    /// </summary>
    public static async Task EnsureKeyspaceExistsAsync(ISession session, string keyspace,
        int replicationFactor = 1, MigrationLog? log = null)
    {
        CqlIdentifier.Validate(keyspace);
        if (await KeyspaceExistsAsync(session, keyspace))
        {
            log?.WriteLine($"Target keyspace \"{keyspace}\" already exists; skipping schema mirror for keyspace.",
                LogType.Info);
            return;
        }

        var dataCenters = await GetTargetDataCentersAsync(session);
        if (dataCenters.Count > 1)
        {
            string dcList = string.Join(", ", dataCenters.OrderBy(d => d, StringComparer.Ordinal));
            string msg =
                $"Refusing to auto-create keyspace \"{keyspace}\": target cluster has multiple " +
                $"datacenters ({dcList}). Auto-create uses SimpleStrategy(RF={replicationFactor}) which " +
                "is unsafe on multi-DC clusters (writes may not reach all DCs and reads may miss replicas). " +
                "Please pre-create the keyspace with an explicit NetworkTopologyStrategy and per-DC " +
                "replication factor, e.g.: CREATE KEYSPACE \"" + keyspace + "\" WITH replication = " +
                "{'class': 'NetworkTopologyStrategy', '<dc-name>': <rf>, ...};";
            log?.WriteLine(msg, LogType.Error);
            throw new InvalidOperationException(msg);
        }

        string cql =
            $"CREATE KEYSPACE IF NOT EXISTS \"{keyspace}\" " +
            $"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replicationFactor}}}";
        log?.WriteLine($"DDL on target: {cql}", LogType.Info);
        await session.ExecuteAsync(new SimpleStatement(cql));
    }

    /// <summary>
    /// Discover distinct datacenter names from the target cluster
    /// (system.local + system.peers). Returns an empty set if the
    /// query is not supported by the target (e.g. Cosmos DB Cassandra
    /// API), in which case the caller falls through to single-DC
    /// behaviour.
    /// </summary>
    private static async Task<HashSet<string>> GetTargetDataCentersAsync(ISession session)
    {
        var dcs = new HashSet<string>(StringComparer.Ordinal);
        try
        {
            var local = await session.ExecuteAsync(new SimpleStatement(
                "SELECT data_center FROM system.local"));
            foreach (var row in local)
            {
                var dc = row.GetValue<string?>("data_center");
                if (!string.IsNullOrWhiteSpace(dc)) dcs.Add(dc);
            }

            var peers = await session.ExecuteAsync(new SimpleStatement(
                "SELECT data_center FROM system.peers"));
            foreach (var row in peers)
            {
                var dc = row.GetValue<string?>("data_center");
                if (!string.IsNullOrWhiteSpace(dc)) dcs.Add(dc);
            }
        }
        catch
        {
            // Targets that do not expose system.local/system.peers
            // (or reject the query) fall through to single-DC
            // behaviour; the caller will still emit SimpleStrategy.
            return new HashSet<string>(StringComparer.Ordinal);
        }
        return dcs;
    }

    /// <summary>
    /// Check if a table exists and is accessible.
    /// Probes actual data (not just metadata) because
    /// Cosmos DB can return metadata for ghost tables
    /// that 404 on data reads.
    /// </summary>
    public static async Task<bool> TableExistsAsync(ISession session, string keyspace, string table)
    {
        CqlIdentifier.Validate(keyspace);
        CqlIdentifier.Validate(table);
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
    public static async Task<List<CassandraColumn>>
        GetTableColumnsAsync(ISession session, string keyspace, string table)
    {
        var statement = new SimpleStatement(
            "SELECT column_name, type, kind, clustering_order, position " +
            "FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
            keyspace, table);
        statement.SetReadTimeoutMillis(MigrationDefaults.SchemaQueryTimeoutMs);

        var resultSet = await RetryExecutor.ExecuteWithTimeoutRetryAsync(() => session.ExecuteAsync(statement));

        return resultSet.Select(r => new CassandraColumn(
            Name: r.GetValue<string>("column_name"),
            Type: r.GetValue<string>("type"),
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
        string targetTable,
        MigrationLog? log = null)
    {
        CqlIdentifier.Validate(sourceKeyspace);
        CqlIdentifier.Validate(sourceTable);
        CqlIdentifier.Validate(targetKeyspace);
        CqlIdentifier.Validate(targetTable);
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
                string dropCql = $"DROP TABLE \"{targetKeyspace}\".\"{targetTable}\"";
                log?.WriteLine($"DDL on target: {dropCql}", LogType.Info);
                await targetSession.ExecuteAsync(new SimpleStatement(dropCql));
            }
            else
            {
                await AlterTableAddMissingColumnsAsync(targetSession, targetKeyspace, targetTable, columns,
                    targetCols, log);
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

        log?.WriteLine($"DDL on target: {cql}", LogType.Info);
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
        List<CassandraColumn> sourceColumns,
        List<CassandraColumn> targetColumns,
        MigrationLog? log = null)
    {
        CqlIdentifier.Validate(targetKeyspace);
        CqlIdentifier.Validate(targetTable);
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
            log?.WriteLine($"DDL on target: {alterCql}", LogType.Info);
            try
            {
                await targetSession.ExecuteAsync(new SimpleStatement(alterCql));
            }
            catch (Exception ex)
            {
                log?.WriteLine($"ALTER TABLE failed for column \"{col.Name}\": {ex.GetType().Name}: {ex.Message}",
                    LogType.Error);
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
    private static string BuildClusteringOrderClause(List<CassandraColumn> columns)
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
}
