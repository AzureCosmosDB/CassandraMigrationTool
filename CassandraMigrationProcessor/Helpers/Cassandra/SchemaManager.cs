using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Helpers.Cassandra
{
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

            await CreateTableFromSourceAsync(sourceSession, targetSession,
                sourceKeyspace, sourceTable, targetKeyspace, targetTable);

            return await GetTableColumnsAsync(sourceSession, sourceKeyspace, sourceTable);
        }

        /// <summary>
        /// Check if a keyspace exists.
        /// </summary>
        public static async Task<bool> KeyspaceExistsAsync(ISession session, string keyspace)
        {
            var keyspaces = await CassandraHelper.ListKeyspacesAsync(session);
            return keyspaces.Contains(keyspace, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Ensure target keyspace exists. Creates with
        /// SimpleStrategy replication if missing.
        /// </summary>
        public static async Task EnsureKeyspaceExistsAsync(ISession session, string keyspace, int replicationFactor = 1)
        {
            if (!await KeyspaceExistsAsync(session, keyspace))
            {
                await session.ExecuteAsync(new SimpleStatement(
                    $"CREATE KEYSPACE IF NOT EXISTS \"{keyspace}\" " +
                    $"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replicationFactor}}}"));
            }
        }

        /// <summary>
        /// Ensure target keyspace exists. Creates with
        /// SimpleStrategy replication if missing.
        /// </summary>
        public static void EnsureKeyspaceExists(ISession session, string keyspace, int replicationFactor = 1)
        {
            EnsureKeyspaceExistsAsync(session, keyspace, replicationFactor).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Check if a table exists and is accessible.
        /// Probes actual data (not just metadata) because
        /// Cosmos DB can return metadata for ghost tables
        /// that 404 on data reads.
        /// </summary>
        public static async Task<bool> TableExistsAsync(ISession session, string keyspace, string table)
        {
            var tables = await CassandraHelper.ListTablesAsync(session, keyspace);
            if (!tables.Contains(table, StringComparer.OrdinalIgnoreCase))
                return false;

            for (int attempt = 1; attempt <= ThrottleMaxRetries; attempt++)
            {
                try
                {
                    var probe = new SimpleStatement(
                        $"SELECT * FROM \"{keyspace}\".\"{table}\" WHERE COSMOS_CHANGEFEED_FROM_START() = true");
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
        /// Check if a table exists in the given keyspace.
        /// </summary>
        public static bool TableExists(ISession session, string keyspace, string table)
        {
            return TableExistsAsync(session, keyspace, table).GetAwaiter().GetResult();
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
        /// Get column metadata for a table.
        /// </summary>
        public static List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)>
            GetTableColumns(ISession session, string keyspace, string table)
        {
            return GetTableColumnsAsync(session, keyspace, table).GetAwaiter().GetResult();
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
        /// Create a table on the target using the source schema.
        /// </summary>
        public static void CreateTableFromSource(ISession sourceSession, ISession targetSession, string sourceKeyspace,
            string sourceTable,
            string targetKeyspace,
            string targetTable)
        {
            CreateTableFromSourceAsync(sourceSession, targetSession, sourceKeyspace, sourceTable,
                targetKeyspace, targetTable).GetAwaiter().GetResult();
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
}
