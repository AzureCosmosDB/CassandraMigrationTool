using Cassandra;
using CassandraMigrationProcessor.Context;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Helpers.Cassandra
{
    /// <summary>
    /// Helper methods for Cassandra schema discovery,
    /// row counts, and table operations.
    /// </summary>
    public static class CassandraHelper
    {
        /// <summary>
        /// List all keyspaces (excluding system keyspaces).
        /// </summary>
        public static async Task<List<string>> ListKeyspacesAsync(ISession session)
        {
            var rs = await session.ExecuteAsync(
                new SimpleStatement(
                    "SELECT keyspace_name FROM system_schema.keyspaces"))
                .ConfigureAwait(false);
            var systemKeyspaces = new HashSet<string>(
                StringComparer.OrdinalIgnoreCase)
            {
                "system", "system_auth", "system_distributed",
                "system_schema", "system_traces", "system_views",
                "system_virtual_schema",
                // Cosmos DB internal keyspaces
                "system_cosmos", "system_cosmos_internal"
            };

            return rs
                .Select(r => r.GetValue<string>("keyspace_name"))
                .Where(k => !systemKeyspaces.Contains(k))
                .OrderBy(k => k)
                .ToList();
        }

        /// <summary>
        /// List all keyspaces (excluding system keyspaces).
        /// </summary>
        public static List<string> ListKeyspaces(ISession session)
        {
            return ListKeyspacesAsync(session).GetAwaiter().GetResult();
        }

        /// <summary>
        /// List all tables in a keyspace.
        /// </summary>
        public static async Task<List<string>> ListTablesAsync(
            ISession session, string keyspace)
        {
            var rs = await session.ExecuteAsync(
                new SimpleStatement(
                    "SELECT table_name FROM system_schema.tables " +
                    "WHERE keyspace_name = ?", keyspace))
                .ConfigureAwait(false);

            return rs
                .Select(r => r.GetValue<string>("table_name"))
                .OrderBy(t => t)
                .ToList();
        }

        /// <summary>
        /// List all tables in a keyspace.
        /// </summary>
        public static List<string> ListTables(
            ISession session, string keyspace)
        {
            return ListTablesAsync(session, keyspace).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get the row count of a table. Tries system.size_estimates
        /// first (OSS Cassandra only), falls back to COUNT(*).
        /// Returns -1 if count cannot be determined (progress
        /// will show rows copied without percentage).
        /// </summary>
        public static async Task<long> GetRowCountAsync(
            ISession session,
            string keyspace,
            string table)
        {
            // 1) Try system.size_estimates (works on OSS Cassandra
            //    and Azure MI, but NOT on Cosmos DB Cassandra API)
            try
            {
                var estStmt = new SimpleStatement(
                    "SELECT mean_partition_size, partitions_count " +
                    "FROM system.size_estimates " +
                    "WHERE keyspace_name = ? AND table_name = ?",
                    keyspace, table);
                estStmt.SetReadTimeoutMillis(10_000);
                var estRs = await session.ExecuteAsync(estStmt)
                    .ConfigureAwait(false);
                long totalPartitions = 0;
                foreach (var row in estRs)
                {
                    totalPartitions += row.GetValue<long>(
                        "partitions_count");
                }
                if (totalPartitions > 0)
                {
                    Console.WriteLine(
                        $"  RowCount from size_estimates: " +
                        $"~{totalPartitions}");
                    return totalPartitions;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"  size_estimates not available: " +
                    $"{ex.GetType().Name}");
            }

            // 2) Try COUNT(*) with short timeout (30s).
            //    For large Cosmos DB tables this will time out —
            //    that's expected; migration proceeds without %.
            try
            {
                var stmt = new SimpleStatement(
                    $"SELECT COUNT(*) FROM " +
                    $"\"{keyspace}\".\"{table}\"");
                stmt.SetReadTimeoutMillis(30_000); // 30s max
                stmt.SetConsistencyLevel(ConsistencyLevel.One);
                var rs = await session.ExecuteAsync(stmt)
                    .ConfigureAwait(false);
                var row = rs.FirstOrDefault();
                if (row != null)
                {
                    long count;
                    try { count = row.GetValue<long>("count"); }
                    catch (ArgumentException)
                    { count = row.GetValue<long>(0); }

                    Console.WriteLine(
                        $"  RowCount from COUNT(*): {count}");
                    return count;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"  COUNT(*) failed (expected for large " +
                    $"tables): {ex.GetType().Name}");
            }

            Console.WriteLine(
                $"  RowCount unavailable — progress will " +
                $"show rows copied without percentage");
            return -1;
        }

        /// <summary>
        /// Get the row count of a table. Tries system
        /// size_estimates first (fast, approximate), then
        /// falls back to COUNT(*) with a short timeout.
        /// </summary>
        public static long GetRowCount(
            ISession session,
            string keyspace,
            string table)
        {
            return GetRowCountAsync(session, keyspace, table).GetAwaiter().GetResult();
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
        public static async Task<List<(string Name, string Type,
            string Kind, string ClusteringOrder, int Position)>>
            GetTableColumnsAsync(
                ISession session,
                string keyspace,
                string table)
        {
            var stmt = new SimpleStatement(
                "SELECT column_name, type, kind, " +
                "clustering_order, position " +
                "FROM system_schema.columns " +
                "WHERE keyspace_name = ? " +
                "AND table_name = ?", keyspace, table);
            stmt.SetReadTimeoutMillis(30_000);

            RowSet rs = null!;
            for (int attempt = 1; attempt <= 3; attempt++)
            {
                try
                {
                    rs = await session.ExecuteAsync(stmt)
                        .ConfigureAwait(false);
                    break;
                }
                catch (Exception ex) when (
                    attempt < 3 &&
                    (ex is TimeoutException
                     || ex.GetType().Name.Contains("Timeout")
                     || ex.InnerException is TimeoutException))
                {
                    await Task.Delay(attempt * 2000)
                        .ConfigureAwait(false);
                }
            }

            return rs.Select(r => (
                Name: r.GetValue<string>("column_name"),
                Type: r.GetValue<string>("type"),
                Kind: r.GetValue<string>("kind"),
                ClusteringOrder: r.GetValue<string>(
                    "clustering_order") ?? "none",
                Position: r.GetValue<int>("position")
            )).ToList();
        }

        /// <summary>
        /// Get column metadata for a table.
        /// </summary>
        public static List<(string Name, string Type,
            string Kind, string ClusteringOrder, int Position)>
            GetTableColumns(
                ISession session,
                string keyspace,
                string table)
        {
            return GetTableColumnsAsync(session, keyspace, table).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Build a WITH CLUSTERING ORDER BY clause from
        /// column metadata. Returns empty string if no
        /// clustering columns or all are default (ASC).
        /// </summary>
        private static string BuildClusteringOrderClause(
            List<(string Name, string Type,
                string Kind, string ClusteringOrder,
                int Position)> columns)
        {
            var clusteringCols = columns
                .Where(c => c.Kind == "clustering")
                .OrderBy(c => c.Position)
                .ToList();

            if (clusteringCols.Count == 0)
                return string.Empty;

            // Only add clause if any clustering column
            // has DESC order (ASC is the default)
            bool hasNonDefault = clusteringCols
                .Any(c => c.ClusteringOrder
                    .Equals("desc",
                        StringComparison.OrdinalIgnoreCase));
            if (!hasNonDefault)
                return string.Empty;

            var orderParts = clusteringCols
                .Select(c =>
                    $"\"{c.Name}\" " +
                    $"{c.ClusteringOrder.ToUpperInvariant()}")
                .ToList();

            return " WITH CLUSTERING ORDER BY " +
                $"({string.Join(", ", orderParts)})";
        }

        /// <summary>
        /// Get the CREATE TABLE statement for a table.
        /// </summary>
        public static string GetCreateTableCql(
            ISession session,
            string keyspace,
            string table)
        {
            var columns = GetTableColumns(session, keyspace, table);
            if (columns.Count == 0)
                return string.Empty;

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
                .Select(c =>
                    c.Kind == "static"
                        ? $"  \"{c.Name}\" {c.Type} static"
                        : $"  \"{c.Name}\" {c.Type}")
                .ToList();

            string pkClause;
            if (clusteringKeys.Count > 0)
            {
                pkClause =
                    $"({string.Join(", ", partitionKeys)}), " +
                    $"{string.Join(", ", clusteringKeys)}";
            }
            else
            {
                pkClause = string.Join(", ", partitionKeys);
            }

            string clusteringOrder =
                BuildClusteringOrderClause(columns);

            return
                $"CREATE TABLE IF NOT EXISTS " +
                $"\"{keyspace}\".\"{table}\" (\n" +
                $"{string.Join(",\n", colDefs)},\n" +
                $"  PRIMARY KEY ({pkClause})\n)" +
                clusteringOrder;
        }

        /// <summary>
        /// Check if a table exists and is accessible.
        /// Probes actual data (not just metadata) because
        /// Cosmos DB can return metadata for ghost tables
        /// that 404 on data reads.
        /// </summary>
        public static async Task<bool> TableExistsAsync(
            ISession session,
            string keyspace,
            string table)
        {
            // First quick metadata check
            var tables = await ListTablesAsync(session, keyspace)
                .ConfigureAwait(false);
            if (!tables.Contains(
                table, StringComparer.OrdinalIgnoreCase))
                return false;

            // Probe actual data read with retry for 429s
            for (int attempt = 1; attempt <= 10; attempt++)
            {
                try
                {
                    var probe = new SimpleStatement(
                        $"SELECT * FROM \"{keyspace}\".\"{table}\"" +
                        " WHERE COSMOS_CHANGEFEED_FROM_START() = true");
                    probe.SetPageSize(1);
                    probe.SetAutoPage(false);
                    probe.SetReadTimeoutMillis(15_000);
                    await session.ExecuteAsync(probe)
                        .ConfigureAwait(false);
                    return true;
                }
                catch (Exception ex)
                {
                    bool isThrottle = ex.Message?.Contains("429") == true
                        || ex.Message?.Contains("rate", StringComparison.OrdinalIgnoreCase) == true
                        || ex.Message?.Contains("TooMany", StringComparison.OrdinalIgnoreCase) == true;

                    if (isThrottle && attempt < 10)
                    {
                        int delaySec = Math.Min(attempt * 3, 30);
                        Console.WriteLine(
                            $"  TableExists: {keyspace}.{table}" +
                            $" probe throttled (attempt {attempt}/10)," +
                            $" retrying in {delaySec}s...");
                        await Task.Delay(delaySec * 1000)
                            .ConfigureAwait(false);
                        continue;
                    }

                    Console.WriteLine(
                        $"  TableExists: {keyspace}.{table}" +
                        $" probe failed: {ex.GetType().Name}: {ex.Message}");
                    return false;
                }
            }
            return false;
        }

        /// <summary>
        /// Check if a table exists in the given keyspace.
        /// </summary>
        public static bool TableExists(
            ISession session,
            string keyspace,
            string table)
        {
            return TableExistsAsync(session, keyspace, table).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Check if a keyspace exists.
        /// </summary>
        public static async Task<bool> KeyspaceExistsAsync(
            ISession session, string keyspace)
        {
            var keyspaces = await ListKeyspacesAsync(session)
                .ConfigureAwait(false);
            return keyspaces.Contains(
                keyspace, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Check if a keyspace exists.
        /// </summary>
        public static bool KeyspaceExists(
            ISession session, string keyspace)
        {
            return KeyspaceExistsAsync(session, keyspace).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Ensure target keyspace exists. Creates with
        /// SimpleStrategy replication if missing.
        /// </summary>
        public static async Task EnsureKeyspaceExistsAsync(
            ISession session,
            string keyspace,
            int replicationFactor = 1)
        {
            if (!await KeyspaceExistsAsync(session, keyspace)
                .ConfigureAwait(false))
            {
                await session.ExecuteAsync(
                    new SimpleStatement(
                        $"CREATE KEYSPACE IF NOT EXISTS \"{keyspace}\" " +
                        $"WITH replication = " +
                        $"{{'class': 'SimpleStrategy', " +
                        $"'replication_factor': {replicationFactor}}}"))
                    .ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Ensure target keyspace exists. Creates with
        /// SimpleStrategy replication if missing.
        /// </summary>
        public static void EnsureKeyspaceExists(
            ISession session,
            string keyspace,
            int replicationFactor = 1)
        {
            EnsureKeyspaceExistsAsync(session, keyspace, replicationFactor).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Create a table on the target using the source schema.
        /// </summary>
        public static async Task CreateTableFromSourceAsync(
            ISession sourceSession,
            ISession targetSession,
            string sourceKeyspace,
            string sourceTable,
            string targetKeyspace,
            string targetTable)
        {
            var columns = await GetTableColumnsAsync(
                sourceSession, sourceKeyspace, sourceTable)
                .ConfigureAwait(false);
            if (columns.Count == 0)
                throw new InvalidOperationException(
                    $"Source table {sourceKeyspace}.{sourceTable} " +
                    $"has no columns or does not exist.");

            if (await TableExistsAsync(targetSession,
                targetKeyspace, targetTable)
                .ConfigureAwait(false))
            {
                var targetCols = await GetTableColumnsAsync(
                    targetSession, targetKeyspace, targetTable)
                    .ConfigureAwait(false);
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
                        if (!srcClustering[i].ClusteringOrder
                            .Equals(tgtClustering[i]
                                .ClusteringOrder,
                                StringComparison
                                    .OrdinalIgnoreCase))
                        {
                            clusteringMismatch = true;
                            break;
                        }
                    }
                }

                if (clusteringMismatch)
                {
                    Console.WriteLine(
                        $"  Clustering mismatch on " +
                        $"{targetKeyspace}.{targetTable}" +
                        $" — dropping and recreating.");
                    await targetSession.ExecuteAsync(
                        new SimpleStatement(
                            $"DROP TABLE " +
                            $"\"{targetKeyspace}\"" +
                            $".\"{targetTable}\""))
                        .ConfigureAwait(false);
                }
                else
                {
                    await AlterTableAddMissingColumnsAsync(
                        targetSession,
                        targetKeyspace,
                        targetTable,
                        columns,
                        targetCols)
                        .ConfigureAwait(false);
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
                .Select(c =>
                    c.Kind == "static"
                        ? $"  \"{c.Name}\" {c.Type} static"
                        : $"  \"{c.Name}\" {c.Type}")
                .ToList();

            string pkClause;
            if (clusteringKeys.Count > 0)
            {
                pkClause =
                    $"({string.Join(", ", partitionKeys)}), " +
                    $"{string.Join(", ", clusteringKeys)}";
            }
            else
            {
                pkClause = string.Join(", ", partitionKeys);
            }

            string clusteringOrder =
                BuildClusteringOrderClause(columns);

            string cql =
                $"CREATE TABLE IF NOT EXISTS " +
                $"\"{targetKeyspace}\".\"{targetTable}\" (\n" +
                $"{string.Join(",\n", colDefs)},\n" +
                $"  PRIMARY KEY ({pkClause})\n)" +
                clusteringOrder;

            Console.WriteLine(
                $"  CreateTableFromSource CQL:\n  {cql}");
            await targetSession.ExecuteAsync(
                new SimpleStatement(cql))
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Create a table on the target using the source schema.
        /// </summary>
        public static void CreateTableFromSource(
            ISession sourceSession,
            ISession targetSession,
            string sourceKeyspace,
            string sourceTable,
            string targetKeyspace,
            string targetTable)
        {
            CreateTableFromSourceAsync(sourceSession, targetSession,
                sourceKeyspace, sourceTable,
                targetKeyspace, targetTable).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Compare source and target columns. For any
        /// regular/static column in source that is missing
        /// from target, execute ALTER TABLE … ADD.
        /// Primary key columns cannot be added after creation.
        /// </summary>
        public static async Task AlterTableAddMissingColumnsAsync(
            ISession targetSession,
            string targetKeyspace,
            string targetTable,
            List<(string Name, string Type,
                string Kind, string ClusteringOrder,
                int Position)> sourceColumns,
            List<(string Name, string Type,
                string Kind, string ClusteringOrder,
                int Position)> targetColumns)
        {
            var targetColNames = new HashSet<string>(
                targetColumns.Select(c => c.Name),
                StringComparer.OrdinalIgnoreCase);

            var missingCols = sourceColumns
                .Where(c => (c.Kind == "regular" ||
                             c.Kind == "static") &&
                            !targetColNames.Contains(c.Name))
                .ToList();

            if (missingCols.Count == 0)
            {
                Console.WriteLine(
                    $"  {targetKeyspace}.{targetTable}" +
                    $" — schema up-to-date (no missing cols)");
                return;
            }

            var failedCols = new List<(string Name, Exception Ex)>();

            foreach (var col in missingCols)
            {
                string staticClause =
                    col.Kind == "static" ? " static" : "";
                string alterCql =
                    $"ALTER TABLE " +
                    $"\"{targetKeyspace}\".\"{targetTable}\" " +
                    $"ADD \"{col.Name}\" {col.Type}{staticClause}";
                Console.WriteLine(
                    $"  ALTER TABLE ADD: {col.Name} " +
                    $"{col.Type}{staticClause}");
                try
                {
                    await targetSession.ExecuteAsync(
                        new SimpleStatement(alterCql))
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(
                        $"  ALTER TABLE ADD failed for " +
                        $"{col.Name}: {ex.Message}");
                    failedCols.Add((col.Name, ex));
                }
            }

            if (failedCols.Count > 0)
            {
                var names = string.Join(", ",
                    failedCols.Select(f => f.Name));
                throw new InvalidOperationException(
                    $"ALTER TABLE failed for " +
                    $"{failedCols.Count} column(s): {names}. " +
                    $"Target schema may be incomplete.");
            }

            Console.WriteLine(
                $"  Added {missingCols.Count} missing column(s)" +
                $" to {targetKeyspace}.{targetTable}");
        }

        /// <summary>
        /// Compare source and target columns. For any
        /// regular/static column in source that is missing
        /// from target, execute ALTER TABLE … ADD.
        /// </summary>
        public static void AlterTableAddMissingColumns(
            ISession targetSession,
            string targetKeyspace,
            string targetTable,
            List<(string Name, string Type,
                string Kind, string ClusteringOrder,
                int Position)> sourceColumns,
            List<(string Name, string Type,
                string Kind, string ClusteringOrder,
                int Position)> targetColumns)
        {
            AlterTableAddMissingColumnsAsync(targetSession,
                targetKeyspace, targetTable,
                sourceColumns, targetColumns).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Truncate a table on the target.
        /// </summary>
        public static async Task TruncateTableAsync(
            ISession session,
            string keyspace,
            string table)
        {
            await session.ExecuteAsync(
                new SimpleStatement(
                    $"TRUNCATE \"{keyspace}\".\"{table}\""))
                .ConfigureAwait(false);
        }

        /// <summary>
        /// Truncate a table on the target.
        /// </summary>
        public static void TruncateTable(
            ISession session,
            string keyspace,
            string table)
        {
            TruncateTableAsync(session, keyspace, table).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Get feed ranges (physical partitions) for a table
        /// from the system_cosmos.feedranges table.
        /// Returns a list of range JSON strings, one per
        /// physical partition. Returns empty list if the
        /// system table is not available.
        /// </summary>
        public static async Task<List<string>> GetFeedRangesAsync(
            ISession session,
            string keyspace,
            string table)
        {
            var ranges = new List<string>();
            try
            {
                var rs = await session.ExecuteAsync(
                    new SimpleStatement(
                        "SELECT range FROM system_cosmos.feedranges " +
                        "WHERE keyspace_name=? " +
                        "AND table_name=?", keyspace, table))
                    .ConfigureAwait(false);
                foreach (var row in rs)
                {
                    var range = row.GetValue<string>("range");
                    if (!string.IsNullOrEmpty(range))
                        ranges.Add(range);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"  GetFeedRanges: {ex.GetType().Name}: " +
                    $"{ex.Message}");
                MigrationJobContext.AddVerboseLog(
                    $"GetFeedRanges error: {ex.Message}");
            }
            return ranges;
        }

        /// <summary>
        /// Get feed ranges (physical partitions) for a table
        /// from the system_cosmos.feedranges table.
        /// </summary>
        public static List<string> GetFeedRanges(
            ISession session,
            string keyspace,
            string table)
        {
            return GetFeedRangesAsync(session, keyspace, table).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Build a prepared INSERT statement for a table.
        /// Returns the prepared statement and ordered column
        /// names.
        /// </summary>
        public static async Task<(PreparedStatement Ps, List<string> ColumnNames)>
            PrepareInsertAsync(
                ISession session,
                string keyspace,
                string table,
                List<(string Name, string Type,
                    string Kind, string ClusteringOrder,
                    int Position)> columns)
        {
            var colNames = columns
                .Select(c => $"\"{c.Name}\"").ToList();
            var placeholders = columns
                .Select(_ => "?").ToList();

            var cql =
                $"INSERT INTO \"{keyspace}\".\"{table}\" " +
                $"({string.Join(", ", colNames)}) " +
                $"VALUES ({string.Join(", ", placeholders)})";

            var ps = await session.PrepareAsync(cql)
                .ConfigureAwait(false);
            return (ps, columns.Select(c => c.Name).ToList());
        }

        /// <summary>
        /// Build a prepared INSERT statement for a table.
        /// </summary>
        public static (PreparedStatement Ps, List<string> ColumnNames)
            PrepareInsert(
                ISession session,
                string keyspace,
                string table,
                List<(string Name, string Type,
                    string Kind, string ClusteringOrder,
                    int Position)> columns)
        {
            return PrepareInsertAsync(session, keyspace, table, columns).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Build a prepared DELETE statement for a table,
        /// using primary key columns (partition + clustering).
        /// Used by FFCF change feed to replicate deletes.
        /// </summary>
        public static (PreparedStatement Ps, List<string> PkColumnNames)
            PrepareDelete(
                ISession session,
                string keyspace,
                string table,
                List<(string Name, string Type,
                    string Kind, string ClusteringOrder,
                    int Position)> columns)
        {
            var pkCols = columns
                .Where(c => c.Kind == "partition_key"
                         || c.Kind == "clustering")
                .ToList();

            if (pkCols.Count == 0)
                throw new InvalidOperationException(
                    $"Table {keyspace}.{table} has no primary key columns");

            var whereClauses = pkCols
                .Select(c => $"\"{c.Name}\" = ?")
                .ToList();

            var cql =
                $"DELETE FROM \"{keyspace}\".\"{table}\" " +
                $"WHERE {string.Join(" AND ", whereClauses)}";

            var ps = session.Prepare(cql);
            return (ps, pkCols.Select(c => c.Name).ToList());
        }
    }
}
