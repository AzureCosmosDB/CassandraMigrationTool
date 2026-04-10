using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Context;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.CassandraDriver
{
    /// <summary>
    /// Helper methods for Cassandra data operations:
    /// keyspace/table listing, row counts, feed ranges,
    /// prepared statements, truncation, and retry logic.
    /// Schema DDL operations live in <see cref="SchemaManager"/>.
    /// </summary>
    public static class CassandraQueries
    {
        // Retry/timeout constants
        private const int SchemaQueryTimeoutMs = 30_000;
        private const int DefaultMaxRetries = 3;
        private const int RetryBaseDelayMs = 2000;

        /// <summary>
        /// Execute an async operation with retry on timeout errors.
        /// </summary>
        internal static async Task<T> ExecuteWithTimeoutRetryAsync<T>(Func<Task<T>> operation,
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
        /// <summary>
        /// List all keyspaces (excluding system keyspaces).
        /// </summary>
        public static async Task<List<string>> ListKeyspacesAsync(ISession session)
        {
            var resultSet = await session.ExecuteAsync(new SimpleStatement(
                    "SELECT keyspace_name FROM system_schema.keyspaces"));
            var systemKeyspaces = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "system", "system_auth", "system_distributed",
                "system_schema", "system_traces", "system_views",
                "system_virtual_schema",
                // Cosmos DB internal keyspaces
                "system_cosmos", "system_cosmos_internal"
            };

            return resultSet
                .Select(r => r.GetValue<string>("keyspace_name"))
                .Where(k => !systemKeyspaces.Contains(k))
                .OrderBy(k => k)
                .ToList();
        }

        /// <summary>
        /// List all tables in a keyspace.
        /// </summary>
        public static async Task<List<string>> ListTablesAsync(ISession session, string keyspace)
        {
            var resultSet = await session.ExecuteAsync(new SimpleStatement(
                    "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", keyspace));

            return resultSet
                .Select(r => r.GetValue<string>("table_name"))
                .OrderBy(t => t)
                .ToList();
        }

        /// <summary>
        /// Get the row count of a table. Tries system.size_estimates
        /// first (OSS Cassandra only), falls back to COUNT(*).
        /// Returns -1 if count cannot be determined (progress
        /// will show rows copied without percentage).
        /// </summary>
        public static async Task<long> GetRowCountAsync(ISession session, string keyspace, string table)
        {
            // COUNT(*) with short timeout. For large tables
            // this may time out — migration proceeds without %.
            try
            {
                var statement = new SimpleStatement($"SELECT COUNT(*) FROM \"{keyspace}\".\"{table}\"");
                statement.SetReadTimeoutMillis(SchemaQueryTimeoutMs);
                statement.SetConsistencyLevel(ConsistencyLevel.One);
                var resultSet = await session.ExecuteAsync(statement);
                var row = resultSet.FirstOrDefault();
                if (row != null)
                {
                    long count;
                    try { count = row.GetValue<long>("count"); }
                    catch (ArgumentException) { count = row.GetValue<long>(0); }
                    return count;
                }
            }
            catch (Exception ex)
            {
                // Timeout is expected for large tables — proceed without %
                if (ExceptionClassifier.IsTransient(ex))
                    return -1;
                // Non-transient (auth, schema) — propagate so caller knows
                throw;
            }

            return -1;
        }

        /// <summary>
        /// Truncate a table on the target.
        /// </summary>
        public static async Task TruncateTableAsync(ISession session, string keyspace, string table)
        {
            await session.ExecuteAsync(new SimpleStatement($"TRUNCATE \"{keyspace}\".\"{table}\""));
        }

        /// <summary>
        /// Get feed ranges (physical partitions) for a table
        /// from the system_cosmos.feedranges table.
        /// Returns a list of range JSON strings, one per
        /// physical partition. Returns empty list if the
        /// system table is not available.
        /// </summary>
        public static async Task<List<string>> GetFeedRangesAsync(ISession session, string keyspace, string table)
        {
            var ranges = new List<string>();
            try
            {
                var resultSet = await session.ExecuteAsync(new SimpleStatement(
                        "SELECT range FROM system_cosmos.feedranges WHERE keyspace_name=? AND table_name=?",
                        keyspace, table));
                foreach (var row in resultSet)
                {
                    var range = row.GetValue<string>("range");
                    if (!string.IsNullOrEmpty(range))
                        ranges.Add(range);
                }
            }
            catch (Exception ex)
            {
                MigrationJobContext.AddVerboseLog($"GetFeedRanges error: {ex.Message}");
            }
            return ranges;
        }

        /// <summary>
        /// Build a prepared INSERT statement for a table.
        /// Returns the prepared statement and ordered column
        /// names.
        /// </summary>
        public static async Task<(PreparedStatement Ps, List<string> ColumnNames)>
            PrepareInsertAsync(ISession session, string keyspace, string table,
                List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> columns)
        {
            var colNames = columns
                .Select(c => $"\"{c.Name}\"").ToList();
            var placeholders = columns
                .Select(_ => "?").ToList();

            var cql =
                $"INSERT INTO \"{keyspace}\".\"{table}\" " +
                $"({string.Join(", ", colNames)}) " +
                $"VALUES ({string.Join(", ", placeholders)})";

            var ps = await session.PrepareAsync(cql);
            return (ps, columns.Select(c => c.Name).ToList());
        }

        /// <summary>
        /// Build a prepared INSERT statement for a table.
        /// </summary>
        // Sync required: called from constructor (PageWriter)
        public static (PreparedStatement Ps, List<string> ColumnNames)
            PrepareInsert(ISession session, string keyspace, string table,
                List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> columns)
        {
            return PrepareInsertAsync(session, keyspace, table, columns).GetAwaiter().GetResult();
        }

    }
}
