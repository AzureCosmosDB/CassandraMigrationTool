using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.CassandraDriver;
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
    /// Get feed ranges (physical partitions) for a table
    /// from the system_cosmos.feedranges table.
    /// Returns a list of range JSON strings, one per
    /// physical partition. Returns empty list if the
    /// system table is not available.
    /// </summary>
    public static async Task<List<string>> GetFeedRangesAsync(ISession session, string keyspace, string table,
        Action<string> verboseLog = null)
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
            verboseLog?.Invoke($"GetFeedRanges error: {ex.Message}");
        }
        return ranges;
    }

    /// <summary>
    /// True when the table is a counter table — i.e. it has at least one
    /// CQL counter column. Cassandra forbids mixing counter and
    /// non-counter regular columns in the same table, so a single
    /// counter column implies every non-PK column is a counter and the
    /// write path must use UPDATE c = c + ? instead of INSERT.
    /// </summary>
    public static bool IsCounterTable(
        IEnumerable<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> columns)
        => columns.Any(IsCounterColumn);

    /// <summary>
    /// Build a prepared INSERT for a non-counter table:
    /// <c>INSERT INTO ks.t (...) VALUES (...)</c>. The returned
    /// ColumnNames are in bind-parameter order (which, for INSERT,
    /// equals the source column order). Callers <b>must not</b> invoke
    /// this for counter tables — Cassandra rejects INSERT against
    /// counter columns; use <see cref="PrepareCounterUpdateAsync"/>.
    /// </summary>
    public static async Task<(PreparedStatement Ps, List<string> ColumnNames)>
        PrepareInsertAsync(ISession session, string keyspace, string table,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> columns)
    {
        if (IsCounterTable(columns))
            throw new InvalidOperationException(
                $"PrepareInsertAsync called on counter table {keyspace}.{table}; " +
                "use PrepareCounterUpdateAsync instead.");

        var colNames = columns.Select(c => $"\"{c.Name}\"").ToList();
        var placeholders = columns.Select(_ => "?").ToList();

        var cql =
            $"INSERT INTO \"{keyspace}\".\"{table}\" " +
            $"({string.Join(", ", colNames)}) " +
            $"VALUES ({string.Join(", ", placeholders)})";

        var bindOrder = columns.Select(c => c.Name).ToList();
        var ps = await session.PrepareAsync(cql);
        return (ps, bindOrder);
    }

    /// <summary>
    /// Build a prepared UPDATE for a counter table:
    /// <c>UPDATE ks.t SET c = c + ?, ... WHERE pk = ? AND ck = ?</c>.
    /// Bind order is counter columns first (in schema order), then the
    /// partition-key + clustering columns in key order. The
    /// <c>CounterColumns</c> list is returned in that same leading-bind
    /// order, so callers can use its length as the counter-bind prefix
    /// length for read-modify-write logic.
    /// </summary>
    public static async Task<(PreparedStatement Ps, List<string> BindOrder, IReadOnlyList<string> CounterColumns)>
        PrepareCounterUpdateAsync(ISession session, string keyspace, string table,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> columns)
    {
        var counterCols = columns.Where(IsCounterColumn).ToList();
        if (counterCols.Count == 0)
            throw new InvalidOperationException(
                $"PrepareCounterUpdateAsync called on non-counter table {keyspace}.{table}; " +
                "use PrepareInsertAsync instead.");

        var keyCols = columns
            .Where(c => c.Kind == "partition_key" || c.Kind == "clustering")
            .OrderBy(c => c.Kind == "partition_key" ? 0 : 1)
            .ThenBy(c => c.Position)
            .ToList();

        var setClause = string.Join(", ",
            counterCols.Select(c => $"\"{c.Name}\" = \"{c.Name}\" + ?"));
        var whereClause = string.Join(" AND ",
            keyCols.Select(c => $"\"{c.Name}\" = ?"));

        var cql =
            $"UPDATE \"{keyspace}\".\"{table}\" " +
            $"SET {setClause} WHERE {whereClause}";

        var bindOrder = counterCols.Select(c => c.Name)
            .Concat(keyCols.Select(c => c.Name))
            .ToList();
        var counterColumnNames = counterCols.Select(c => c.Name).ToList();

        var ps = await session.PrepareAsync(cql);
        return (ps, bindOrder, counterColumnNames);
    }

    private static bool IsCounterColumn(
        (string Name, string Type, string Kind, string ClusteringOrder, int Position) c)
    {
        return string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase);
    }
}
