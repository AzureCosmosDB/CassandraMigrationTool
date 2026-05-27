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
    /// Build a prepared write statement for a table.
    /// For regular tables this is INSERT INTO ... VALUES (...).
    /// For counter tables (any column with CQL type "counter")
    /// Cassandra forbids INSERT, so we emit
    /// UPDATE ... SET c = c + ?, ... WHERE pk = ? AND ck = ?
    /// instead. The returned ColumnNames are in the bind-parameter
    /// order, which differs from the source column order for counter
    /// tables — callers must look up row values by name (or reorder)
    /// rather than relying on positional alignment with the source
    /// schema.
    /// </summary>
    public static async Task<(PreparedStatement Ps, List<string> ColumnNames, bool IsCounterTable, IReadOnlyList<string> CounterColumns)>
        PrepareInsertAsync(ISession session, string keyspace, string table,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> columns)
    {
        bool isCounterTable = columns.Any(IsCounterColumn);
        IReadOnlyList<string> counterColumnNames;

        string cql;
        List<string> bindOrder;
        if (isCounterTable)
        {
            var counterCols = columns.Where(IsCounterColumn).ToList();
            var keyCols = columns
                .Where(c => c.Kind == "partition_key" || c.Kind == "clustering")
                .OrderBy(c => c.Kind == "partition_key" ? 0 : 1)
                .ThenBy(c => c.Position)
                .ToList();

            var setClause = string.Join(", ",
                counterCols.Select(c => $"\"{c.Name}\" = \"{c.Name}\" + ?"));
            var whereClause = string.Join(" AND ",
                keyCols.Select(c => $"\"{c.Name}\" = ?"));

            cql =
                $"UPDATE \"{keyspace}\".\"{table}\" " +
                $"SET {setClause} WHERE {whereClause}";

            bindOrder = counterCols.Select(c => c.Name)
                .Concat(keyCols.Select(c => c.Name))
                .ToList();
            counterColumnNames = counterCols.Select(c => c.Name).ToList();
        }
        else
        {
            var colNames = columns
                .Select(c => $"\"{c.Name}\"").ToList();
            var placeholders = columns
                .Select(_ => "?").ToList();

            cql =
                $"INSERT INTO \"{keyspace}\".\"{table}\" " +
                $"({string.Join(", ", colNames)}) " +
                $"VALUES ({string.Join(", ", placeholders)})";

            bindOrder = columns.Select(c => c.Name).ToList();
            counterColumnNames = Array.Empty<string>();
        }

        var ps = await session.PrepareAsync(cql);
        return (ps, bindOrder, isCounterTable, counterColumnNames);
    }

    private static bool IsCounterColumn(
        (string Name, string Type, string Kind, string ClusteringOrder, int Position) c)
    {
        return string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase);
    }
}
