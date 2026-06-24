using Cassandra;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.CassandraDriver;
/// <summary>
/// Helper methods for Cassandra data operations:
/// keyspace/table listing, row counts, feed ranges,
/// prepared statements, truncation, and retry logic.
/// Schema DDL operations live in <see cref="SchemaManager"/>.
/// </summary>
public static class CassandraQueries
{
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
    /// Get the row count of a table via COUNT(*) with a retry at a
    /// longer fallback timeout. Returns -1 if the count cannot be
    /// determined; the UI then renders "Copying (3k/s)" (rate only)
    /// instead of a numeric percent to signal unknown.
    /// </summary>
    public static async Task<long> GetRowCountAsync(ISession session, string keyspace, string table)
    {
        long count = await TryCountAsync(session, keyspace, table, MigrationDefaults.SchemaQueryTimeoutMs);
        if (count >= 0)
            return count;

        return await TryCountAsync(session, keyspace, table, MigrationDefaults.RowCountFallbackTimeoutMs);
    }

    private static async Task<long> TryCountAsync(ISession session, string keyspace, string table, int timeoutMs)
    {
        // COUNT(*) with bounded timeout. Very large tables may still
        // time out — migration proceeds without %.
        try
        {
            var statement = new SimpleStatement($"SELECT COUNT(*) FROM \"{keyspace}\".\"{table}\"");
            statement.SetReadTimeoutMillis(timeoutMs);
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
            // Timeout / transient — caller may retry with a larger
            // budget. Return -1 to signal "unknown, try again".
            if (ExceptionClassifier.IsTransient(ex))
                return -1;
            // Non-transient (auth, schema) — propagate so caller knows
            throw;
        }

        return -1;
    }

    /// <summary>
    /// Get feed ranges (physical partitions) for a table from
    /// <c>system_cosmos.feedranges</c>. Returns an empty list on
    /// non-Cosmos clusters where the system table doesn't exist.
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
            ranges.AddRange(resultSet
                .Select(row => row.GetValue<string>("range"))
                .Where(range => !string.IsNullOrEmpty(range)));
        }
        catch (InvalidQueryException ex)
        {
            // Expected on non-Cosmos clusters where system_cosmos.feedranges
            // does not exist. Caller falls back to token-range partitioning.
            verboseLog?.Invoke($"GetFeedRanges: system table unavailable ({ex.Message})");
        }
        // All other exceptions (timeout, auth, NoHost, etc.) must propagate.
        // Returning an empty list silently would cause the table to be
        // marked complete with zero rows copied.
        return ranges;
    }

    /// <summary>
    /// True when the table is a counter table (has at least one CQL
    /// counter column). Cassandra forbids mixing counter and
    /// non-counter regular columns, so the write path must use
    /// UPDATE c = c + ? instead of INSERT.
    /// </summary>
    public static bool IsCounterTable(
        IEnumerable<CassandraColumn> columns)
        => columns.Any(IsCounterColumn);

    /// <summary>
    /// Build a prepared <c>INSERT ... JSON ? USING TIMESTAMP ? AND TTL ?</c>
    /// for a non-counter table. The destination server handles all
    /// type coercion from the JSON envelope, so this path supports
    /// every CQL type the source table contains (UDT, tuple, decimal,
    /// varint, duration, nested collections) without per-type code on
    /// the migrator. Bind layout is
    /// <c>(jsonEnvelope, writetimeMicros, ttlSeconds)</c>;
    /// <c>USING TTL 0</c> is equivalent to omitting the clause so a
    /// single prepared statement covers both metadata-present and
    /// metadata-absent cases. Throws if called on a counter table —
    /// counters cannot be inserted via <c>INSERT JSON</c> and would
    /// also reject the <c>USING</c> clauses.
    /// </summary>
    public static async Task<PreparedStatement>
        PrepareInsertJsonAsync(ISession session, string keyspace, string table,
            List<CassandraColumn> columns)
    {
        if (IsCounterTable(columns))
            throw new InvalidOperationException(
                $"PrepareInsertJsonAsync called on counter table {keyspace}.{table}; " +
                "counter tables must use PrepareCounterUpdateAsync.");

        var cql =
            $"INSERT INTO \"{keyspace}\".\"{table}\" JSON ? " +
            "USING TIMESTAMP ? AND TTL ?";
        return await session.PrepareAsync(cql);
    }

    /// <summary>
    /// Build a prepared UPDATE for a counter table. Bind order is
    /// counter columns first (schema order), then partition-key and
    /// clustering columns in key order.
    /// </summary>
    public static async Task<(PreparedStatement Ps, List<string> BindOrder)>
        PrepareCounterUpdateAsync(ISession session, string keyspace, string table,
            List<CassandraColumn> columns)
    {
        var counterCols = columns.Where(IsCounterColumn).ToList();
        if (counterCols.Count == 0)
            throw new InvalidOperationException(
                $"PrepareCounterUpdateAsync called on non-counter table {keyspace}.{table}; " +
                "use PrepareInsertJsonAsync instead.");

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

        var ps = await session.PrepareAsync(cql);
        return (ps, bindOrder);
    }

    private static bool IsCounterColumn(
        CassandraColumn c)
    {
        return string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase);
    }
}
