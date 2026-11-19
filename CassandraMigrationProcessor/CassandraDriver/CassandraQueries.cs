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
    /// Build a prepared typed <c>INSERT … (cols) VALUES (?, …)</c> for a
    /// non-counter table. The returned <c>BindOrder</c> lists the column
    /// names in bind-parameter order. This is the fast binary copy path
    /// (paired with <c>SELECT *</c>); it does not carry
    /// <c>USING TIMESTAMP/TTL</c>, so TTL and writetime are not preserved.
    /// Throws if called on a counter table — use
    /// <see cref="PrepareCounterUpdateAsync"/> instead.
    /// </summary>
    public static async Task<(PreparedStatement Ps, List<string> BindOrder)>
        PrepareInsertAsync(ISession session, string keyspace, string table,
            List<CassandraColumn> columns)
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
    /// Build a prepared partial <c>INSERT … JSON ? DEFAULT UNSET USING
    /// TIMESTAMP ? AND TTL ?</c> used to restore the source row-liveness
    /// marker before divergent cells are replayed with UPDATE statements.
    /// <c>DEFAULT UNSET</c> makes columns absent from the baseline payload
    /// stay untouched rather than being overwritten with <c>null</c>. Bind
    /// order matches <see cref="PrepareInsertJsonAsync"/>:
    /// (json, writetime, ttl).
    /// </summary>
    public static async Task<PreparedStatement>
        PrepareInsertJsonPartialAsync(ISession session, string keyspace, string table,
            List<CassandraColumn> columns)
    {
        if (IsCounterTable(columns))
            throw new InvalidOperationException(
                $"PrepareInsertJsonPartialAsync called on counter table {keyspace}.{table}; " +
                "counter tables must use PrepareCounterUpdateAsync.");

        var cql =
            $"INSERT INTO \"{keyspace}\".\"{table}\" JSON ? DEFAULT UNSET " +
            "USING TIMESTAMP ? AND TTL ?";
        return await session.PrepareAsync(cql);
    }

    /// <summary>
    /// UPDATE that assigns one column from its JSON representation. Using
    /// UPDATE is important: unlike INSERT it never creates or replaces the
    /// row-liveness marker. Bind order is
    /// (writetime, ttl, jsonValue, primary-key JSON values in key order).
    /// Static columns are addressed by the partition key only.
    /// </summary>
    internal static string BuildUpdateJsonColumnCql(
        string keyspace,
        string table,
        CassandraColumn column,
        IReadOnlyList<CassandraColumn> primaryKeyColumns)
        => BuildUpdateJsonColumnsCql(keyspace, table, [column], primaryKeyColumns);

    /// <summary>
    /// UPDATE that assigns several columns sharing one writetime/TTL. Bind
    /// order is (writetime, ttl, one JSON value per column, key values). All
    /// columns must be static, or all non-static.
    /// </summary>
    internal static string BuildUpdateJsonColumnsCql(
        string keyspace,
        string table,
        IReadOnlyList<CassandraColumn> columns,
        IReadOnlyList<CassandraColumn> primaryKeyColumns)
    {
        if (columns.Count == 0)
            throw new ArgumentException("At least one column is required.", nameof(columns));
        bool isStatic = columns[0].Kind == "static";
        if (columns.Any(c => (c.Kind == "static") != isStatic))
            throw new ArgumentException(
                "Static and non-static columns cannot share one UPDATE.", nameof(columns));
        var setClause = string.Join(", ",
            columns.Select(c => $"{CqlIdentifier.Quote(c.Name)} = fromJson(?)"));
        return BuildUpdateCql(keyspace, table, setClause, isStatic, primaryKeyColumns);
    }

    /// <summary>
    /// UPDATE that adds elements to a non-frozen collection
    /// (<c>c = c + fromJson(?)</c>). Bind order matches
    /// <see cref="BuildUpdateJsonColumnCql"/>.
    /// </summary>
    internal static string BuildAddJsonColumnCql(
        string keyspace,
        string table,
        CassandraColumn column,
        IReadOnlyList<CassandraColumn> primaryKeyColumns)
    {
        var quoted = CqlIdentifier.Quote(column.Name);
        return BuildUpdateCql(keyspace, table,
            $"{quoted} = {quoted} + fromJson(?)",
            column.Kind == "static", primaryKeyColumns);
    }

    /// <summary>
    /// UPDATE that removes elements or keys from a non-frozen set or map
    /// (<c>c = c - fromJson(?)</c>, the JSON value being an array of the
    /// elements or keys). Bind order matches
    /// <see cref="BuildUpdateJsonColumnCql"/>.
    /// </summary>
    internal static string BuildSubtractJsonColumnCql(
        string keyspace,
        string table,
        CassandraColumn column,
        IReadOnlyList<CassandraColumn> primaryKeyColumns)
    {
        var quoted = CqlIdentifier.Quote(column.Name);
        return BuildUpdateCql(keyspace, table,
            $"{quoted} = {quoted} - fromJson(?)",
            column.Kind == "static", primaryKeyColumns);
    }

    /// <summary>
    /// UPDATE that assigns one field of a non-frozen UDT
    /// (<c>u.f = fromJson(?)</c>). Bind order matches
    /// <see cref="BuildUpdateJsonColumnCql"/>.
    /// </summary>
    internal static string BuildUpdateJsonUdtFieldCql(
        string keyspace,
        string table,
        CassandraColumn column,
        string field,
        IReadOnlyList<CassandraColumn> primaryKeyColumns)
        => BuildUpdateCql(keyspace, table,
            $"{CqlIdentifier.Quote(column.Name)}.{CqlIdentifier.Quote(field)} = fromJson(?)",
            column.Kind == "static", primaryKeyColumns);

    private static string BuildUpdateCql(
        string keyspace,
        string table,
        string setClause,
        bool isStatic,
        IReadOnlyList<CassandraColumn> primaryKeyColumns)
    {
        if (primaryKeyColumns.Count == 0)
            throw new ArgumentException(
                $"Cannot build UPDATE for {keyspace}.{table} without primary-key columns.",
                nameof(primaryKeyColumns));
        var restrictedKeys = isStatic
            ? primaryKeyColumns.Where(c => c.Kind == "partition_key")
            : primaryKeyColumns;
        var whereClause = string.Join(" AND ",
            restrictedKeys.Select(c => $"{CqlIdentifier.Quote(c.Name)} = fromJson(?)"));
        return
            $"UPDATE {CqlIdentifier.Quote(keyspace)}.{CqlIdentifier.Quote(table)} " +
            $"USING TIMESTAMP ? AND TTL ? " +
            $"SET {setClause} WHERE {whereClause}";
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
