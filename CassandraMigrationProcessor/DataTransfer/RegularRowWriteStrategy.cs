using System.Buffers;
using System.Text;
using System.Text.Json;
using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Row-write strategy for non-counter (regular) target tables. Writes
/// each row via a prepared
/// <c>INSERT INTO … JSON ? USING TIMESTAMP ? AND TTL ?</c> statement
/// bound from a cleaned <c>SELECT JSON *</c> envelope. The destination
/// server handles all type marshalling, so this path transparently
/// supports every CQL type the source table contains (UDT, tuple,
/// decimal, varint, duration, nested collections) without any
/// per-type code on the migrator.
/// <para>
/// When <see cref="CdcRowMetadata"/> is supplied the strategy binds
/// the source writetime + remaining TTL; otherwise it binds wall-clock
/// micros + <c>0</c> (no TTL), which is semantically identical to
/// omitting the clause.
/// </para>
/// <para>
/// Cell-level preservation: when enabled and the source row carries
/// per-column writetime/TTL that diverges from the row-level values
/// (<see cref="CdcRowMetadata.HasPerColumnDivergence"/>), the row is
/// split into one partial <c>INSERT … JSON ? DEFAULT UNSET USING
/// TIMESTAMP ? AND TTL ?</c> per distinct (writetime, ttl) group so each
/// cell lands with its own timestamp and TTL. Uniform rows keep the
/// single-statement fast path unchanged.
/// </para>
/// </summary>
internal sealed class RegularRowWriteStrategy : IRowWriteStrategy
{
    private readonly WorkerLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsertJson;

    // Non-null only when cell-level preservation is enabled: the
    // DEFAULT UNSET partial insert plus the set of primary-key column
    // names that must appear in every partial write.
    private readonly PreparedStatement? _preparedInsertJsonPartial;
    private readonly HashSet<string>? _primaryKeyColumns;

    private readonly RetryPolicy _retryPolicy;
    private readonly ConsistencyLevel _writeConsistencyLevel;
    private readonly string _rowKind;

    private RegularRowWriteStrategy(WorkerLog log, ISession targetSession,
        PreparedStatement preparedInsertJson,
        PreparedStatement? preparedInsertJsonPartial,
        HashSet<string>? primaryKeyColumns,
        RetryPolicy retryPolicy,
        ConsistencyLevel writeConsistencyLevel, string tableLabel)
    {
        _log = log;
        _targetSession = targetSession;
        _preparedInsertJson = preparedInsertJson;
        _preparedInsertJsonPartial = preparedInsertJsonPartial;
        _primaryKeyColumns = primaryKeyColumns;
        _retryPolicy = retryPolicy;
        _writeConsistencyLevel = writeConsistencyLevel;
        _rowKind = $"JSON row[{tableLabel}]";
    }

    /// <summary>
    /// Async factory. Prepares the <c>INSERT … JSON ?</c> statement
    /// against the target (and, when <paramref name="preserveCellLevel"/>
    /// is set, the DEFAULT UNSET partial variant used for per-cell
    /// splitting). The factory shape mirrors
    /// <see cref="CounterRowWriteStrategy.CreateAsync"/> so
    /// <see cref="RowWriteStrategyFactory"/> can dispatch generically.
    /// </summary>
    public static async Task<RegularRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession,
        List<CassandraColumn> columns,
        string targetKeyspace, string targetTable, RetryPolicy retryPolicy,
        ConsistencyLevel writeConsistencyLevel,
        bool preserveCellLevel)
    {
        var psJson = await CassandraQueries.PrepareInsertJsonAsync(
            targetSession, targetKeyspace, targetTable, columns);

        PreparedStatement? psPartial = null;
        HashSet<string>? pkColumns = null;
        if (preserveCellLevel)
        {
            // Prepared eagerly so a target that cannot honour DEFAULT UNSET
            // fails fast and visibly at strategy creation rather than
            // silently degrading cell-level fidelity mid-migration.
            psPartial = await CassandraQueries.PrepareInsertJsonPartialAsync(
                targetSession, targetKeyspace, targetTable, columns);
            pkColumns = new HashSet<string>(
                columns
                    .Where(c => c.Kind is "partition_key" or "clustering")
                    .Select(c => c.Name),
                StringComparer.Ordinal);
        }

        return new RegularRowWriteStrategy(
            log, targetSession, psJson, psPartial, pkColumns, retryPolicy,
            writeConsistencyLevel, tableLabel: $"{targetKeyspace}.{targetTable}");
    }

    public Task<WriteOutcome> WriteRowAsync(
        object[] sourceRow,
        WriteCounters counters,
        CdcRowMetadata? metadata,
        CancellationToken cancellationToken)
    {
        // Regular tables are read via SELECT JSON * and written via
        // WriteJsonRowAsync. The typed binary path is reserved for
        // counter tables (CounterRowWriteStrategy).
        throw new NotSupportedException(
            "Regular tables must be written via WriteJsonRowAsync; " +
            "the typed object[] path is reserved for counter tables.");
    }

    public Task<WriteOutcome> WriteJsonRowAsync(
        string cleanedJson,
        WriteCounters counters,
        CdcRowMetadata? metadata,
        CancellationToken cancellationToken)
    {
        // Cell-level split path: only when preservation is enabled (partial
        // statement prepared) AND the row actually diverges. Everything
        // else takes the unchanged single-statement fast path.
        if (_preparedInsertJsonPartial is not null
            && metadata is { HasPerColumnDivergence: true })
        {
            return WriteSplitRowAsync(cleanedJson, metadata, counters, cancellationToken);
        }

        int ttlSeconds = ResolveTtlSeconds(metadata);
        long ts = metadata?.WritetimeMicros ?? NowMicros();
        // Bind layout: (envelope, writetime, ttl).
        var bound = _preparedInsertJson.Bind(cleanedJson, ts, ttlSeconds);
        bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        bound.SetConsistencyLevel(_writeConsistencyLevel);

        return RowWriteRetry.ExecuteAsync(
            attempt: () => _targetSession.ExecuteAsync(bound).WaitAsync(cancellationToken),
            policy: _retryPolicy,
            log: _log, rowKind: _rowKind,
            counters: counters,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Split one source row into per-(writetime, ttl) partial inserts so
    /// each cell is re-applied with its own <c>USING TIMESTAMP</c> and
    /// <c>USING TTL</c>. Primary-key columns are replicated into every
    /// group; a column with no per-cell metadata (nulls, non-frozen
    /// collections) falls back to the row-level group. Groups are written
    /// under one shared retry budget and counted as a single row.
    /// </summary>
    private Task<WriteOutcome> WriteSplitRowAsync(
        string cleanedJson,
        CdcRowMetadata metadata,
        WriteCounters counters,
        CancellationToken cancellationToken)
    {
        var groups = BuildGroups(cleanedJson, metadata);
        var attempts = new List<Func<Task>>(groups.Count);
        foreach (var group in groups)
        {
            int ttlSeconds = ResolveTtlSecondsFromExpiry(group.Key.Expiry);
            long ts = group.Key.Writetime ?? NowMicros();
            var bound = _preparedInsertJsonPartial!.Bind(group.Json, ts, ttlSeconds);
            bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
            bound.SetConsistencyLevel(_writeConsistencyLevel);
            attempts.Add(() => _targetSession.ExecuteAsync(bound).WaitAsync(cancellationToken));
        }

        return RowWriteRetry.ExecuteRowGroupsAsync(
            attempts, _retryPolicy, _log, _rowKind, counters, cancellationToken);
    }

    private readonly record struct GroupKey(long? Writetime, long? Expiry);

    private readonly record struct GroupWrite(GroupKey Key, string Json);

    /// <summary>
    /// Parse the cleaned envelope and materialise one JSON payload per
    /// distinct (writetime, expiry) group. Each payload carries the row's
    /// primary-key columns plus that group's data columns.
    /// </summary>
    private List<GroupWrite> BuildGroups(string cleanedJson, CdcRowMetadata metadata)
    {
        using var doc = JsonDocument.Parse(cleanedJson);
        var root = doc.RootElement;

        var pkProps = new List<JsonProperty>();
        // Preserve stable, insertion-like ordering of groups for
        // deterministic output; grouping-key equality drives membership.
        var order = new List<GroupKey>();
        var byKey = new Dictionary<GroupKey, List<JsonProperty>>();

        var rowKey = new GroupKey(metadata.WritetimeMicros, metadata.ExpiryEpochSeconds);
        var perColumn = metadata.PerColumn!;

        foreach (var prop in root.EnumerateObject())
        {
            if (_primaryKeyColumns!.Contains(prop.Name))
            {
                pkProps.Add(prop);
                continue;
            }

            GroupKey key = perColumn.TryGetValue(prop.Name, out var cell)
                ? new GroupKey(cell.WritetimeMicros, cell.ExpiryEpochSeconds)
                : rowKey;

            if (!byKey.TryGetValue(key, out var list))
            {
                list = new List<JsonProperty>();
                byKey[key] = list;
                order.Add(key);
            }
            list.Add(prop);
        }

        var result = new List<GroupWrite>(order.Count);
        foreach (var key in order)
            result.Add(new GroupWrite(key, WriteSubsetJson(pkProps, byKey[key])));
        return result;
    }

    /// <summary>
    /// Emit a JSON object containing the primary-key columns followed by
    /// the supplied data columns, copying each value verbatim from the
    /// source document.
    /// </summary>
    private static string WriteSubsetJson(
        List<JsonProperty> pkProps, List<JsonProperty> dataProps)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer,
            new JsonWriterOptions { SkipValidation = true }))
        {
            writer.WriteStartObject();
            foreach (var pk in pkProps) pk.WriteTo(writer);
            foreach (var col in dataProps) col.WriteTo(writer);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static long NowMicros()
        => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L;

    private static int ResolveTtlSeconds(CdcRowMetadata? metadata)
    {
        if (metadata is not { HasTtl: true } md) return 0;
        return ResolveTtlSecondsFromExpiry(md.ExpiryEpochSeconds);
    }

    /// <summary>
    /// Replay-clamp policy: a cell/row with a remaining TTL &gt; 0 carries
    /// that value forward; an already-expired one is written with TTL=1 so
    /// the destination tombstones it via the same LWW path the source
    /// took; no TTL writes with TTL=0 (no expiry).
    /// </summary>
    private static int ResolveTtlSecondsFromExpiry(long? expiryEpochSeconds)
    {
        if (expiryEpochSeconds is not long expiry) return 0;
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long remaining = expiry - now;
        return remaining > 0 ? (int)Math.Min(remaining, int.MaxValue) : 1;
    }
}

