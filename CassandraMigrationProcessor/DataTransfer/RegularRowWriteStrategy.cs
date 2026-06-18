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
/// </summary>
internal sealed class RegularRowWriteStrategy : IRowWriteStrategy
{
    private readonly WorkerLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsertJson;
    private readonly RetryPolicy _retryPolicy;
    private readonly string _rowKind;

    private RegularRowWriteStrategy(WorkerLog log, ISession targetSession,
        PreparedStatement preparedInsertJson, RetryPolicy retryPolicy, string tableLabel)
    {
        _log = log;
        _targetSession = targetSession;
        _preparedInsertJson = preparedInsertJson;
        _retryPolicy = retryPolicy;
        _rowKind = $"JSON row[{tableLabel}]";
    }

    /// <summary>
    /// Async factory. Prepares the <c>INSERT … JSON ?</c> statement
    /// against the target. The factory shape mirrors
    /// <see cref="CounterRowWriteStrategy.CreateAsync"/> so
    /// <see cref="RowWriteStrategyFactory"/> can dispatch generically.
    /// </summary>
    public static async Task<RegularRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession,
        List<CassandraColumn> columns,
        string targetKeyspace, string targetTable, RetryPolicy retryPolicy)
    {
        var psJson = await CassandraQueries.PrepareInsertJsonAsync(
            targetSession, targetKeyspace, targetTable, columns);
        return new RegularRowWriteStrategy(log, targetSession, psJson, retryPolicy,
            tableLabel: $"{targetKeyspace}.{targetTable}");
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
        int ttlSeconds = ResolveTtlSeconds(metadata);
        long ts = metadata?.WritetimeMicros
                  ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L;
        // Bind layout: (envelope, writetime, ttl).
        var bound = _preparedInsertJson.Bind(cleanedJson, ts, ttlSeconds);
        bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        bound.SetConsistencyLevel(ConsistencyLevel.LocalOne);

        return RowWriteRetry.ExecuteAsync(
            attempt: () => _targetSession.ExecuteAsync(bound).WaitAsync(cancellationToken),
            policy: _retryPolicy,
            log: _log, rowKind: _rowKind,
            counters: counters,
            cancellationToken: cancellationToken);
    }

    private static int ResolveTtlSeconds(CdcRowMetadata? metadata)
    {
        // Replay-clamp policy: a row with a remaining TTL > 0 carries
        // that value forward; an already-expired row is written with
        // TTL=1 so the destination tombstones it via the same LWW path
        // the source took; a row with no TTL at all writes with TTL=0
        // (no expiry).
        if (metadata is not { HasTtl: true } md) return 0;
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long? remaining = md.ComputeRemainingTtlSeconds(now);
        if (remaining is long r && r > 0)
            return (int)Math.Min(r, int.MaxValue);
        return 1;
    }
}
