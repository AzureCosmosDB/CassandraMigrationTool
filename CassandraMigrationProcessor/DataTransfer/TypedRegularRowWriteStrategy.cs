using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Fast binary row-write strategy for non-counter (regular) target
/// tables, used when the job selects the non-JSON copy path
/// (<see cref="Models.Job.UseJsonCopy"/> = false). Per-row work is a
/// single token-aware typed <c>INSERT … VALUES</c> bound directly from
/// the source <c>object[]</c>, executed at the job's configured write
/// consistency. Retry, latency accounting, and error handling are
/// delegated to <see cref="RowWriteRetry"/>. Null source values are
/// bound as <c>null</c> so the target faithfully mirrors the source —
/// including tombstone semantics needed for resume/online catch-up.
/// <para>
/// This path does <em>not</em> carry <c>USING TIMESTAMP/TTL</c>, so TTL
/// and writetime are not preserved; the CDC <c>metadata</c> argument is
/// ignored. It is the counterpart to <see cref="RegularRowWriteStrategy"/>
/// (the JSON metadata-preserving path).
/// </para>
/// </summary>
internal sealed class TypedRegularRowWriteStrategy : IRowWriteStrategy
{
    private readonly WorkerLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsert;
    private readonly int[] _bindOrderToSourceIndex;
    private readonly RetryPolicy _retryPolicy;
    private readonly ConsistencyLevel _writeConsistencyLevel;
    private readonly bool _bindOrderIsIdentity;

    private TypedRegularRowWriteStrategy(WorkerLog log, ISession targetSession,
        PreparedStatement preparedInsert, int[] bindOrderToSourceIndex,
        RetryPolicy retryPolicy, ConsistencyLevel writeConsistencyLevel)
    {
        _log = log;
        _targetSession = targetSession;
        _preparedInsert = preparedInsert;
        _bindOrderToSourceIndex = bindOrderToSourceIndex;
        _retryPolicy = retryPolicy;
        _writeConsistencyLevel = writeConsistencyLevel;
        _bindOrderIsIdentity = IsIdentityMap(bindOrderToSourceIndex);
    }

    /// <summary>
    /// Async factory. Prepares the typed <c>INSERT … VALUES</c> statement
    /// against the target and builds the bind-order → source-index map.
    /// The factory shape mirrors <see cref="RegularRowWriteStrategy.CreateAsync"/>
    /// so <see cref="RowWriteStrategyFactory"/> can dispatch generically.
    /// </summary>
    public static async Task<TypedRegularRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession,
        List<CassandraColumn> columns,
        string targetKeyspace, string targetTable, RetryPolicy retryPolicy,
        ConsistencyLevel writeConsistencyLevel)
    {
        var (ps, bindOrder) = await CassandraQueries.PrepareInsertAsync(
            targetSession, targetKeyspace, targetTable, columns);
        var bindOrderToSourceIndex =
            RowWriteStrategyFactory.BuildBindOrderToSourceIndex(bindOrder, columns);
        return new TypedRegularRowWriteStrategy(
            log, targetSession, ps, bindOrderToSourceIndex, retryPolicy,
            writeConsistencyLevel);
    }

    private static bool IsIdentityMap(int[] map)
    {
        for (int i = 0; i < map.Length; i++)
            if (map[i] != i) return false;
        return true;
    }

    private BoundStatement BindRow(object[] sourceRow)
    {
        object[] bindValues;
        if (_bindOrderToSourceIndex.Length == sourceRow.Length && _bindOrderIsIdentity)
        {
            bindValues = sourceRow;
        }
        else
        {
            bindValues = new object[_bindOrderToSourceIndex.Length];
            for (int b = 0; b < _bindOrderToSourceIndex.Length; b++)
                bindValues[b] = sourceRow[_bindOrderToSourceIndex[b]];
        }
        return _preparedInsert.Bind(bindValues);
    }

    public Task<WriteOutcome> WriteRowAsync(
        object[] sourceRow,
        WriteCounters counters,
        CdcRowMetadata? metadata,
        CancellationToken cancellationToken)
    {
        // metadata is intentionally ignored: the fast binary path does not
        // preserve TTL/writetime (SELECT * strips the __sys_* columns).
        var bound = BindRow(sourceRow);
        bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        bound.SetConsistencyLevel(_writeConsistencyLevel);

        return RowWriteRetry.ExecuteAsync(
            attempt: () => _targetSession.ExecuteAsync(bound).WaitAsync(cancellationToken),
            policy: _retryPolicy,
            log: _log, rowKind: "Row",
            counters: counters,
            cancellationToken: cancellationToken);
    }

    public Task<WriteOutcome> WriteJsonRowAsync(
        string cleanedJson,
        WriteCounters counters,
        CdcRowMetadata? metadata,
        CancellationToken cancellationToken)
        => throw new NotSupportedException(
            "The typed binary copy path writes via WriteRowAsync; " +
            "the JSON envelope path is reserved for RegularRowWriteStrategy.");
}
