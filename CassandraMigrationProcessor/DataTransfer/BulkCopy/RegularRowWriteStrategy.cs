using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Row-write strategy for non-counter (regular) target tables. Per-row
/// work is a single token-aware INSERT bound from the source row,
/// executed at <see cref="ConsistencyLevel.LocalOne"/>. Retry, latency
/// accounting, and error handling are delegated to
/// <see cref="RowWriteRetry"/>. Null source values are bound as
/// <c>null</c> so the target faithfully mirrors the source — including
/// the tombstone semantics needed for resume/online catch-up correctness.
/// </summary>
internal sealed class RegularRowWriteStrategy : IRowWriteStrategy
{
    private readonly WorkerLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsert;
    private readonly int[] _bindOrderToSourceIndex;
    private readonly RetryPolicy _retryPolicy;
    private readonly bool _bindOrderIsIdentity;

    private RegularRowWriteStrategy(WorkerLog log, ISession targetSession, PreparedStatement preparedInsert,
        int[] bindOrderToSourceIndex, RetryPolicy retryPolicy)
    {
        _log = log;
        _targetSession = targetSession;
        _preparedInsert = preparedInsert;
        _bindOrderToSourceIndex = bindOrderToSourceIndex;
        _retryPolicy = retryPolicy;
        _bindOrderIsIdentity = IsIdentityMap(bindOrderToSourceIndex);
    }

    /// <summary>
    /// Async factory. Prepares the INSERT statement against the target
    /// and builds the bind-order → source-index map. The factory shape
    /// mirrors <see cref="CounterRowWriteStrategy.CreateAsync"/> so
    /// <see cref="RowWriteStrategyFactory"/> can dispatch generically.
    /// </summary>
    public static async Task<RegularRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession, WorkerConfig config, RetryPolicy retryPolicy)
    {
        var (ps, bindOrder) = await CassandraQueries.PrepareInsertAsync(
            targetSession, config.Context.TargetKeyspaceName, config.Context.TargetTableName, config.Columns);
        var bindOrderToSourceIndex = RowWriteStrategyFactory.BuildBindOrderToSourceIndex(bindOrder, config.Columns);
        return new RegularRowWriteStrategy(log, targetSession, ps, bindOrderToSourceIndex, retryPolicy);
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

    public Task WriteRowAsync(object[] sourceRow, PipelineContext ctx, WriteCounters counters, int rowIndex)
    {
        var bound = BindRow(sourceRow);
        bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        bound.SetConsistencyLevel(ConsistencyLevel.LocalOne);

        return RowWriteRetry.ExecuteAsync(
            attempt: () => _targetSession.ExecuteAsync(bound),
            policy: _retryPolicy,
            log: _log, rowIndex: rowIndex, rowKind: "Row",
            ctx: ctx, counters: counters);
    }
}
