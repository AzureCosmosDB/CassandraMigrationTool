using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Row-write strategy for counter target tables. Counter UPDATEs in
/// Cassandra are NOT idempotent — a transient timeout may have applied
/// the increment server-side or not, and a naive retry produces
/// double-counts. To get idempotency we use read-modify-write per row:
/// <list type="number">
///   <item>SELECT current target counter values for this row's PK.</item>
///   <item>Compute delta = origin − target for each counter column.</item>
///   <item>UPDATE c = c + delta. Bind <see cref="Unset"/>.Value when:
///     (a) origin is null — never incremented on source; Cassandra
///         rejects null counter increments and there is no safe way to
///         "unset" a target counter cell, so we leave it as-is;
///     (b) delta is 0 — target is already correct, skip the cell.</item>
///   <item>Skip the row entirely when every counter ends up unset.</item>
/// </list>
/// The delta is recomputed against the current target on every retry
/// attempt by <see cref="RowWriteRetry"/>, so partial applies from
/// earlier attempts are reconciled. This matches the approach used by
/// Datastax/cassandra-data-migrator (CopyJobSession.bind +
/// TargetUpdateStatement.bind).
///
/// Both the SELECT and the UPDATE run at
/// <see cref="ConsistencyLevel.LocalQuorum"/> so the read sees the
/// latest target writes — important for retry correctness across
/// replicas. This is more expensive than the LocalOne used for regular
/// tables but is the only safe choice for counter idempotency.
/// </summary>
internal sealed class CounterRowWriteStrategy : IRowWriteStrategy
{
    private readonly WorkerLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsert;
    private readonly int[] _bindOrderToSourceIndex;
    private readonly RetryPolicy _retryPolicy;
    private readonly int _counterBindCount;
    private readonly PreparedStatement _targetSelectByPk;

    private CounterRowWriteStrategy(WorkerLog log, ISession targetSession, PreparedStatement preparedInsert,
        int[] bindOrderToSourceIndex, RetryPolicy retryPolicy,
        int counterBindCount, PreparedStatement targetSelectByPk)
    {
        _log = log;
        _targetSession = targetSession;
        _preparedInsert = preparedInsert;
        _bindOrderToSourceIndex = bindOrderToSourceIndex;
        _retryPolicy = retryPolicy;
        _counterBindCount = counterBindCount;
        _targetSelectByPk = targetSelectByPk;
    }

    /// <summary>
    /// Async factory. Prepares the counter UPDATE on the target via
    /// <see cref="CassandraQueries.PrepareCounterUpdateAsync"/>, builds
    /// the bind-order → source-index map, and prepares the SELECT-by-PK
    /// used for read-modify-write.
    /// </summary>
    public static async Task<CounterRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession,
        List<CassandraColumn> columns,
        string targetKeyspace, string targetTable, RetryPolicy retryPolicy)
    {
        var (ps, bindOrder) = await CassandraQueries.PrepareCounterUpdateAsync(
            targetSession, targetKeyspace, targetTable, columns);
        var bindOrderToSourceIndex = RowWriteStrategyFactory.BuildBindOrderToSourceIndex(bindOrder, columns);

        // PrepareCounterUpdateAsync emits counter columns first in
        // bindOrder, so the count of counter columns in the schema is
        // exactly the counter-bind prefix length we need for RMW.
        int counterBindCount = columns.Count(c =>
            string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase));

        var selectCounterCols = string.Join(", ",
            bindOrder.Take(counterBindCount).Select(n => $"\"{n}\""));
        var whereKeyCols = string.Join(" AND ",
            bindOrder.Skip(counterBindCount).Select(n => $"\"{n}\" = ?"));
        var selectCql =
            $"SELECT {selectCounterCols} " +
            $"FROM \"{targetKeyspace}\".\"{targetTable}\" " +
            $"WHERE {whereKeyCols}";
        var targetSelectByPk = await targetSession.PrepareAsync(selectCql);

        return new CounterRowWriteStrategy(log, targetSession, ps, bindOrderToSourceIndex,
            retryPolicy, counterBindCount, targetSelectByPk);
    }

    public Task WriteRowAsync(object[] sourceRow, Action onFatal, WriteCounters counters, int rowIndex, CancellationToken cancellationToken)
    {
        return RowWriteRetry.ExecuteAsync(
            attempt: () => ReadModifyWriteAsync(sourceRow, cancellationToken),
            policy: _retryPolicy,
            log: _log, rowIndex: rowIndex, rowKind: "Counter row",
            onFatal: onFatal, counters: counters,
            cancellationToken: cancellationToken);
    }

    private async Task ReadModifyWriteAsync(object[] sourceRow, CancellationToken cancellationToken)
    {
        // 1) SELECT current target counter values for this PK.
        var keyValues = new object[_bindOrderToSourceIndex.Length - _counterBindCount];
        for (int k = 0; k < keyValues.Length; k++)
            keyValues[k] = sourceRow[_bindOrderToSourceIndex[_counterBindCount + k]];

        var selectBound = _targetSelectByPk.Bind(keyValues);
        selectBound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        selectBound.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        var rs = await _targetSession.ExecuteAsync(selectBound).WaitAsync(cancellationToken);
        Row? targetRow = null;
        foreach (var r in rs) { targetRow = r; break; }

        // 2) Compute deltas. Unset on null-origin or zero-delta.
        var bindValues = new object[_bindOrderToSourceIndex.Length];
        bool anyDelta = false;
        for (int b = 0; b < _counterBindCount; b++)
        {
            var originRaw = sourceRow[_bindOrderToSourceIndex[b]];
            if (originRaw == null)
            {
                bindValues[b] = Unset.Value;
                continue;
            }
            long origin = Convert.ToInt64(originRaw);
            long target = 0;
            if (targetRow != null)
            {
                var t = targetRow.GetValue<long?>(b);
                if (t.HasValue) target = t.Value;
            }
            long delta = origin - target;
            if (delta == 0)
            {
                bindValues[b] = Unset.Value;
                continue;
            }
            bindValues[b] = delta;
            anyDelta = true;
        }
        for (int b = _counterBindCount; b < bindValues.Length; b++)
            bindValues[b] = sourceRow[_bindOrderToSourceIndex[b]];

        if (!anyDelta)
        {
            // Target is already correct (or origin had no counter
            // increments to migrate). Skip the UPDATE — RowWriteRetry
            // will still count this as a successful row.
            return;
        }

        // 3) Apply UPDATE c = c + delta.
        var bound = _preparedInsert.Bind(bindValues);
        bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        bound.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        await _targetSession.ExecuteAsync(bound).WaitAsync(cancellationToken);
    }
}
