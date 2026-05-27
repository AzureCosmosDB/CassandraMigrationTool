using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

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
    private readonly MigrationLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsert;
    private readonly int[] _bindOrderToSourceIndex;
    private readonly int _workerId;
    private readonly int _maxWriteRetries;
    private readonly int _counterBindCount;
    private readonly PreparedStatement _targetSelectByPk;

    private CounterRowWriteStrategy(MigrationLog log, ISession targetSession, PreparedStatement preparedInsert,
        int[] bindOrderToSourceIndex, int workerId, int maxWriteRetries,
        int counterBindCount, PreparedStatement targetSelectByPk)
    {
        _log = log;
        _targetSession = targetSession;
        _preparedInsert = preparedInsert;
        _bindOrderToSourceIndex = bindOrderToSourceIndex;
        _workerId = workerId;
        _maxWriteRetries = maxWriteRetries;
        _counterBindCount = counterBindCount;
        _targetSelectByPk = targetSelectByPk;
    }

    /// <summary>
    /// Async factory. Computes how many leading bind slots are counter
    /// columns (PrepareInsertAsync emits them first by contract) and
    /// prepares the SELECT-by-PK used for read-modify-write.
    /// </summary>
    public static async Task<CounterRowWriteStrategy> CreateAsync(
        MigrationLog log, ISession targetSession, PreparedStatement preparedInsert,
        int[] bindOrderToSourceIndex, IReadOnlyList<string> bindOrder,
        string targetKeyspace, string targetTable,
        IEnumerable<string> counterColumnNames,
        int workerId, int maxWriteRetries)
    {
        var counterNames = new HashSet<string>(counterColumnNames, StringComparer.OrdinalIgnoreCase);

        int counterBindCount = 0;
        for (int i = 0; i < bindOrder.Count; i++)
        {
            if (counterNames.Contains(bindOrder[i])) counterBindCount++;
            else break;
        }

        var selectCounterCols = string.Join(", ",
            bindOrder.Take(counterBindCount).Select(n => $"\"{n}\""));
        var whereKeyCols = string.Join(" AND ",
            bindOrder.Skip(counterBindCount).Select(n => $"\"{n}\" = ?"));
        var selectCql =
            $"SELECT {selectCounterCols} " +
            $"FROM \"{targetKeyspace}\".\"{targetTable}\" " +
            $"WHERE {whereKeyCols}";
        var targetSelectByPk = await targetSession.PrepareAsync(selectCql);

        return new CounterRowWriteStrategy(log, targetSession, preparedInsert, bindOrderToSourceIndex,
            workerId, maxWriteRetries, counterBindCount, targetSelectByPk);
    }

    public Task WriteRowAsync(object[] sourceRow, PipelineContext ctx, WriteCounters counters, int rowIndex)
    {
        return RowWriteRetry.ExecuteAsync(
            attempt: () => ReadModifyWriteAsync(sourceRow),
            maxAttempts: _maxWriteRetries,
            log: _log, workerId: _workerId, rowIndex: rowIndex, rowKind: "Counter row",
            ctx: ctx, counters: counters);
    }

    private async Task ReadModifyWriteAsync(object[] sourceRow)
    {
        // 1) SELECT current target counter values for this PK.
        var keyValues = new object[_bindOrderToSourceIndex.Length - _counterBindCount];
        for (int k = 0; k < keyValues.Length; k++)
            keyValues[k] = sourceRow[_bindOrderToSourceIndex[_counterBindCount + k]];

        var selectBound = _targetSelectByPk.Bind(keyValues);
        selectBound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        selectBound.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        var rs = await _targetSession.ExecuteAsync(selectBound);
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
        await _targetSession.ExecuteAsync(bound);
    }
}
