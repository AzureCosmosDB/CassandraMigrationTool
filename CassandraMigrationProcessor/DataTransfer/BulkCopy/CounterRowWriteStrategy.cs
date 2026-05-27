using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Diagnostics;
using System.Threading;
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
/// attempt, so partial applies from earlier attempts are reconciled.
/// This matches the approach used by Datastax/cassandra-data-migrator
/// (CopyJobSession.bind + TargetUpdateStatement.bind).
///
/// Both the SELECT and the UPDATE run at
/// <see cref="ConsistencyLevel.LocalQuorum"/> so the read sees the
/// latest target writes — important for retry correctness across
/// replicas. This is more expensive than the LocalOne used for regular
/// tables but is the only safe choice for counter idempotency.
/// </summary>
internal sealed class CounterRowWriteStrategy : IRowWriteStrategy
{
    private const int WriteTimeoutMs = 60_000;
    private const int RetryDelayMs = 500;

    private readonly MigrationLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsert;
    private readonly int[] _bindOrderToSourceIndex;
    private readonly int _workerId;
    private readonly int _maxWriteRetries;
    private readonly int _counterBindCount;
    private readonly PreparedStatement _targetSelectByPk;

    public CounterRowWriteStrategy(MigrationLog log, ISession targetSession, PreparedStatement preparedInsert,
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

    public async Task WriteRowAsync(object[] sourceRow, PipelineContext ctx, WriteCounters counters, int rowIndex)
    {
        for (int attempt = 1; attempt <= _maxWriteRetries; attempt++)
        {
            var rowStart = Stopwatch.GetTimestamp();
            try
            {
                // 1) SELECT current target counter values for this PK.
                var keyValues = new object[_bindOrderToSourceIndex.Length - _counterBindCount];
                for (int k = 0; k < keyValues.Length; k++)
                    keyValues[k] = sourceRow[_bindOrderToSourceIndex[_counterBindCount + k]];

                var selectBound = _targetSelectByPk.Bind(keyValues);
                selectBound.SetReadTimeoutMillis(WriteTimeoutMs);
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
                    // increments to migrate). Skip the UPDATE.
                    long elapsedNoop = (Stopwatch.GetTimestamp() - rowStart) * 1000 / Stopwatch.Frequency;
                    Interlocked.Add(ref counters.LatencySum, elapsedNoop);
                    Interlocked.Increment(ref counters.Done);
                    return;
                }

                // 3) Apply UPDATE c = c + delta.
                var bound = _preparedInsert.Bind(bindValues);
                bound.SetReadTimeoutMillis(WriteTimeoutMs);
                bound.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                await _targetSession.ExecuteAsync(bound);

                long elapsed = (Stopwatch.GetTimestamp() - rowStart) * 1000 / Stopwatch.Frequency;
                Interlocked.Add(ref counters.LatencySum, elapsed);
                Interlocked.Increment(ref counters.Done);
                return;
            }
            catch (Exception ex)
            {
                if (ExceptionClassifier.IsFatal(ex))
                {
                    _log.WriteLine($"[W{_workerId}] FATAL counter row {rowIndex}: {ex.GetType().Name}: {ex.Message}",
                        LogType.Error);
                    Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                    Interlocked.Increment(ref counters.Failed);
                    return;
                }

                if (ExceptionClassifier.IsTransient(ex) && attempt < _maxWriteRetries)
                {
                    // The read-modify-write loop re-reads the target on
                    // every retry, so a partial apply from the previous
                    // attempt is reconciled by the next delta.
                    await Task.Delay(RetryDelayMs * attempt);
                    continue;
                }

                Interlocked.Increment(ref counters.Failed);
                _log.WriteLine($"[W{_workerId}] Counter row {rowIndex} FAILED after {attempt} attempt(s): {ex.GetType().Name}: {ex.Message}",
                    LogType.Error);

                if (!ExceptionClassifier.IsTransient(ex))
                {
                    Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                }
                return;
            }
        }
    }
}
