using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Row-write strategy for non-counter (regular) target tables. Per-row
/// work is a single token-aware INSERT bound from the source row,
/// executed at <see cref="ConsistencyLevel.LocalOne"/> with a bounded
/// retry loop on transient errors. Null source values are bound as
/// <c>null</c> so the target faithfully mirrors the source — including
/// the tombstone semantics needed for resume/online catch-up correctness.
/// </summary>
internal sealed class RegularRowWriteStrategy : IRowWriteStrategy
{
    private const int WriteTimeoutMs = 60_000;
    private const int RetryDelayMs = 500;

    private readonly MigrationLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsert;
    private readonly int[] _bindOrderToSourceIndex;
    private readonly int _workerId;
    private readonly int _maxWriteRetries;
    private readonly bool _bindOrderIsIdentity;

    public RegularRowWriteStrategy(MigrationLog log, ISession targetSession, PreparedStatement preparedInsert,
        int[] bindOrderToSourceIndex, int workerId, int maxWriteRetries)
    {
        _log = log;
        _targetSession = targetSession;
        _preparedInsert = preparedInsert;
        _bindOrderToSourceIndex = bindOrderToSourceIndex;
        _workerId = workerId;
        _maxWriteRetries = maxWriteRetries;
        _bindOrderIsIdentity = IsIdentityMap(bindOrderToSourceIndex);
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

    public async Task WriteRowAsync(object[] sourceRow, PipelineContext ctx, WriteCounters counters, int rowIndex)
    {
        var bound = BindRow(sourceRow);
        bound.SetReadTimeoutMillis(WriteTimeoutMs);
        bound.SetConsistencyLevel(ConsistencyLevel.LocalOne);

        for (int attempt = 1; attempt <= _maxWriteRetries; attempt++)
        {
            var writeStart = Stopwatch.GetTimestamp();
            try
            {
                await _targetSession.ExecuteAsync(bound);
                long elapsed = (Stopwatch.GetTimestamp() - writeStart) * 1000 / Stopwatch.Frequency;
                Interlocked.Add(ref counters.LatencySum, elapsed);
                Interlocked.Increment(ref counters.Done);
                return;
            }
            catch (Exception ex)
            {
                if (ExceptionClassifier.IsFatal(ex))
                {
                    _log.WriteLine($"[W{_workerId}] FATAL row {rowIndex}: {ex.GetType().Name}: {ex.Message}",
                        LogType.Error);
                    Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                    Interlocked.Increment(ref counters.Failed);
                    return;
                }

                if (ExceptionClassifier.IsTransient(ex) && attempt < _maxWriteRetries)
                {
                    await Task.Delay(RetryDelayMs * attempt);
                    continue;
                }

                Interlocked.Increment(ref counters.Failed);
                _log.WriteLine($"[W{_workerId}] Row {rowIndex} FAILED after {attempt} attempt(s): {ex.GetType().Name}: {ex.Message}",
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
