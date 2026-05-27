using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Writer for non-counter (regular) target tables. Per-row work is a
/// single token-aware INSERT bound from the source row, executed at
/// <see cref="ConsistencyLevel.LocalOne"/> with a bounded retry loop on
/// transient errors. Null source values are bound as <c>null</c> so the
/// target faithfully mirrors the source — including the tombstone
/// semantics needed for resume/online catch-up correctness.
/// </summary>
internal sealed class RegularPageWriter : PageWriter
{
    public RegularPageWriter(MigrationLog log, ISession targetSession, PreparedStatement preparedInsert,
        int[] bindOrderToSourceIndex, int pageSize, int workerId, int maxWriteRetries,
        CancellationToken cancellationToken)
        : base(log, targetSession, preparedInsert, bindOrderToSourceIndex, pageSize, workerId, maxWriteRetries, cancellationToken)
    {
    }

    private BoundStatement BindRow(object[] sourceRow)
    {
        object[] bindValues;
        if (_bindOrderToSourceIndex.Length == sourceRow.Length
            && IsIdentityMap(_bindOrderToSourceIndex))
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

    protected override async Task WriteRowAsync(object[] sourceRow, PipelineContext ctx, WriteCounters counters, int rowIndex)
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
            catch (System.Exception ex)
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
