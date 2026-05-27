using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Shared bounded-retry helper for per-row writes used by every
/// <see cref="IRowWriteStrategy"/>. Owns the attempt loop, latency
/// timing, fatal/transient exception classification, error logging,
/// fatal-flag propagation, and <see cref="WriteCounters"/> accounting.
/// Strategies provide only the per-attempt body via
/// <paramref name="attempt"/> — that callback is re-invoked on every
/// retry, which is what makes counter read-modify-write idempotent
/// (each attempt re-reads the current target before writing).
/// </summary>
internal static class RowWriteRetry
{
    public const int WriteTimeoutMs = 60_000;
    private const int RetryDelayMs = 500;

    public static async Task ExecuteAsync(
        Func<Task> attempt,
        int maxAttempts,
        MigrationLog log,
        int workerId,
        int rowIndex,
        string rowKind,
        PipelineContext ctx,
        WriteCounters counters)
    {
        for (int n = 1; n <= maxAttempts; n++)
        {
            var start = Stopwatch.GetTimestamp();
            try
            {
                await attempt();
                long elapsed = (Stopwatch.GetTimestamp() - start) * 1000 / Stopwatch.Frequency;
                Interlocked.Add(ref counters.LatencySum, elapsed);
                Interlocked.Increment(ref counters.Done);
                return;
            }
            catch (Exception ex)
            {
                if (ExceptionClassifier.IsFatal(ex))
                {
                    log.WriteLine($"[W{workerId}] FATAL {rowKind} {rowIndex}: {ex.GetType().Name}: {ex.Message}",
                        LogType.Error);
                    Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                    Interlocked.Increment(ref counters.Failed);
                    return;
                }

                if (ExceptionClassifier.IsTransient(ex) && n < maxAttempts)
                {
                    await Task.Delay(RetryDelayMs * n);
                    continue;
                }

                Interlocked.Increment(ref counters.Failed);
                log.WriteLine($"[W{workerId}] {rowKind} {rowIndex} FAILED after {n} attempt(s): {ex.GetType().Name}: {ex.Message}",
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
