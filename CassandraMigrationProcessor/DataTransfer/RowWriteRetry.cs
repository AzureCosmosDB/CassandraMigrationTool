using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Shared bounded-retry helper for per-row writes used by every
/// <see cref="IRowWriteStrategy"/>. Owns the attempt loop, latency
/// timing, fatal/transient exception classification, error logging,
/// fatal-flag propagation, and <see cref="WriteCounters"/> accounting.
/// Strategies provide only the per-attempt body via
/// <paramref name="attempt"/> and a <see cref="RetryPolicy"/> that
/// controls max attempts and inter-attempt backoff — that callback is
/// re-invoked on every retry, which is what makes counter
/// read-modify-write idempotent (each attempt re-reads the current
/// target before writing).
/// </summary>
internal static class RowWriteRetry
{
    public const int WriteTimeoutMs = 60_000;

    public static async Task ExecuteAsync(
        Func<Task> attempt,
        RetryPolicy policy,
        WorkerLog log,
        int rowIndex,
        string rowKind,
        Action onFatal,
        WriteCounters counters)
    {
        for (int n = 1; n <= policy.MaxAttempts; n++)
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
                    log.WriteLine($"FATAL {rowKind} {rowIndex}: {ex.GetType().Name}: {ex.Message}",
                        LogType.Error);
                    onFatal();
                    Interlocked.Increment(ref counters.Failed);
                    return;
                }

                if (ExceptionClassifier.IsTransient(ex) && n < policy.MaxAttempts)
                {
                    await Task.Delay(policy.DelayBeforeRetry(n));
                    continue;
                }

                Interlocked.Increment(ref counters.Failed);
                log.WriteLine($"{rowKind} {rowIndex} FAILED after {n} attempt(s): {ex.GetType().Name}: {ex.Message}",
                    LogType.Error);

                if (!ExceptionClassifier.IsTransient(ex))
                {
                    onFatal();
                }
                return;
            }
        }
    }
}
