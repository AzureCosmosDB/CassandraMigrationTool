using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System.Diagnostics;
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

    public static async Task<WriteOutcome> ExecuteAsync(
        Func<Task> attempt,
        RetryPolicy policy,
        WorkerLog log,
        string rowKind,
        WriteCounters counters,
        CancellationToken cancellationToken)
    {
        var (outcome, latency, error) =
            await ExecuteWithRetryAsync(attempt, policy, log, rowKind, cancellationToken);
        ApplyToCounters(counters, outcome, latency, error);
        return outcome;
    }

    /// <summary>
    /// Executes one source row that must be written as <em>several</em>
    /// statements (per-cell TTL/writetime preservation splits a row into
    /// one partial <c>INSERT … DEFAULT UNSET</c> per distinct
    /// (writetime, ttl) group). Each group attempt gets the same bounded
    /// retry as a single-statement row, but the shared
    /// <see cref="WriteCounters"/> are advanced <em>once</em> for the whole
    /// row so page-level "all rows written" accounting stays correct.
    /// Groups run sequentially and stop at the first fatal outcome; the
    /// row's outcome is the worst of its groups (Fatal &gt; Failed &gt;
    /// Success). Re-running the page on resume is safe because every
    /// partial insert binds the source <c>USING TIMESTAMP</c>, making the
    /// writes idempotent.
    /// </summary>
    public static async Task<WriteOutcome> ExecuteRowGroupsAsync(
        IReadOnlyList<Func<Task>> groupAttempts,
        RetryPolicy policy,
        WorkerLog log,
        string rowKind,
        WriteCounters counters,
        CancellationToken cancellationToken)
    {
        var rowOutcome = WriteOutcome.Success;
        long totalLatency = 0;
        Exception? lastError = null;

        foreach (var attempt in groupAttempts)
        {
            var (outcome, latency, error) =
                await ExecuteWithRetryAsync(attempt, policy, log, rowKind, cancellationToken);
            totalLatency += latency;
            if (outcome != WriteOutcome.Success)
            {
                rowOutcome = outcome;
                lastError = error;
                // A fatal group aborts the row immediately; a non-fatal
                // failure also stops writing the remaining groups because
                // the row is already lost and will be retried whole on the
                // next page pass.
                break;
            }
        }

        ApplyToCounters(counters, rowOutcome, totalLatency, lastError);
        return rowOutcome;
    }

    /// <summary>
    /// Runs one attempt callback under the bounded-retry / classification
    /// loop WITHOUT touching <see cref="WriteCounters"/>. Returns the
    /// terminal outcome, the successful attempt's latency (0 on failure),
    /// and the last observed exception so the caller can decide how to
    /// account for it (single row vs. multi-statement row).
    /// </summary>
    private static async Task<(WriteOutcome Outcome, long LatencyMs, Exception? Error)>
        ExecuteWithRetryAsync(
            Func<Task> attempt,
            RetryPolicy policy,
            WorkerLog log,
            string rowKind,
            CancellationToken cancellationToken)
    {
        int attempts = 0;
        try
        {
            long elapsed = await RetryExecutor.ExecuteAsync(
                async (attemptNumber, _) =>
                {
                    attempts = attemptNumber;
                    var start = Stopwatch.GetTimestamp();
                    await attempt().ConfigureAwait(false);
                    return (Stopwatch.GetTimestamp() - start)
                        * 1000 / Stopwatch.Frequency;
                },
                policy,
                cancellationToken: cancellationToken);
            return (WriteOutcome.Success, elapsed, null);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (ExceptionClassifier.IsFatal(ex))
            {
                log.WriteLine(
                    $"FATAL {rowKind}: {ex.GetType().Name}: {ex.Message}",
                    LogType.Error);
                return (WriteOutcome.Fatal, 0, ex);
            }

            log.WriteLine(
                $"{rowKind} FAILED after {attempts} attempt(s): " +
                $"{ex.GetType().Name}: {ex.Message}",
                LogType.Error);
            return (WriteOutcome.Failed, 0, ex);
        }
    }

    private static void ApplyToCounters(
        WriteCounters counters, WriteOutcome outcome, long latencyMs, Exception? error)
    {
        if (outcome == WriteOutcome.Success)
        {
            Interlocked.Add(ref counters.LatencySum, latencyMs);
            Interlocked.Increment(ref counters.Done);
            return;
        }

        if (error != null) counters.LastException = error;
        Interlocked.Increment(ref counters.Failed);
    }
}
