using CassandraMigrationProcessor.Models;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Infrastructure;
public class RetryHelper
{
    private const int DefaultMaxTries = 10;
    private const int DefaultInitialDelayMs = 2000;
    private const int MaxBackoffMs = 60_000;

    private static int EscalateDelay(int currentDelay)
        => Math.Min(currentDelay * 2, MaxBackoffMs);

    public async Task<TaskResult> ExecuteWithRetryAsync(
        Func<Task<TaskResult>> taskFunc,
        Func<Exception, int, int, Task<TaskResult>> exceptionHandler,
        MigrationLog MigrationLog,
        int maxTries = DefaultMaxTries,
        int initialDelayMs = DefaultInitialDelayMs,
        CancellationToken ct = default)
    {
        int attempt = 0;
        int delay = initialDelayMs;

        while (attempt < maxTries)
        {
            if (ct.IsCancellationRequested)
                return TaskResult.Canceled;

            try
            {
                var result = await taskFunc();
                if (result == TaskResult.Canceled)
                    return TaskResult.Canceled;
                if (result == TaskResult.Abort)
                    return TaskResult.Abort;
                if (result != TaskResult.Retry)
                    return result;

                attempt++;
                MigrationLog.WriteLine($"Retrying attempt {attempt} in {delay / 1000} seconds...");
                await Task.Delay(delay, ct);
                delay = EscalateDelay(delay);

            }
            catch (OperationCanceledException)
            {
                return TaskResult.Canceled;
            }
            catch (Exception ex)
            {
                attempt++;
                int currentBackoffSeconds = delay / 1000;
                MigrationLog.WriteLine(
                    $"Retry attempt {attempt} caught {ex.GetType().FullName}: {ex.Message}",
                    LogType.Error);
                if (ex.StackTrace != null)
                    MigrationLog.WriteLine($"  at {ex.StackTrace}", LogType.Error);
                if (ex.InnerException != null)
                    MigrationLog.WriteLine(
                        $"  Inner: {ex.InnerException.GetType().FullName}: {ex.InnerException.Message}",
                        LogType.Error);

                // Built-in fatal short-circuit. Even if a caller-supplied handler
                // mistakenly returns Retry for AuthenticationException /
                // InvalidQueryException / SyntaxError / UnauthorizedException,
                // we refuse to retry — these are permanent and infinite retry
                // hides the misconfiguration from the operator.
                if (ExceptionClassifier.IsFatal(ex))
                {
                    MigrationLog.WriteLine(
                        $"Fatal exception type {ex.GetType().Name} — not retrying",
                        LogType.Error);
                    return TaskResult.Abort;
                }

                var shouldRetry = await exceptionHandler(ex, attempt, currentBackoffSeconds);
                if (shouldRetry == TaskResult.Canceled)
                    return TaskResult.Canceled;
                if (shouldRetry == TaskResult.Abort || attempt >= maxTries)
                    return TaskResult.Abort;

                if (ct.IsCancellationRequested)
                    return TaskResult.Canceled;

                await Task.Delay(delay, ct);
                delay = EscalateDelay(delay);
            }
        }
        return TaskResult.FailedAfterRetries;
    }
}
