using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Context;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Helpers.JobManagement
{
    public class RetryHelper
    {
        public async Task<TaskResult> ExecuteTask(
            Func<Task<TaskResult>> taskFunc,
            Func<Exception, int, int, Task<TaskResult>> exceptionHandler,
            Log log,
            int maxTries=10,
            int initialDelayMs = 2000,
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
                    log.WriteLine($"Retrying attempt {attempt} in {delay/1000} seconds...");
                    await Task.Delay(delay, ct);
                    delay = Math.Min(delay * 2, 60_000); // Cap at 60s

                }
                catch (OperationCanceledException)
                {
                    return TaskResult.Canceled;
                }
                catch (Exception ex)
                {
                    attempt++;
                    int currentBackoffSeconds = delay / 1000;
                    var shouldRetry = await exceptionHandler(ex, attempt, currentBackoffSeconds);
                    if (shouldRetry==TaskResult.Abort || attempt >= maxTries)
                        return TaskResult.Abort;

                    if (ct.IsCancellationRequested)
                        return TaskResult.Canceled;

                    await Task.Delay(delay, ct);
                    delay = Math.Min(delay * 2, 60_000); // Cap at 60s
                }
            }
            return TaskResult.FailedAfterRetries;
        }
    }

}
