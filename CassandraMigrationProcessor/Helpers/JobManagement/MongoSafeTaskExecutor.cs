using System;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Helpers.JobManagement
{
    /// <summary>
    /// Executes an async operation with a timeout. Cancels
    /// the operation and optionally disposes a resource on
    /// timeout or failure.
    /// </summary>
    public static class SafeTaskExecutor
    {
        public static async Task<T> ExecuteAsync<T>(
            Func<CancellationToken, Task<T>> operation,
            int timeoutSeconds,
            string operationName,
            Action<string>? logAction = null,
            CancellationToken? externalToken = null,
            IDisposable? resourceToDispose = null)
        {
            using var cts = externalToken.HasValue
                ? CancellationTokenSource.CreateLinkedTokenSource(
                    externalToken.Value)
                : new CancellationTokenSource();

            var startTime = DateTime.UtcNow;
            var timeoutTask = Task.Delay(
                TimeSpan.FromSeconds(timeoutSeconds),
                CancellationToken.None);

            logAction?.Invoke(
                $"[START] {operationName} | " +
                $"Timeout={timeoutSeconds}s | " +
                $"Start={startTime:O}");

            var workerTask = Task.Run(async () =>
            {
                try
                {
                    var result = await operation(cts.Token);
                    logAction?.Invoke(
                        $"[TASK-END] {operationName} completed.");
                    return result;
                }
                catch (OperationCanceledException)
                {
                    logAction?.Invoke(
                        $"[CANCELLED] {operationName} cancelled.");
                    throw;
                }
                catch (Exception ex)
                {
                    logAction?.Invoke(
                        $"[EXCEPTION] {operationName}: " +
                        $"{ex.GetType().Name} - {ex}");
                    throw;
                }
            }, cts.Token);

            Task completedTask;
            try
            {
                completedTask = await Task.WhenAny(
                    workerTask, timeoutTask);
            }
            catch (Exception ex)
            {
                logAction?.Invoke(
                    $"[WHENANY-ERROR] {operationName}: {ex}");
                throw;
            }

            if (completedTask == timeoutTask)
            {
                logAction?.Invoke(
                    $"[TIMEOUT] {operationName} exceeded " +
                    $"{timeoutSeconds}s. Cancelling...");
                cts.Cancel();

                try
                {
                    await Task.WhenAny(
                        workerTask, Task.Delay(2000));
                }
                catch { }

                if (resourceToDispose != null)
                {
                    try
                    {
                        resourceToDispose.Dispose();
                    }
                    catch { }
                }

                throw new TimeoutException(
                    $"{operationName} timed out after " +
                    $"{timeoutSeconds}s");
            }

            try
            {
                var result = await workerTask;
                logAction?.Invoke(
                    $"[SUCCESS] {operationName} finished in " +
                    $"{(DateTime.UtcNow - startTime).TotalSeconds:F1}s.");
                return result;
            }
            catch (Exception ex)
            {
                logAction?.Invoke(
                    $"[FAILURE] {operationName} failed: {ex}");

                if (resourceToDispose != null)
                {
                    try { resourceToDispose.Dispose(); }
                    catch { }
                }
                throw;
            }
        }
    }
}
