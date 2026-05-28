namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Shared retry helper used by schema/query code paths. Wraps an async
/// operation with linear backoff retry on transient Cassandra exceptions
/// (timeouts, throttles, transport errors as classified by
/// <see cref="ExceptionClassifier.IsTransient"/>).
/// </summary>
internal static class RetryExecutor
{
    /// <summary>
    /// Execute an async operation with retry on transient errors.
    /// Delay between attempts is <c>attempt * baseDelayMs</c> (linear).
    /// </summary>
    public static async Task<T> ExecuteWithTimeoutRetryAsync<T>(
        Func<Task<T>> operation,
        int maxRetries = MigrationDefaults.SchemaQueryMaxRetries,
        int baseDelayMs = MigrationDefaults.SchemaQueryRetryBaseDelayMs)
    {
        Exception? lastException = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (Exception ex) when (attempt < maxRetries
                && ExceptionClassifier.IsTransient(ex))
            {
                lastException = ex;
                await Task.Delay(attempt * baseDelayMs);
            }
        }
        throw lastException ?? new TimeoutException("Operation timed out after all retries");
    }
}
