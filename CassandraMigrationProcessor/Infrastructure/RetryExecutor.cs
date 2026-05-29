namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Shared transient-fault retry helper. Wraps an async operation with
/// linear backoff retry on transient Cassandra exceptions (timeouts,
/// throttles, transport errors as classified by
/// <see cref="ExceptionClassifier.IsTransient"/>). Caller-agnostic —
/// nothing here is schema- or query-specific; see callers in
/// SchemaManager, PageReader, etc.
/// </summary>
internal static class RetryExecutor
{
    /// <summary>
    /// Execute an async operation with retry on transient errors.
    /// Delay between attempts is <c>attempt * baseDelayMs</c> (linear).
    /// Defaults come from <see cref="MigrationDefaults.TransientRetryMaxAttempts"/>
    /// and <see cref="MigrationDefaults.TransientRetryBaseDelayMs"/>;
    /// pass explicit values for hot read/write paths.
    /// </summary>
    public static async Task<T> ExecuteAsync<T>(
        Func<Task<T>> operation,
        int maxRetries = MigrationDefaults.TransientRetryMaxAttempts,
        int baseDelayMs = MigrationDefaults.TransientRetryBaseDelayMs)
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
