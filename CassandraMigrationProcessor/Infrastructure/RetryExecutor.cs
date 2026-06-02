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
    /// Delay between attempts is taken from
    /// <see cref="ExceptionClassifier.GetRetryDelayMs"/> (which honours
    /// server <c>RetryAfterMs</c> hints, applies exponential backoff
    /// with jitter, and caps the per-sleep ceiling). The supplied
    /// cancellation token is honoured both during the operation and
    /// during the backoff sleep, so Stop observes promptly instead
    /// of waiting for the next retry timer to fire.
    /// </summary>
    public static async Task<T> ExecuteAsync<T>(
        Func<Task<T>> operation,
        int maxRetries = MigrationDefaults.TransientRetryMaxAttempts,
        int baseDelayMs = MigrationDefaults.TransientRetryBaseDelayMs,
        CancellationToken cancellationToken = default)
    {
        Exception? lastException = null;
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (attempt < maxRetries
                && ExceptionClassifier.IsTransient(ex))
            {
                lastException = ex;
                var delay = Math.Max(
                    ExceptionClassifier.GetRetryDelayMs(ex, attempt),
                    attempt * baseDelayMs);
                await Task.Delay(delay, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        throw lastException ?? new TimeoutException("Operation timed out after all retries");
    }

    /// <summary>
    /// Non-generic overload for fire-and-forget operations.
    /// </summary>
    public static Task ExecuteAsync(
        Func<Task> operation,
        int maxRetries = MigrationDefaults.TransientRetryMaxAttempts,
        int baseDelayMs = MigrationDefaults.TransientRetryBaseDelayMs,
        CancellationToken cancellationToken = default)
    {
        return ExecuteAsync<int>(
            async () => { await operation().ConfigureAwait(false); return 0; },
            maxRetries, baseDelayMs, cancellationToken);
    }
}
