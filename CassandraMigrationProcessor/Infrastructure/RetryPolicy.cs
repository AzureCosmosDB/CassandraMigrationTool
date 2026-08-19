namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Immutable retry configuration. Encapsulates the max attempt count
/// and the inter-attempt backoff so retry callers (per-row writes,
/// table-level retries, ad-hoc transient loops) share a single
/// replaceable knob instead of separate int/const parameters.
/// <para>
/// Use the <see cref="Linear"/> factory for the default linear-backoff
/// policy (<c>BaseDelay × attempt</c>), <see cref="Exponential"/> for
/// capped exponential backoff (deterministic, suited for tests), or
/// <see cref="FromException"/> to honour server-supplied
/// <c>RetryAfterMs</c> hints with exponential fallback.
/// </para>
/// </summary>
public sealed class RetryPolicy
{
    public int MaxAttempts { get; }
    private readonly Predicate<Exception> _shouldRetry;
    private readonly Func<Exception, int, TimeSpan> _delayFor;

    private RetryPolicy(
        int maxAttempts,
        Predicate<Exception> shouldRetry,
        Func<Exception, int, TimeSpan> delayFor)
    {
        if (maxAttempts < 1)
            throw new ArgumentOutOfRangeException(nameof(maxAttempts), "Must be >= 1.");
        MaxAttempts = maxAttempts;
        _shouldRetry = shouldRetry
            ?? throw new ArgumentNullException(nameof(shouldRetry));
        _delayFor = delayFor
            ?? throw new ArgumentNullException(nameof(delayFor));
    }

    public bool ShouldRetry(Exception exception)
    {
        return _shouldRetry(exception);
    }

    /// <summary>
    /// Returns the delay to wait before retry attempt <paramref name="attempt"/>
    /// (1-based, so the delay returned for <c>attempt=1</c> is the wait
    /// between the first failed attempt and the second attempt).
    /// </summary>
    public TimeSpan DelayBeforeRetry(Exception exception, int attempt)
    {
        return _delayFor(exception, attempt);
    }

    public static RetryPolicy Create(
        int maxAttempts,
        Predicate<Exception> shouldRetry,
        Func<Exception, int, TimeSpan> delayFor)
    {
        return new RetryPolicy(maxAttempts, shouldRetry, delayFor);
    }

    /// <summary>
    /// Linear backoff: <c>baseDelay × attempt</c>. Matches the historical
    /// behaviour of the bulk-copy retry loop.
    /// </summary>
    public static RetryPolicy Linear(
        int maxAttempts,
        TimeSpan baseDelay,
        Predicate<Exception>? shouldRetry = null)
    {
        return new RetryPolicy(
            maxAttempts,
            shouldRetry ?? ExceptionClassifier.IsTransient,
            (_, attempt) => TimeSpan.FromMilliseconds(
                baseDelay.TotalMilliseconds * attempt));
    }

    /// <summary>
    /// Capped exponential backoff: <c>baseDelay × 2^(attempt-1)</c>,
    /// clamped to <paramref name="cap"/>. Suitable when a retry storm is
    /// likely (target throttling, e.g. 429s).
    /// </summary>
    public static RetryPolicy Exponential(
        int maxAttempts,
        TimeSpan baseDelay,
        TimeSpan cap,
        Predicate<Exception>? shouldRetry = null)
    {
        return new RetryPolicy(
            maxAttempts,
            shouldRetry ?? ExceptionClassifier.IsTransient,
            (_, attempt) =>
            {
                var ms = baseDelay.TotalMilliseconds
                    * Math.Pow(2, attempt - 1);
                return ms >= cap.TotalMilliseconds
                    ? cap
                    : TimeSpan.FromMilliseconds(ms);
            });
    }

    public static RetryPolicy Transient(
        int maxAttempts = MigrationDefaults.TransientRetryMaxAttempts,
        int minimumDelayMs = MigrationDefaults.TransientRetryBaseDelayMs)
    {
        return new RetryPolicy(
            maxAttempts,
            ExceptionClassifier.IsTransient,
            (exception, attempt) => TimeSpan.FromMilliseconds(
                Math.Max(
                    ExceptionClassifier.GetRetryDelayMs(exception, attempt),
                    attempt * minimumDelayMs)));
    }

    /// <summary>
    /// Single-shot delay computed from a server-hinted exception via
    /// <see cref="ExceptionClassifier.GetRetryDelayMs"/>: honours
    /// <c>RetryAfterMs=NNN</c> markers in the exception's message chain,
    /// falls back to capped exponential backoff with jitter otherwise.
    /// Returned as a <see cref="TimeSpan"/> ready for <c>Task.Delay</c>.
    /// </summary>
    public static TimeSpan FromException(Exception ex, int attempt)
    {
        return TimeSpan.FromMilliseconds(
            ExceptionClassifier.GetRetryDelayMs(ex, attempt));
    }
}
