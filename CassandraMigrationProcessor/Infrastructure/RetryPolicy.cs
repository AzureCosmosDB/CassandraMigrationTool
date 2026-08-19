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
    private readonly Func<int, TimeSpan> _delayFor;

    private RetryPolicy(int maxAttempts, Func<int, TimeSpan> delayFor)
    {
        if (maxAttempts < 1)
            throw new ArgumentOutOfRangeException(nameof(maxAttempts), "Must be >= 1.");
        MaxAttempts = maxAttempts;
        _delayFor = delayFor;
    }

    /// <summary>
    /// Returns the delay to wait before retry attempt <paramref name="attempt"/>
    /// (1-based, so the delay returned for <c>attempt=1</c> is the wait
    /// between the first failed attempt and the second attempt).
    /// </summary>
    public TimeSpan DelayBeforeRetry(int attempt) => _delayFor(attempt);

    /// <summary>
    /// Linear backoff: <c>baseDelay × attempt</c>. Matches the historical
    /// behaviour of the bulk-copy retry loop.
    /// </summary>
    public static RetryPolicy Linear(int maxAttempts, TimeSpan baseDelay)
        => new(maxAttempts, attempt => TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * attempt));

    /// <summary>
    /// Capped exponential backoff: <c>baseDelay × 2^(attempt-1)</c>,
    /// clamped to <paramref name="cap"/>. Suitable when a retry storm is
    /// likely (target throttling, e.g. 429s).
    /// </summary>
    public static RetryPolicy Exponential(int maxAttempts, TimeSpan baseDelay, TimeSpan cap)
        => new(maxAttempts, attempt =>
        {
            var ms = baseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1);
            return ms >= cap.TotalMilliseconds ? cap : TimeSpan.FromMilliseconds(ms);
        });

    /// <summary>
    /// Single-shot delay computed from a server-hinted exception via
    /// <see cref="ExceptionClassifier.GetRetryDelayMs"/>: honours
    /// <c>RetryAfterMs=NNN</c> markers in the exception's message chain,
    /// falls back to capped exponential backoff with jitter otherwise.
    /// Returned as a <see cref="TimeSpan"/> ready for <c>Task.Delay</c>.
    /// </summary>
    public static TimeSpan FromException(Exception ex, int attempt)
        => TimeSpan.FromMilliseconds(ExceptionClassifier.GetRetryDelayMs(ex, attempt));
}
