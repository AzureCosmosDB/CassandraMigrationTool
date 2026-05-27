using System;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Immutable retry configuration for per-row writes. Encapsulates the
/// max attempt count and the inter-attempt backoff so the
/// <see cref="RowWriteRetry"/> helper has a single, replaceable knob
/// instead of separate int/const parameters.
/// <para>
/// Use the <see cref="Linear"/> factory for the default linear-backoff
/// policy (<c>BaseDelay × attempt</c>), or the <see cref="Exponential"/>
/// factory for capped exponential backoff. Both stay deterministic so
/// behaviour stays predictable in tests.
/// </para>
/// </summary>
internal sealed class RetryPolicy
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
}
