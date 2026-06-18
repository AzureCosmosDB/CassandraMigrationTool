using Cassandra;
using CassandraMigrationProcessor.DataTransfer;

namespace CassandraMigrationProcessor.Infrastructure;
/// <summary>
/// Centralized exception classification for Cassandra operations.
/// Transient and fatal sets are registrable so new exception types
/// can be added without modifying this class (Open/Closed principle).
/// </summary>
public static class ExceptionClassifier
{
    // Subclass-aware list (matched via IsInstanceOfType, not type
    // equality) so a future driver release that introduces a more
    // specific subclass — e.g. ServerOverloadedException : OverloadedException —
    // continues to classify as transient without code changes.
    private static readonly Type[] _transientBases = new[]
    {
        typeof(NoHostAvailableException),
        typeof(WriteTimeoutException),
        typeof(ReadTimeoutException),
        typeof(UnavailableException),
        typeof(OverloadedException),
        typeof(TimeoutException),
        typeof(System.IO.IOException),
        typeof(System.Net.Sockets.SocketException),
        typeof(ObjectDisposedException),
        typeof(MigrationTransientException),
    };

    private static readonly Type[] _fatalBases = new[]
    {
        typeof(AuthenticationException),
        typeof(UnauthorizedException),
        typeof(InvalidQueryException),
        typeof(SyntaxError),
        typeof(MigrationFatalException),
        typeof(MigrationSchemaException),
        typeof(MigrationAuthException),
    };

    private static bool IsKindOf(Exception ex, Type[] bases)
    {
        for (int i = 0; i < bases.Length; i++)
            if (bases[i].IsInstanceOfType(ex))
                return true;
        return false;
    }

    /// <summary>
    /// Transient errors that should be retried.
    /// </summary>
    public static bool IsTransient(Exception ex)
    {
        var inner = UnwrapAggregate(ex);

        // Fatal short-circuits FIRST. The previous fallthrough
        // evaluated IsThrottle(ex) on the original aggregate after
        // UnwrapAggregate had already picked a fatal inner — so an
        // aggregate that contained {fatal, throttle} returned true
        // here and RetryExecutor would retry a fatal failure
        // indefinitely. Classify off the unwrapped exception only,
        // and gate throttle behind the fatal check.
        if (IsKindOf(inner, _fatalBases)) return false;

        if (IsKindOf(inner, _transientBases))
            return true;

        // Throttle markers may live in outer or inner exception text
        // (e.g. NoHostAvailableException wrapping a 429 response from
        // Cosmos DB). Check the unwrapped chain — but only after the
        // fatal short-circuit above.
        return IsThrottle(inner);
    }

    /// <summary>
    /// Fatal errors that should stop the entire job.
    /// </summary>
    public static bool IsFatal(Exception ex)
    {
        var inner = UnwrapAggregate(ex);
        return IsKindOf(inner, _fatalBases);
    }

    /// <summary>
    /// Whether the error indicates the table/resource
    /// does not exist (vs a transient failure).
    /// </summary>
    public static bool IsNotFound(Exception ex)
    {
        var inner = UnwrapAggregate(ex);

        if (inner is InvalidQueryException iqe)
        {
            var msg = iqe.Message ?? string.Empty;
            return msg.Contains("unconfigured table", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("not found", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("does not exist", StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    /// <summary>
    /// Whether the error is a rate-limit / throttle. Detects both
    /// typed <see cref="OverloadedException"/> and message-level markers
    /// (Cosmos DB exposes 429s via NoHostAvailable / DriverException
    /// with the throttle text in the message chain).
    /// </summary>
    public static bool IsThrottle(Exception ex)
    {
        if (UnwrapAggregate(ex) is OverloadedException) return true;

        var msg = BuildMessageChain(ex);
        return msg.Contains("429", StringComparison.Ordinal)
            || msg.Contains("TooManyRequests", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("OverloadedException", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("Request rate is large", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("RetryAfterMs", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("rate limit", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Whether the error is an authentication / authorisation failure.
    /// Handles direct <see cref="AuthenticationException"/>, wrapping via
    /// <see cref="Exception.InnerException"/>, and Cassandra driver's
    /// <see cref="NoHostAvailableException"/> that aggregates per-host
    /// errors in its <c>Errors</c> dictionary.
    /// </summary>
    public static bool IsAuth(Exception ex)
    {
        if (ex is AuthenticationException) return true;
        if (ex.InnerException is AuthenticationException) return true;
        if (ex is NoHostAvailableException nhae)
            return nhae.Errors?.Values?.Any(e => e is AuthenticationException) ?? false;
        return false;
    }

    private const int MaxExponentAttempt = 10;
    private const int MaxBackoffMs = 30_000;

    /// <summary>
    /// Compute the recommended retry delay for an attempt. If the error
    /// carries a <c>RetryAfterMs=NNN</c> hint (Cosmos DB throttle
    /// response) honour it with small jitter; otherwise fall back to
    /// capped exponential backoff (1s, 2s, 4s, …) with jitter.
    /// The exponent is clamped at <see cref="MaxExponentAttempt"/> so a
    /// long-lived job whose attempt counter climbs past 10 doesn't
    /// schedule a multi-day sleep on the next backoff.
    /// <para>
    /// Pure computation; lives here only so the static classifier can
    /// share one implementation. See <see cref="RetryPolicy.FromException"/>
    /// for the policy-shaped wrapper preferred by new callers.
    /// </para>
    /// </summary>
    public static int GetRetryDelayMs(Exception ex, int attempt)
    {
        var msg = BuildMessageChain(ex);
        var idx = msg.IndexOf("RetryAfterMs=", StringComparison.OrdinalIgnoreCase);
        if (idx >= 0)
        {
            var start = idx + "RetryAfterMs=".Length;
            var end = start;
            while (end < msg.Length && char.IsDigit(msg[end])) end++;
            if (end > start
                && int.TryParse(msg.AsSpan(start, end - start), out var retryMs)
                && retryMs > 0)
            {
                return retryMs + Random.Shared.Next(100, 500);
            }
        }

        var clamped = Math.Min(Math.Max(attempt, 1), MaxExponentAttempt);
        var backoff = (int)(Math.Pow(2, clamped - 1) * 1000)
            + Random.Shared.Next(100, 500);
        return Math.Min(backoff, MaxBackoffMs);
    }

    private static Exception UnwrapAggregate(Exception ex)
    {
        // Walk until we leave the AggregateException chain. Task.WhenAll
        // of nested Task.Run can wrap an Aggregate inside an Aggregate;
        // a single Unwrap missed the real inner type and the caller
        // received a generic Exception classification.
        while (ex is AggregateException ae)
        {
            // Defensive: a manually-constructed AggregateException
            // can carry zero inners. Indexing [0] would throw
            // IndexOutOfRangeException from inside a catch-when
            // filter, silently re-evaluating the filter to false
            // and masking the real failure. Return the aggregate
            // itself in that case so the classifier sees *something*
            // and the caller's handlers fire normally.
            if (ae.InnerExceptions.Count == 0)
                return ae;
            if (ae.InnerExceptions.Count == 1)
            {
                ex = ae.InnerExceptions[0];
                continue;
            }
            // Multi-inner aggregate: prefer the most specific known
            // classification (auth > fatal > throttle > transient).
            var preferred =
                ae.InnerExceptions.FirstOrDefault(e => IsKindOf(e, _fatalBases))
                ?? ae.InnerExceptions.FirstOrDefault(IsThrottle)
                ?? ae.InnerExceptions.FirstOrDefault(e => IsKindOf(e, _transientBases))
                ?? ae.InnerExceptions[0];
            return preferred;
        }
        return ex;
    }

    private static string BuildMessageChain(Exception ex)
    {
        // Walks the entire AggregateException + InnerException chain so
        // marker text (RetryAfterMs=NNN, 429, "Request rate is large")
        // is detected even when the driver buries the underlying
        // server response several wrappers deep. The previous
        // single-level walk silently missed deep throttle markers and
        // GetRetryDelayMs fell back to plain exponential backoff,
        // ignoring the server's hint.
        var sb = new System.Text.StringBuilder(ex.Message ?? string.Empty);
        foreach (var e in Walk(ex))
        {
            if (ReferenceEquals(e, ex)) continue;
            if (!string.IsNullOrEmpty(e.Message))
            {
                sb.Append(' ').Append(e.Message);
            }
        }
        return sb.ToString();
    }

    /// <summary>
    /// Flat enumeration of <paramref name="ex"/> and every wrapped
    /// inner / aggregate inner, up to <paramref name="maxDepth"/> hops.
    /// Walks both <see cref="Exception.InnerException"/> and the
    /// <see cref="AggregateException.InnerExceptions"/> list (after
    /// <see cref="AggregateException.Flatten"/>); the depth cap guards
    /// against pathological cycles. Consolidates the five previous
    /// ad-hoc walkers (DataCopyWorker.LogExceptionChain, IsOutOfMemory,
    /// WorkerPool fault-flush, ExceptionClassifier.UnwrapAggregate,
    /// BuildMessageChain) so all classification text-search paths see
    /// the same chain.
    /// </summary>
    public static IEnumerable<Exception> Walk(Exception? ex, int maxDepth = 8)
    {
        if (ex == null) yield break;
        var stack = new Stack<(Exception ex, int depth)>();
        var seen = new HashSet<Exception>(ReferenceEqualityComparer.Instance);
        stack.Push((ex, 0));
        while (stack.Count > 0)
        {
            var (cur, depth) = stack.Pop();
            if (cur == null || depth > maxDepth) continue;
            if (!seen.Add(cur)) continue;
            yield return cur;
            if (cur is AggregateException agg)
            {
                foreach (var inner in agg.Flatten().InnerExceptions)
                {
                    if (inner != null) stack.Push((inner, depth + 1));
                }
            }
            if (cur.InnerException != null)
                stack.Push((cur.InnerException, depth + 1));
        }
    }
}
