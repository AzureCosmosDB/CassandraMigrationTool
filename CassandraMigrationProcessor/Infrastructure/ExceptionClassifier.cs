using Cassandra;

namespace CassandraMigrationProcessor.Infrastructure;
/// <summary>
/// Centralized exception classification for Cassandra operations.
/// Transient and fatal sets are registrable so new exception types
/// can be added without modifying this class (Open/Closed principle).
/// </summary>
public static class ExceptionClassifier
{
    private static readonly HashSet<Type> _transientTypes = new()
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
    };

    private static readonly HashSet<Type> _fatalTypes = new()
    {
        typeof(AuthenticationException),
        typeof(UnauthorizedException),
        typeof(InvalidQueryException),
        typeof(SyntaxError),
    };

    /// <summary>
    /// Transient errors that should be retried.
    /// </summary>
    public static bool IsTransient(Exception ex)
    {
        var inner = UnwrapAggregate(ex);

        if (_transientTypes.Contains(inner.GetType()))
            return true;

        // Throttle markers may live in outer or inner exception text
        // (e.g. NoHostAvailableException wrapping a 429 response from
        // Cosmos DB). Run the same classifier we use elsewhere.
        return IsThrottle(ex);
    }

    /// <summary>
    /// Fatal errors that should stop the entire job.
    /// </summary>
    public static bool IsFatal(Exception ex)
    {
        var inner = UnwrapAggregate(ex);
        return _fatalTypes.Contains(inner.GetType());
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

    /// <summary>
    /// Compute the recommended retry delay for an attempt. If the error
    /// carries a <c>RetryAfterMs=NNN</c> hint (Cosmos DB throttle
    /// response) honour it with small jitter; otherwise fall back to
    /// capped exponential backoff (1s, 2s, 4s, …) with jitter.
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

        return (int)(Math.Pow(2, attempt - 1) * 1000) + Random.Shared.Next(100, 500);
    }

    private static Exception UnwrapAggregate(Exception ex)
        => ex is AggregateException agg && agg.InnerException != null
            ? agg.InnerException : ex;

    private static string BuildMessageChain(Exception ex)
    {
        if (ex.InnerException == null)
            return ex.Message ?? string.Empty;
        return (ex.Message ?? string.Empty) + " " + (ex.InnerException.Message ?? string.Empty);
    }
}
