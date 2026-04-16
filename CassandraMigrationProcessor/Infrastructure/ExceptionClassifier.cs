using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;

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

    public static void RegisterTransient(Type exceptionType) => _transientTypes.Add(exceptionType);
    public static void RegisterFatal(Type exceptionType) => _fatalTypes.Add(exceptionType);

    /// <summary>
    /// Transient errors that should be retried.
    /// </summary>
    public static bool IsTransient(Exception ex)
    {
        var inner = UnwrapAggregate(ex);

        if (_transientTypes.Contains(inner.GetType()))
            return true;

        // Cosmos DB 429 throttling (message-based, not type-based)
        var msg = inner.Message ?? string.Empty;
        if (msg.Contains("429")
            || msg.Contains("TooManyRequests", StringComparison.OrdinalIgnoreCase))
            return true;

        return false;
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
    /// Whether the error is a rate-limit / throttle.
    /// </summary>
    public static bool IsThrottle(Exception ex)
    {
        if (ex is OverloadedException) return true;

        var msg = ex.Message ?? string.Empty;
        return msg.Contains("429")
            || msg.Contains("TooManyRequests", StringComparison.OrdinalIgnoreCase)
            || msg.Contains("rate is large", StringComparison.OrdinalIgnoreCase);
    }

    private static Exception UnwrapAggregate(Exception ex)
        => ex is AggregateException agg && agg.InnerException != null
            ? agg.InnerException : ex;
}
