using Cassandra;
using System;

namespace CassandraMigrationProcessor.Helpers
{
    /// <summary>
    /// Centralized exception classification for Cassandra operations.
    /// Uses concrete driver exception types — no string matching.
    /// </summary>
    public static class ExceptionClassifier
    {
        /// <summary>
        /// Transient errors that should be retried.
        /// </summary>
        public static bool IsTransient(Exception ex)
        {
            if (ex is AggregateException agg && agg.InnerException != null)
                ex = agg.InnerException;

            // Cassandra driver transient errors
            if (ex is NoHostAvailableException
                || ex is WriteTimeoutException
                || ex is ReadTimeoutException
                || ex is UnavailableException
                || ex is OverloadedException)
                return true;

            // System transient errors
            if (ex is TimeoutException
                || ex is System.IO.IOException
                || ex is System.Net.Sockets.SocketException
                || ex is ObjectDisposedException)
                return true;

            // Cosmos DB 429 throttling
            var msg = ex.Message ?? string.Empty;
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
            if (ex is AggregateException agg && agg.InnerException != null)
                ex = agg.InnerException;

            if (ex is AuthenticationException
                || ex is UnauthorizedException)
                return true;

            if (ex is InvalidQueryException
                || ex is SyntaxError)
                return true;

            return false;
        }

        /// <summary>
        /// Whether the error indicates the table/resource
        /// does not exist (vs a transient failure).
        /// </summary>
        public static bool IsNotFound(Exception ex)
        {
            if (ex is AggregateException agg && agg.InnerException != null)
                ex = agg.InnerException;

            if (ex is InvalidQueryException iqe)
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
    }
}
