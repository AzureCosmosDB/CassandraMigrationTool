namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Executes synchronous and asynchronous operations using caller-provided
/// retry policies. Operation-specific exception classification, delay, and
/// logging remain outside the executor.
/// </summary>
internal static class RetryExecutor
{
    public static async Task<T> ExecuteAsync<T>(
        Func<int, CancellationToken, Task<T>> operation,
        RetryPolicy policy,
        Action<Exception, int>? onRetry = null,
        Action<Exception, int>? onFailure = null,
        CancellationToken cancellationToken = default)
    {
        var result = await ExecuteCoreAsync(
            operation, policy, onRetry, onFailure, cancellationToken)
            .ConfigureAwait(false);
        if (result.Succeeded)
            return result.Value!;

        System.Runtime.ExceptionServices.ExceptionDispatchInfo
            .Capture(result.Exception!)
            .Throw();
        throw new System.Diagnostics.UnreachableException();
    }

    public static Task<T> ExecuteAsync<T>(
        Func<Task<T>> operation,
        CancellationToken cancellationToken = default)
    {
        return ExecuteAsync(
            (_, _) => operation(),
            RetryPolicy.Transient(),
            cancellationToken: cancellationToken);
    }

    public static Task ExecuteAsync(
        Func<Task> operation,
        CancellationToken cancellationToken = default)
    {
        return ExecuteAsync(
            async (_, _) =>
            {
                await operation().ConfigureAwait(false);
                return true;
            },
            RetryPolicy.Transient(),
            cancellationToken: cancellationToken);
    }

    public static T Execute<T>(
        Func<int, T> operation,
        RetryPolicy policy,
        Action<Exception, int>? onRetry = null)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return operation(attempt);
            }
            catch (Exception ex) when (
                attempt < policy.MaxAttempts
                && policy.ShouldRetry(ex))
            {
                onRetry?.Invoke(ex, attempt);
                Thread.Sleep(policy.DelayBeforeRetry(ex, attempt));
            }
        }
    }

    public static async Task<T?> ExecuteOrDefaultAsync<T>(
        Func<int, CancellationToken, Task<T>> operation,
        RetryPolicy policy,
        Action<Exception, int>? onRetry = null,
        Action<Exception, int>? onFailure = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var result = await ExecuteCoreAsync(
            operation, policy, onRetry, onFailure, cancellationToken)
            .ConfigureAwait(false);
        return result.Succeeded ? result.Value : null;
    }

    private static async Task<RetryResult<T>> ExecuteCoreAsync<T>(
        Func<int, CancellationToken, Task<T>> operation,
        RetryPolicy policy,
        Action<Exception, int>? onRetry,
        Action<Exception, int>? onFailure,
        CancellationToken cancellationToken)
    {
        for (int attempt = 1; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var value = await operation(attempt, cancellationToken)
                    .ConfigureAwait(false);
                return RetryResult<T>.Success(value);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (policy.ShouldRetry(ex))
            {
                onFailure?.Invoke(ex, attempt);
                if (attempt >= policy.MaxAttempts)
                    return RetryResult<T>.Failure(ex);

                onRetry?.Invoke(ex, attempt);
                await Task.Delay(
                        policy.DelayBeforeRetry(ex, attempt),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private readonly record struct RetryResult<T>(
        bool Succeeded,
        T? Value,
        Exception? Exception)
    {
        public static RetryResult<T> Success(T value)
        {
            return new RetryResult<T>(true, value, null);
        }

        public static RetryResult<T> Failure(Exception exception)
        {
            return new RetryResult<T>(false, default, exception);
        }
    }
}
