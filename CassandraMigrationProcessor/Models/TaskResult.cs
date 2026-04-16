namespace CassandraMigrationProcessor.Models;
public enum TaskResult
{
    Success,
    Retry,
    Abort,
    FailedAfterRetries,
    Canceled
}
