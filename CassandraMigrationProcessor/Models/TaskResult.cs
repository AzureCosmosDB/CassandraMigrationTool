namespace CassandraMigrationProcessor.Models;

/// <summary>Outcome of a retry-aware async operation: success, retry-needed, abort, or canceled.</summary>
public enum TaskResult
{
    Success,
    Retry,
    Abort,
    Canceled
}
