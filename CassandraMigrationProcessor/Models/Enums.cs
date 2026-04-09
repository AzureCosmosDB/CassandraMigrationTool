namespace CassandraMigrationProcessor.Models
{
    /// <summary>
    /// CDC mode for Cassandra migration (change feed).
    /// </summary>
    public enum CDCMode
    {
        Offline,
        Online
    }

    public enum JobStatus
    {
        Pending,
        Running,
        Paused,
        Completed,
        Cancelled,
        Faulted
    }

    public enum TaskResult
    {
        Success,
        Retry,
        Abort,
        FailedAfterRetries,
        Canceled,
        HasMore
    }

    public enum JobType
    {
        CqlCopy,
    }
}
