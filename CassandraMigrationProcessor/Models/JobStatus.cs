namespace CassandraMigrationProcessor.Models;
public enum JobStatus
{
    Pending,
    Running,
    Paused,
    Completed,
    Cancelled,
    Faulted
}
