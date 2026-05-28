namespace CassandraMigrationProcessor.Models;

/// <summary>Lifecycle state of a <see cref="Job"/>.</summary>
public enum JobStatus
{
    Pending,
    Running,
    Paused,
    Completed,
    Cancelled,
    Faulted
}
