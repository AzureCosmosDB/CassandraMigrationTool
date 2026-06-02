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

public static class JobStatusExtensions
{
    /// <summary>
    /// True when the job has reached a final lifecycle state:
    /// <see cref="JobStatus.Completed"/>, <see cref="JobStatus.Faulted"/>,
    /// or <see cref="JobStatus.Cancelled"/>. Used to gate "stamp EndedOn",
    /// "drop runtime credentials", and "online-replay still active"
    /// decisions consistently across the processor and the web UI.
    /// </summary>
    public static bool IsTerminal(this JobStatus status)
        => status is JobStatus.Completed or JobStatus.Faulted or JobStatus.Cancelled;
}
