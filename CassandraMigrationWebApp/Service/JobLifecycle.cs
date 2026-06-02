using CassandraMigrationProcessor.Models;

namespace CassandraMigrationWebApp.Service;

/// <summary>
/// Single source-of-truth display state for a job. Computed by
/// <see cref="JobManager.GetLiveStatus(Job)"/> by combining runtime
/// intent (<see cref="CassandraMigrationProcessor.DataTransfer.JobCommand"/>)
/// with the persisted <see cref="JobStatus"/>. Never stored on disk.
///
/// Persisted <see cref="JobStatus"/> only holds durable lifecycle
/// (Pending, Running, Paused, Completed, Cancelled, Faulted). Transient
/// states like "Pausing", "Cancelling", "CuttingOver" live only here
/// while the runner is still draining the pipeline.
/// </summary>
public enum LiveJobStatus
{
    NotStarted,
    Running,
    Pausing,
    Cancelling,
    CuttingOver,
    Paused,
    Completed,
    Cancelled,
    Faulted,
    Interrupted,
}

/// <summary>
/// UI-facing helpers for <see cref="LiveJobStatus"/>. All label/badge
/// derivation flows through this class so the Index and JobViewer
/// pages share one truth.
/// </summary>
public static class JobLifecycle
{
    public static string Label(LiveJobStatus s) => s switch
    {
        LiveJobStatus.NotStarted   => "Not Started",
        LiveJobStatus.Running      => "Running",
        LiveJobStatus.Pausing      => "Pausing...",
        LiveJobStatus.Cancelling   => "Cancelling...",
        LiveJobStatus.CuttingOver  => "Cutting over...",
        LiveJobStatus.Paused       => "Paused",
        LiveJobStatus.Completed    => "Completed",
        LiveJobStatus.Cancelled    => "Cancelled",
        LiveJobStatus.Faulted      => "Faulted",
        LiveJobStatus.Interrupted  => "Interrupted",
        _ => s.ToString(),
    };

    public static string BadgeClass(LiveJobStatus s) => s switch
    {
        LiveJobStatus.Completed    => "bg-success",
        LiveJobStatus.Cancelled    => "bg-dark",
        LiveJobStatus.Running      => "bg-primary",
        LiveJobStatus.Faulted      => "bg-danger",
        LiveJobStatus.NotStarted   => "bg-secondary",
        LiveJobStatus.Pausing
            or LiveJobStatus.Cancelling
            or LiveJobStatus.CuttingOver
            or LiveJobStatus.Paused
            or LiveJobStatus.Interrupted => "bg-warning text-dark",
        _ => "bg-secondary",
    };

    /// <summary>
    /// States from which a Resume action is meaningful.
    /// </summary>
    public static bool CanResume(LiveJobStatus s) =>
        s is LiveJobStatus.NotStarted
          or LiveJobStatus.Paused
          or LiveJobStatus.Faulted
          or LiveJobStatus.Interrupted;

    /// <summary>
    /// States from which a Pause action is meaningful (process must be
    /// live; pausing-in-progress is still allowed but a no-op).
    /// </summary>
    public static bool CanPause(LiveJobStatus s) =>
        s == LiveJobStatus.Running;

    /// <summary>
    /// True when every table in the job has either drained its bulk
    /// copy or never existed at the source — i.e. the operator can
    /// safely cutover without leaving rows un-migrated. Returns false
    /// for a job with zero tables (All() over an empty list would
    /// otherwise vacuously enable cutover on an invalid job).
    /// </summary>
    public static bool IsCutoverReady(Job? job) =>
        job != null && job.Tables.Count > 0 && job.Tables.All(mu =>
            mu.SourceStatus == TableStatus.NotFound || mu.CopyComplete);

    /// <summary>
    /// Format an ETA from a remaining-row count and a current rows/sec
    /// throughput. Mirrors the "Xs / Xm Xs / Xh Xm" stepping used by
    /// the homepage list and the job-summary card so both surfaces
    /// render identical strings for identical inputs.
    /// Returns an empty string when the throughput is non-positive or
    /// no work remains.
    /// </summary>
    public static string FormatEta(long remainingRows, double rowsPerSecond)
    {
        if (rowsPerSecond <= 0 || remainingRows <= 0)
            return string.Empty;

        double etaSec = remainingRows / rowsPerSecond;
        if (etaSec < 60) return $"{etaSec:F0}s";
        if (etaSec < 3600) return $"{etaSec/60:F0}m {etaSec%60:F0}s";
        return $"{etaSec/3600:F0}h {(etaSec%3600)/60:F0}m";
    }
}
