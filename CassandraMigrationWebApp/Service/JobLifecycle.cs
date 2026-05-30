using CassandraMigrationProcessor.Models;

namespace CassandraMigrationWebApp.Service;

/// <summary>
/// Single source-of-truth display state for a job. Computed by
/// <see cref="JobManager.GetLiveStatus(Job)"/> by combining runtime
/// intent flags (pause/cancel/cutover requested) with the persisted
/// <see cref="JobStatus"/>. Never stored on disk.
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
        LiveJobStatus.Cancelled    => "bg-secondary",
        LiveJobStatus.Running      => "bg-primary",
        LiveJobStatus.Pausing      => "bg-warning text-dark",
        LiveJobStatus.Cancelling   => "bg-warning text-dark",
        LiveJobStatus.CuttingOver  => "bg-warning text-dark",
        LiveJobStatus.Paused       => "bg-warning text-dark",
        LiveJobStatus.Faulted      => "bg-danger",
        LiveJobStatus.Interrupted  => "bg-warning text-dark",
        LiveJobStatus.NotStarted   => "bg-secondary",
        _ => "bg-secondary",
    };

    /// <summary>
    /// Terminal states are non-resumable. Once a job reaches Completed
    /// or Cancelled it stays there until the operator deletes it.
    /// </summary>
    public static bool IsTerminal(LiveJobStatus s) =>
        s == LiveJobStatus.Completed || s == LiveJobStatus.Cancelled;

    /// <summary>
    /// States from which a Resume action is meaningful.
    /// </summary>
    public static bool CanResume(LiveJobStatus s) =>
        s == LiveJobStatus.NotStarted ||
        s == LiveJobStatus.Paused ||
        s == LiveJobStatus.Faulted ||
        s == LiveJobStatus.Interrupted;

    /// <summary>
    /// States from which a Pause action is meaningful (process must be
    /// live; pausing-in-progress is still allowed but a no-op).
    /// </summary>
    public static bool CanPause(LiveJobStatus s) =>
        s == LiveJobStatus.Running;

    /// <summary>
    /// States from which Cancel is meaningful. Excludes terminal states
    /// and the already-paused state (paused-then-cancel is a separate
    /// concern handled by the action toolbar).
    /// </summary>
    public static bool CanCancel(LiveJobStatus s) =>
        s == LiveJobStatus.Running ||
        s == LiveJobStatus.Pausing ||
        s == LiveJobStatus.Paused;
}
