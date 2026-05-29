namespace CassandraMigrationProcessor.Context;

/// <summary>
/// Per-run pause coordination owned by a single
/// <see cref="DataTransfer.MigrationJobRunner"/>. Replaces the
/// process-wide pause flag + event that used to live on
/// <see cref="MigrationJobContext"/>; in the target architecture
/// (see <c>docs/TargetArchitecture.md</c>) every job gets its own
/// instance, so a pause request belonging to one job cannot leak
/// into a sibling or successor run.
/// </summary>
/// <remarks>
/// Pause is intentionally a soft cooperative flag, not a
/// <see cref="CancellationToken"/>. Workers and coordinators read
/// <see cref="ControlledPaused"/> at safe checkpoints to drain in
/// flight rows before yielding, instead of being torn down mid
/// write. Subscribers to <see cref="PauseRequested"/> use the
/// event to unblock workers parked on a drain signal, since the
/// flag alone never trips their cancellation token.
/// </remarks>
public sealed class JobRunState
{
    private volatile bool _controlledPaused;

    /// <summary>
    /// Has a controlled pause been requested for this run?
    /// Returns to false after <see cref="Reset"/>.
    /// </summary>
    public bool ControlledPaused => _controlledPaused;

    /// <summary>
    /// Raised synchronously the moment a pause request lands.
    /// Subscribers (e.g. <see cref="DataTransfer.JobPipeline"/>,
    /// <see cref="DataTransfer.TableCopyCoordinator"/>) use this
    /// to release workers waiting on a drain signal — those
    /// workers never observe the flag because they are blocked
    /// on a different primitive.
    /// </summary>
    public event Action? PauseRequested;

    /// <summary>
    /// Request a controlled pause for this run. Idempotent: a
    /// second call still raises <see cref="PauseRequested"/>
    /// once (subscribers must be re-entrant).
    /// </summary>
    public void Request()
    {
        _controlledPaused = true;
        PauseRequested?.Invoke();
    }

    /// <summary>
    /// Clear the pause flag. Called by the job runner's finally
    /// block after the pause has been honoured and the job's
    /// final status (Paused) has been recorded.
    /// </summary>
    public void Reset()
    {
        _controlledPaused = false;
    }
}
