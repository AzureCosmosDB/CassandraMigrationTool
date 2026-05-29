namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// User-intent signals routed into a running migration. Distinct from
/// <see cref="Models.JobStatus"/>, which is the lifecycle state written
/// back after the run reacts.
/// </summary>
public enum JobCommand
{
    None,
    PauseRequested,
    StopRequested,
}

/// <summary>
/// Shared control surface between <c>JobManager</c> (the user-facing
/// shell that captures intent) and <see cref="MigrationJobRunner"/>
/// (which observes intent and decides the final <see cref="Models.JobStatus"/>).
/// <para>
/// Carries both the cancellation token observed by every awaitable in
/// the pipeline AND the user-intent flag the runner reads in its
/// finally block. Pause and Stop both cancel the token — the runner
/// tells them apart by reading <see cref="Requested"/>.
/// </para>
/// </summary>
public sealed class JobControl : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private int _requested;

    public JobControl()
    {
        _cts = new CancellationTokenSource();
    }

    /// <summary>
    /// Cancellation token observed throughout the pipeline. Trips on
    /// either <see cref="RequestPause"/> or <see cref="RequestStop"/>.
    /// </summary>
    public CancellationToken Token => _cts.Token;

    /// <summary>
    /// Latest user intent. <see cref="JobCommand.None"/> if no
    /// pause/stop has been requested yet (the run completed naturally
    /// or faulted on its own).
    /// </summary>
    public JobCommand Requested
        => (JobCommand)Volatile.Read(ref _requested);

    public void RequestPause()
    {
        Interlocked.CompareExchange(ref _requested,
            (int)JobCommand.PauseRequested, (int)JobCommand.None);
        SafeCancel();
    }

    public void RequestStop()
    {
        Volatile.Write(ref _requested, (int)JobCommand.StopRequested);
        SafeCancel();
    }

    private void SafeCancel()
    {
        try { _cts.Cancel(); }
        catch (ObjectDisposedException) { /* finally already disposed control */ }
    }

    public void Dispose() => _cts.Dispose();
}
