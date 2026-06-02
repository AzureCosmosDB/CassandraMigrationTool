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
    CutoverRequested,
}

/// <summary>
/// Unified job lifecycle surface shared by <c>JobManager</c> (which
/// captures user intent) and <see cref="MigrationJobRunner"/> (which
/// observes intent + fault state and decides the final
/// <see cref="Models.JobStatus"/>). Owns a single
/// <see cref="CancellationTokenSource"/> — cancellation IS the latch
/// and the first captured exception IS the reason.
/// </summary>
public sealed class JobControl : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private int _requested;
    private Exception? _firstFault;

    public JobControl()
    {
        _cts = new CancellationTokenSource();
    }

    /// <summary>
    /// Cancellation token observed throughout the pipeline. Trips on
    /// any of <see cref="RequestPause"/>, <see cref="RequestStop"/>,
    /// <see cref="RequestCutover"/>, or <see cref="ReportFault"/>.
    /// </summary>
    public CancellationToken Token => _cts.Token;

    /// <summary>
    /// Latest user intent. <see cref="JobCommand.None"/> if no
    /// pause/stop/cutover has been requested.
    /// </summary>
    public JobCommand Requested
        => (JobCommand)Volatile.Read(ref _requested);

    /// <summary>
    /// First captured fatal exception (or null if no fault). Set
    /// exactly once via <see cref="ReportFault"/>; subsequent faults
    /// are ignored so the original root cause is preserved.
    /// </summary>
    public Exception? FirstFault => Volatile.Read(ref _firstFault);

    /// <summary>True iff a worker has reported a fatal fault.</summary>
    public bool IsFatal => FirstFault != null;

    /// <summary>True iff the token has been cancelled (any reason).</summary>
    public bool ShouldStop => _cts.IsCancellationRequested;

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

    /// <summary>
    /// User-initiated cutover on an Online/CDC job. Honoured by the
    /// runner's finally as a terminal-Completed signal (vs.
    /// Cancel which writes terminal-Cancelled).
    /// </summary>
    public void RequestCutover()
    {
        Volatile.Write(ref _requested, (int)JobCommand.CutoverRequested);
        SafeCancel();
    }

    /// <summary>
    /// Records the first fatal fault and cancels the token. Idempotent;
    /// subsequent calls are no-ops so the original root cause is kept.
    /// </summary>
    public void ReportFault(Exception ex)
    {
        if (ex is null) return;
        Interlocked.CompareExchange(ref _firstFault, ex, null);
        SafeCancel();
    }

    private void SafeCancel()
    {
        try { _cts.Cancel(); }
        catch (ObjectDisposedException) { /* finally already disposed control */ }
    }

    public void Dispose() => _cts.Dispose();
}

/// <summary>
/// Sentinel exception used by sites that detect a fatal condition
/// (e.g. "write retries exhausted", "source read retries exhausted")
/// where there isn't a single driver exception to forward. Carries
/// an operator-readable reason for surfacing in logs / final job
/// status.
/// </summary>
public sealed class MigrationFatalException : Exception
{
    public MigrationFatalException(string message) : base(message) { }
    public MigrationFatalException(string message, Exception inner) : base(message, inner) { }
}
