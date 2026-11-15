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
    /// Latched user intent. <see cref="JobCommand.None"/> if no
    /// pause/stop/cutover has been requested. See
    /// <see cref="CanTransition"/> for the latch rules.
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

    /// <summary>Requests a resumable pause. Returns false if another intent is already latched.</summary>
    public bool RequestPause() => Request(JobCommand.PauseRequested);

    /// <summary>Requests a terminal cancel. Returns false if a stop/cutover is already latched.</summary>
    public bool RequestStop() => Request(JobCommand.StopRequested);

    /// <summary>
    /// User-initiated cutover on an Online/CDC job. Honoured by the
    /// runner's finally as a terminal-Completed signal (vs.
    /// Cancel which writes terminal-Cancelled). Returns false if a
    /// stop/cutover is already latched.
    /// </summary>
    public bool RequestCutover() => Request(JobCommand.CutoverRequested);

    /// <summary>
    /// Intent latch rules: the first intent wins, except that a pending
    /// pause may be upgraded to a terminal stop/cutover. Terminal
    /// intents never overwrite each other, so a late or double click
    /// cannot relabel a cancel as a completed cutover (or vice versa).
    /// </summary>
    internal static bool CanTransition(JobCommand from, JobCommand to) =>
        from == JobCommand.None
        || (from == JobCommand.PauseRequested
            && to is JobCommand.StopRequested or JobCommand.CutoverRequested);

    private bool Request(JobCommand command)
    {
        bool accepted = false;
        int current = Volatile.Read(ref _requested);
        while (CanTransition((JobCommand)current, command))
        {
            int seen = Interlocked.CompareExchange(ref _requested, (int)command, current);
            if (seen == current)
            {
                accepted = true;
                break;
            }
            current = seen;
        }
        SafeCancel();
        return accepted;
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
/// Base type for tool-originated migration exceptions. Sentinel
/// hierarchy used by sites that detect a known condition where there
/// isn't a single driver exception to forward; carries an
/// operator-readable reason for surfacing in logs / final job status.
/// Subclasses (Fatal / Transient / Schema / Auth) let callers
/// classify by type rather than parsing message text.
/// </summary>
public abstract class MigrationException : Exception
{
    protected MigrationException(string message) : base(message) { }
    protected MigrationException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// Fatal condition (e.g. "write retries exhausted", "source read
/// retries exhausted", "N consecutive auth errors"). Recognised by
/// <see cref="Infrastructure.ExceptionClassifier.IsFatal"/> so the
/// classifier treats its own sentinel as fatal.
/// </summary>
public sealed class MigrationFatalException : MigrationException
{
    public MigrationFatalException(string message) : base(message) { }
    public MigrationFatalException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Transient condition the caller wants to flag for retry.</summary>
public sealed class MigrationTransientException : MigrationException
{
    public MigrationTransientException(string message) : base(message) { }
    public MigrationTransientException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Schema-mismatch or invalid-query condition surfaced by the tool.</summary>
public sealed class MigrationSchemaException : MigrationException
{
    public MigrationSchemaException(string message) : base(message) { }
    public MigrationSchemaException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Authentication / authorization condition surfaced by the tool.</summary>
public sealed class MigrationAuthException : MigrationException
{
    public MigrationAuthException(string message) : base(message) { }
    public MigrationAuthException(string message, Exception inner) : base(message, inner) { }
}
