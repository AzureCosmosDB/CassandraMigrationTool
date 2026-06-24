using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.DataTransfer;
using CassandraMigrationProcessor.Context;

namespace CassandraMigrationWebApp.Service;
public class JobManager
{
    private MigrationJobRunner? MigrationJobRunner { get; set; }
    private MigrationLog _log;
    private JobControl? _control;
    private string _runningJobId = string.Empty;
    private readonly object _stateLock = new();

    private DateTime _lastJobHeartBeat = DateTime.MinValue;
    private string _lastJobID = string.Empty;
    private readonly MigrationJobContext _context;

    public JobManager(MigrationJobContext context)
    {
        _context = context;
        _log = CreateLog();
    }

    private MigrationLog CreateLog()
    {
        var log = new MigrationLog();
        if (_context.LogStore != null)
            log.SetStorage(_context.CreateLogStorageCallbacks(_context.LogStore));
        return log;
    }

    #region Configuration Management

    public bool UpdateConfig(CassandraMigrationProcessor.Models.AppSettings updated_config, out string errorMessage)
    {
        if (updated_config == null)
        {
            errorMessage = "Migration settings cannot be null.";
            return false;
        }
        // Save the updated config
        return SettingsManager.Save(updated_config, out errorMessage);
    }

    public CassandraMigrationProcessor.Models.AppSettings GetConfig()
    {
        AppSettings config = new AppSettings();
        SettingsManager.Load(config);
        return config;
    }

    #endregion 
    #region Job Management

    public List<TableMigration> GetMigrationUnits(Job mj)
    {
        var units = new List<TableMigration>();
        if (mj != null)
        {
            foreach (var mub in mj.Tables)
            {
                var mu = _context.GetMigrationUnit(mub.Id, mj.Id);
                if (mu != null)
                    units.Add(mu);
            }
        }
        return units;
    }


    public Job? GetMigrationJobById(string id)
    {
        return _context.GetMigrationJob(id);
    }

    public List<string> GetMigrationIds()
    {
        return _context.JobIndex.MigrationJobIds;
    }

    public void ClearJobFiles(string jobId)
    {
        Task.Run(() =>
        {
            try
            {
                _context.Store.Delete($"{Path.Join(JobStore.JobsFolder, jobId)}");
                _context.LogStore.DeleteLogs(jobId);

                string dumpPath = Path.Join(DataDirectoryResolver.GetWorkingFolder(), "cassandradump", jobId);
                if (Directory.Exists(dumpPath))
                    Directory.Delete(dumpPath, true);

                _context.JobIndex.MigrationJobIds?.Remove(jobId);
                _context.SaveJobList();
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Failed to clear job files for job '{jobId}'. {ex}", LogType.Error);
            }
        });
    }

    #endregion 
    #region MigrationLog Management

    public List<LogObject> GetMonitorMessages(string id)
    {
        if (IsProcessRunning(id))
            return _log.GetMonitorMessages() ?? new List<LogObject>();
        return new List<LogObject>();
    }

    public bool DidMigrationJobExitRecently(string jobId)
    {
        if (jobId != _lastJobID) return false;

        if (DateTime.UtcNow.AddSeconds(-10) > _lastJobHeartBeat)
        {
            _lastJobID = string.Empty;
            return false; // heartbeat can be max 10 seconds old
        }

        return true;
    }

    public LogBucket GetLogBucket(string id, out string fileName, out bool isLiveLog)
    {
        //Check if migration worker is initialized and active. Return MigrationLog bucket if it is.
        LogBucket? bucket = null;
        if (IsProcessRunning(id))
        {
            bucket = _log.GetCurrentLogBucket(id);
            _lastJobHeartBeat = DateTime.UtcNow;
            _lastJobID = id;
            isLiveLog = true;
            fileName = string.Empty;
            return bucket ?? new LogBucket { Logs = new List<LogObject>() };
        }

        // If migration worker is not running, get from file
        isLiveLog = false;
        MigrationLog migrationLog = CreateLog();
        return migrationLog.ReadLogFile(id, out fileName) ?? new LogBucket { Logs = new List<LogObject>() };
    }

    public int GetLogCount(string jobId)
    {
        MigrationLog migrationLog = CreateLog();
        return migrationLog.GetLogCount(jobId);
    }

    #endregion

    #region Migration Job Runner Management

    /// <summary>
    /// Common preamble for user-initiated lifecycle intents. Captures
    /// <see cref="_runningJobId"/> under the state lock, returns false
    /// when no job is running, and emits the standard audit log line.
    /// </summary>
    private bool TryClaimRunningJob(string action, out string jobId)
    {
        jobId = _runningJobId;
        if (string.IsNullOrEmpty(jobId)) return false;
        _log.WriteLine($"User requested {action} for job {jobId}", LogType.Info);
        return true;
    }

    public void StopMigration() =>
        // _runningJobId is cleared by the runner's finally so the
        // UI keeps showing "Cancelling..." while the pipeline drains.
        TerminateRun("CANCEL", c => c.RequestStop());

    /// <summary>
    /// User-initiated cutover on an Online/CDC job. Records cutover
    /// intent so the run-finished finally block writes Completed
    /// (terminal). Pipeline drain is the same as Cancel.
    /// </summary>
    public void RequestCutover() =>
        TerminateRun("CUTOVER", c => c.RequestCutover());

    /// <summary>
    /// Shared terminate-run preamble for <see cref="StopMigration"/>
    /// and <see cref="RequestCutover"/>. Holds the state lock, claims
    /// the running job (no-op if none), signals the supplied intent on
    /// <see cref="JobControl"/>, then eagerly tears the pipeline down.
    /// </summary>
    private void TerminateRun(string action, Action<JobControl> requestOnControl)
    {
        lock (_stateLock)
        {
            if (!TryClaimRunningJob(action, out _)) return;
            if (_control != null) requestOnControl(_control);
            MigrationJobRunner?.Stop();
        }
    }

    /// <summary>
    /// User-initiated pause: cancels the JobControl token. The runner's
    /// finally reads <see cref="JobCommand.PauseRequested"/> and writes
    /// <see cref="JobStatus.Paused"/>.
    /// </summary>
    public void RequestControlledPause()
    {
        lock (_stateLock)
        {
            TryClaimRunningJob("PAUSE", out _);
            _control?.RequestPause();
        }
    }

    /// <summary>
    /// Returns true while the job is still in a phase where a controlled
    /// pause is meaningful — i.e. bulk copy hasn't fully drained yet. Once
    /// every table is past its copy phase the pause command is a no-op,
    /// so the UI hides the button.
    /// </summary>
    public bool IsControlledPauseApplicable(CassandraMigrationProcessor.Models.Job? job = null)
    {
        // If job is provided, check if bulk copy (offline phase) is still ongoing
        if (job != null)
        {
            // Check if all units have completed their copy phase
            if (job.Tables.All(mu => mu.CopyComplete))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Single source-of-truth for what a job looks like to the UI.
    /// Combines runtime intent (<see cref="JobControl.Requested"/>)
    /// with the persisted <see cref="JobStatus"/> so transient states
    /// (Pausing / Cancelling / CuttingOver) stay coherent.
    /// </summary>
    public LiveJobStatus GetLiveStatus(Job? job)
    {
        if (job == null) return LiveJobStatus.NotStarted;

        var jobId = !string.IsNullOrWhiteSpace(job.Id)
            ? job.Id
            : throw new InvalidOperationException("Job.Id must be non-null and non-empty when computing live status.");

        // Process-live state takes priority over persisted Status.
        if (IsProcessRunning(jobId))
        {
            return _control?.Requested switch
            {
                JobCommand.CutoverRequested => LiveJobStatus.CuttingOver,
                JobCommand.StopRequested    => LiveJobStatus.Cancelling,
                JobCommand.PauseRequested   => LiveJobStatus.Pausing,
                _                           => LiveJobStatus.Running,
            };
        }

        // Runner is idle — fall back to persisted Status.
        return job.Status switch
        {
            JobStatus.Completed => LiveJobStatus.Completed,
            JobStatus.Cancelled => LiveJobStatus.Cancelled,
            JobStatus.Faulted   => LiveJobStatus.Faulted,
            JobStatus.Paused    => LiveJobStatus.Paused,
            // Persisted Running with no live runner = process crashed
            // before the finally block could normalise. Surface as
            // Interrupted so the operator knows it's resumable.
            JobStatus.Running   => LiveJobStatus.Interrupted,
            JobStatus.Pending   => HasMadeProgress(job)
                                       ? LiveJobStatus.Interrupted
                                       : LiveJobStatus.NotStarted,
            _                   => LiveJobStatus.NotStarted,
        };
    }

    private static bool HasMadeProgress(Job job)
    {
        if (job.Tables == null) return false;
        return job.Tables.Any(mu => mu.CopyRowsCopied > 0);
    }

    public Task StartMigration(Job job, string sourceConnectionString, string targetConnectionString)
    {
        MigrationLog runLog = null!;
        JobControl runControl = null!;
        bool shouldWriteRejectionLog = false;
        string? runningJobIdForRejection = null;
        // Single source-of-truth for "user resumed from a non-terminal-but-
        // not-fresh state" — drives both the audit-log verb (RESUME vs
        // START) and the EndedOn reset below, so the two stay in lockstep.
        // Pending is only treated as resume when prior table copy progress
        // exists; otherwise it's a first-time start.
        bool isResume =
            job.Status is JobStatus.Paused or JobStatus.Faulted
            || (job.Status is JobStatus.Pending && HasMadeProgress(job));
        lock (_stateLock)
        {
            if (!string.IsNullOrEmpty(_runningJobId))
            {
                runningJobIdForRejection = _runningJobId;
                _log.WriteLine(
                    $"Job {runningJobIdForRejection} already running, cannot start {job.Id}",
                    LogType.Warning);
                shouldWriteRejectionLog = true;
            }
            else
            {
                _log = CreateLog();
                _log.Initialize(job.Id);
                _log.SetJob(job);
                _log.WriteLine(
                    $"User requested {(isResume ? "RESUME" : "START")} for job {job.Id} (prior status={job.Status})",
                    LogType.Info);
                // MigrationJobRunner is published below, once its sessions
                // are open (built via async factory). Claiming _runningJobId
                // here is enough to make concurrent StartMigration calls
                // see the slot as taken; Pause/Stop arriving during the
                // open window record intent on _control.
                _control = new JobControl();
                runControl = _control;
                runLog = _log;
                _runningJobId = job.Id;
            }
        }

        if (shouldWriteRejectionLog)
        {
            // Also write the rejection to the REJECTED
            // job's own log at Info level so an operator looking at
            // Job B's Job Logs panel sees a clear "Cannot start"
            // entry instead of staring at an empty log while the
            // banner says "Not Started". The pre-flight check in
            // OnMigrationDetailsPopUpSubmit catches the common path
            // before reaching here; this log is the defense-in-depth
            // for any caller that bypasses the UI guard or hits the
            // TOCTOU race between pre-flight and slot claim.
            try
            {
                using var rejectionLog = CreateLog();
                rejectionLog.Initialize(job.Id);
                rejectionLog.SetJob(job);
                rejectionLog.WriteLine(
                    $"Cannot start — job {runningJobIdForRejection} is currently running. " +
                    "Pause or complete that job first, then click Resume Job here.",
                    LogType.Info);
            }
            catch (Exception ex) when (ex is IOException or InvalidOperationException or ObjectDisposedException)
            {
                // Never let the rejection-log path mask the original
                // rejection; the primary log line above is already
                // recorded on the active job. Surface this failure on
                // the primary log so it stays diagnosable.
                _log?.WriteLine(
                    $"Failed to write rejection log for job {job.Id}: {ex.Message}",
                    LogType.Warning);
            }
            return Task.CompletedTask;
        }

        // Clear Running status on all other jobs so stale flags don't
        // cause unwanted auto-resume after an app recycle.
        var staleRunningJobs = new List<Job>();
        foreach (var otherId in GetMigrationIds().Where(otherId => otherId != job.Id))
        {
            var other = GetMigrationJobById(otherId);
            if (other is { Status: JobStatus.Running })
            {
                staleRunningJobs.Add(other);
            }
        }

        foreach (var staleJob in staleRunningJobs)
        {
            staleJob.Status = JobStatus.Pending;
            try
            {
                _context.SaveMigrationJob(staleJob);
            }
            catch (Exception ex)
            {
                _log.WriteLine(
                    $"Failed to clear stale Running status for job {staleJob.Id}: {ex.Message}",
                    LogType.Warning);
            }
        }

        _context.ActiveMigrationJobId = job.Id;
        // Clear EndedOn on resume from a terminal/terminal-ish state
        // so a Faulted → Resume → Completed path doesn't leave EndedOn
        // stamped at the original fault time.
        if (isResume)
            job.EndedOn = null;
        job.Status = JobStatus.Running;
        _context.Credentials.Remember(job.Id, sourceConnectionString, targetConnectionString);

        var config = new AppSettings();
        SettingsManager.Load(config);

        // Background migration: stored so exceptions are observable.
        // The runner is the sole writer of job.Status from this point
        // on (observes _control.Requested in finally to distinguish
        // Pause from Stop, writes Faulted on exception).
        _ = Task.Run(async () =>
        {
            MigrationJobRunner? runner = null;
            try
            {
                runner = await MigrationJobRunner.CreateAsync(runLog, job, config, runControl);
                lock (_stateLock)
                {
                    MigrationJobRunner = runner;
                    _context.ActiveRunner = runner;
                }
                await runner.StartAsync();
            }
            catch (Exception ex)
            {
                // Defensive: StartAsync handles its own failures. This
                // catch only exists to keep an unexpected escape (e.g.
                // failure during CreateAsync session acquisition) from
                // leaving the job stuck in Running.
                Console.WriteLine($"Migration unexpectedly threw for Job ID: {job.Id}: {ex}");
                runLog.WriteLine($"Migration unexpectedly threw: {ex}", LogType.Error);
                // Ensure escaped exceptions always produce persisted terminal metadata.
                // Preserve an already-terminal status (e.g. Cancelled), but fault any
                // non-terminal state so the job does not remain in-progress.
                if (!job.Status.IsTerminal())
                {
                    job.Status = JobStatus.Faulted;
                }
                // Stamp EndedOn here too: if CreateAsync throws before StartAsync runs,
                // the runner's StampEndedOnIfTerminal never fires.
                if (!job.EndedOn.HasValue)
                    job.EndedOn = DateTime.UtcNow;
                _context.SaveMigrationJob(job);
            }
            finally
            {
                // Retire per-job runtime state. Paused jobs keep their
                // connection-string entries so Resume-with-Existing
                // works without re-prompting; terminal jobs drop
                // everything.
                bool isTerminal = job.Status.IsTerminal();

                // Cleanup must always run even if runner.DisposeAsync
                // throws — otherwise dispose failure would leak
                // _runningJobId / ActiveMigrationJobId for the rest of
                // the process lifetime.
                try
                {
                    if (runner != null)
                    {
                        await runner.DisposeAsync();
                    }
                }
                catch (Exception disposeEx)
                {
                    Console.WriteLine($"[Manager] Runner DisposeAsync threw for {job.Id}: {disposeEx}");
                    try { runLog.WriteLine($"[Manager] Runner dispose threw (state cleared regardless): {disposeEx.Message}", LogType.Warning); }
                    catch { /* logging is best-effort during shutdown */ }
                }
                finally
                {
                    lock (_stateLock)
                    {
                        _context.ActiveRunner = null;
                        MigrationJobRunner = null;
                        _control?.Dispose();
                        _control = null;
                        _runningJobId = string.Empty;
                    }

                    try { _context.RetireJob(job.Id, isTerminal); }
                    catch (Exception retireEx)
                    {
                        Console.WriteLine($"[Manager] RetireJob threw for {job.Id}: {retireEx}");
                    }
                }
            }
        });

        Console.WriteLine($"Started migration for Job ID: {job.Id}");

        return Task.CompletedTask;
    }

    public string GetRunningJobId() => _runningJobId;

    public bool IsProcessRunning(string id)
    {
        return !string.IsNullOrEmpty(_runningJobId) && _runningJobId == id;
    }

    /// <summary>
    /// Append a "Resume dispatched by operator" entry to the persistent
    /// log file for <paramref name="jobId"/>. Call this **only** at the
    /// point a resume actually dispatches to <see cref="StartMigration"/>,
    /// i.e. after the precondition gate (resumable state, no other active
    /// job, credentials available) has passed. The entry is the
    /// operator-visible signal that the click drove a state change;
    /// rejected clicks are surfaced via the UI error banner instead so
    /// the log file stays a faithful record of "what the system did"
    /// rather than "where the user clicked".
    /// </summary>
    public void LogResumeDispatched(string jobId, string note = "")
    {
        if (string.IsNullOrEmpty(jobId)) return;
        try
        {
            string msg = $"[Manager] Resume dispatched by operator{(string.IsNullOrEmpty(note) ? "" : " - " + note)}";

            // Only write to the live in-flight log when this is the
            // same job running; otherwise scope a transient log to the
            // target jobId so the entry lands in the right file.
            if (IsProcessRunning(jobId))
            {
                _log.WriteLine(msg, LogType.Info);
                return;
            }

            using var transient = CreateLog();
            transient.Initialize(jobId);
            transient.WriteLine(msg, LogType.Info);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Manager] LogResumeDispatched failed for {jobId}: {ex.Message}");
        }
    }

    #endregion
}
