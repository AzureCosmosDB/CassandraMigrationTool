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
        _context.JobIndex.MigrationJobIds?.Remove(jobId);
        _context.SaveJobList();

        Task.Run(() =>
        {
            _context.Store.Delete($"{Path.Combine(JobStore.JobsFolder, jobId)}");
            _context.LogStore.DeleteLogs(jobId);

            string dumpPath = Path.Combine(DataDirectoryResolver.GetWorkingFolder(), "cassandradump", jobId);
            if (Directory.Exists(dumpPath))
                Directory.Delete(dumpPath, true);
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

        if (System.DateTime.UtcNow.AddSeconds(-10) > _lastJobHeartBeat)
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
        MigrationLog MigrationLog = CreateLog();
        return MigrationLog.ReadLogFile(id, out fileName) ?? new LogBucket { Logs = new List<LogObject>() };
    }

    public int GetLogCount(string jobId)
    {
        MigrationLog MigrationLog = CreateLog();
        return MigrationLog.GetLogCount(jobId);
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
        jobId = _runningJobId ?? string.Empty;
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
        TryClaimRunningJob("PAUSE", out _);
        _control?.RequestPause();
    }

    /// <summary>
    /// Checks if controlled pause is applicable for the given job type and current job state.
    /// </summary>
    public bool IsControlledPauseApplicable(JobType jobType, CassandraMigrationProcessor.Models.Job? job = null)
    {
        if (jobType != JobType.CqlCopy)
            return false;

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

        // Process-live state takes priority over persisted Status.
        if (IsProcessRunning(job.Id ?? string.Empty))
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
        foreach (var mu in job.Tables)
            if (mu.CopyRowsCopied > 0) return true;
        return false;
    }

    public Task StartMigration(Job job, string sourceConnectionString, string targetConnectionString)
    {
        MigrationLog runLog;
        JobControl runControl;
        // Single source-of-truth for "user resumed from a non-terminal-but-
        // not-fresh state" — drives both the audit-log verb (RESUME vs
        // START) and the EndedOn reset below, so the two stay in lockstep.
        bool isResume = job.Status is JobStatus.Paused or JobStatus.Pending or JobStatus.Faulted;
        lock (_stateLock)
        {
            if (!string.IsNullOrEmpty(_runningJobId))
            {
                _log.WriteLine(
                    $"Job {_runningJobId} already running, cannot start {job.Id}",
                    LogType.Warning);
                return Task.CompletedTask;
            }

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

        _context.Credentials.Remember(job.Id, sourceConnectionString, targetConnectionString);

        // Clear Running status on all other jobs so stale flags don't
        // cause unwanted auto-resume after an app recycle.
        foreach (var otherId in GetMigrationIds())
        {
            if (otherId == job.Id) continue;
            var other = GetMigrationJobById(otherId);
            if (other is { Status: JobStatus.Running })
            {
                other.Status = JobStatus.Pending;
                _context.SaveMigrationJob(other);
            }
        }

        _context.ActiveMigrationJobId = job.Id;
        // Clear EndedOn on resume from a terminal/terminal-ish state
        // so a Faulted → Resume → Completed path doesn't leave EndedOn
        // stamped at the original fault time.
        if (isResume)
            job.EndedOn = null;
        job.Status = JobStatus.Running;

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
                if (job.Status == JobStatus.Running)
                {
                    job.Status = JobStatus.Faulted;
                    // Stamp EndedOn here too: if CreateAsync throws
                    // before StartAsync runs, the runner's
                    // StampEndedOnIfTerminal never fires.
                    if (!job.EndedOn.HasValue)
                        job.EndedOn = DateTime.UtcNow;
                    _context.SaveMigrationJob(job);
                }
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
    /// Append a "Resume requested by operator" entry to the persistent
    /// log file for <paramref name="jobId"/>. Used as immediate
    /// feedback before <see cref="StartMigration"/> is invoked.
    /// </summary>
    public void LogOperatorResumeRequest(string jobId, string note = "")
    {
        if (string.IsNullOrEmpty(jobId)) return;
        try
        {
            string msg = $"[Manager] Resume requested by operator{(string.IsNullOrEmpty(note) ? "" : " — " + note)}";

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
            Console.WriteLine($"[Manager] LogOperatorResumeRequest failed for {jobId}: {ex.Message}");
        }
    }

    #endregion
}
