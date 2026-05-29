using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.DataTransfer;
using CassandraMigrationProcessor.Context;

namespace CassandraMigrationWebApp.Service;
public class JobManager
{
    private MigrationJobRunner? MigrationJobRunner { get; set; }
    private MigrationLog _log;
    private CancellationTokenSource? _migrationCts;
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

    public void StopMigration()
    {
        lock (_stateLock)
        {
            if (!string.IsNullOrEmpty(_runningJobId))
                _log.WriteLine($"User requested CANCEL for job {_runningJobId}", LogType.Info);
            try { _migrationCts?.Cancel(); }
            catch (ObjectDisposedException) { /* finally already retired the job */ }
            MigrationJobRunner?.Stop();
            _runningJobId = string.Empty;
        }
    }

    /// <summary>
    /// True between the user clicking PAUSE and the next migration
    /// start. Treated as a soft "user intent" flag — the actual work
    /// stop is driven by cancelling <see cref="_migrationCts"/>, which
    /// every running coordinator and worker already observes through
    /// the normal cancellation-token path. The flag's only job is to
    /// let the run-finished finally write back <see cref="JobStatus.Paused"/>
    /// instead of <see cref="JobStatus.Pending"/> / <see cref="JobStatus.Faulted"/>.
    /// </summary>
    private volatile bool _pauseRequested;

    public void RequestControlledPause()
    {
        if (!string.IsNullOrEmpty(_runningJobId))
            _log.WriteLine($"User requested PAUSE for job {_runningJobId}", LogType.Info);

        // Pause = "cancel the running work, remember it was a pause."
        // No need to plumb a separate signal through the pipeline /
        // coordinator / worker stack: those already react to the job
        // CTS being cancelled. The flag is read by the finally block
        // below to write back JobStatus.Paused.
        _pauseRequested = true;
        lock (_stateLock)
        {
            try { _migrationCts?.Cancel(); }
            catch (ObjectDisposedException) { /* finally already retired the job */ }
        }
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
    /// Gets whether controlled pause is currently requested
    /// </summary>
    public bool IsControlledPauseRequested()
    {
        return _pauseRequested;
    }

    public Task StartMigration(Job job, string sourceConnectionString, string targetConnectionString)
    {
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
            bool isResume = job.Status is JobStatus.Paused or JobStatus.Pending or JobStatus.Faulted;
            _log.WriteLine(
                $"User requested {(isResume ? "RESUME" : "START")} for job {job.Id} (prior status={job.Status})",
                LogType.Info);
            MigrationJobRunner = new MigrationJobRunner(_log);
            _context.ActiveRunner = MigrationJobRunner;
            _migrationCts = new CancellationTokenSource();
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
        job.Status = JobStatus.Running;

        var config = new AppSettings();
        SettingsManager.Load(config);

        // Background migration: stored so exceptions are observable and
        // the task can be awaited during shutdown if needed.
        _ = Task.Run(async () =>
        {
            // Capture any unhandled exception so the finally block can
            // attribute the job correctly (Faulted, not Pending).
            // Without this, an exception thrown from StartAsync was
            // logged but lost — finally saw Status == Running and (with
            // no per-table Failed markers, because the run never got
            // far enough to set any) wrote Pending, making a hard
            // failure look like a fresh job in the UI.
            Exception? unhandledException = null;
            try
            {
                await MigrationJobRunner.StartAsync(job, config, _migrationCts.Token);
            }
            catch (Exception ex)
            {
                unhandledException = ex;
                Console.WriteLine($"Migration failed for Job ID: {job.Id}: {ex}");
                _log.WriteLine($"Migration failed: {ex}", LogType.Error);
            }
            finally
            {
                // Determine final status. Real failures (unhandled
                // exception or any table marked Failed) outrank a
                // user-requested pause — if the operator clicked Pause
                // after the run had already faulted (race), we still
                // want the badge to read Faulted so the failure is
                // visible. Only after ruling out failures do we honour
                // the pause flag; only after ruling out both do we fall
                // through to the Running→Pending normalisation.
                bool tableFailed = job.Tables?.Any(
                    mu => mu.SourceStatus ==
                        TableStatus.Failed) ?? false;
                bool wasPauseRequested = _pauseRequested;
                _pauseRequested = false;

                if (unhandledException != null || tableFailed)
                {
                    job.Status = JobStatus.Faulted;
                }
                else if (wasPauseRequested)
                {
                    job.Status = JobStatus.Paused;
                }
                else if (job.Status == JobStatus.Running)
                {
                    job.Status = JobStatus.Pending;
                }

                _context.SaveMigrationJob(job);

                // Retire per-job runtime state from the process-wide
                // singletons. Without this every Start cycle leaks the
                // runner reference and the CTS, and the connection
                // strings / unit cache / loaded-job dictionary keep
                // entries for finished jobs until the process restarts.
                // Paused jobs intentionally keep their connection-string
                // entries so Resume-with-Existing continues to work
                // without re-prompting; terminal jobs (Completed /
                // Faulted / Cancelled) drop everything.
                bool isTerminal = job.Status == JobStatus.Completed
                               || job.Status == JobStatus.Faulted
                               || job.Status == JobStatus.Cancelled;

                lock (_stateLock)
                {
                    _context.ActiveRunner = null;
                    MigrationJobRunner = null;
                    try { _migrationCts?.Dispose(); }
                    catch (ObjectDisposedException) { /* concurrent Stop won the race */ }
                    _migrationCts = null;
                    _runningJobId = string.Empty;
                }

                _context.RetireJob(job.Id, isTerminal);
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

    #endregion
}
