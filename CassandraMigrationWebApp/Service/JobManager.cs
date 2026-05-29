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

    public void StopMigration()
    {
        lock (_stateLock)
        {
            if (!string.IsNullOrEmpty(_runningJobId))
                _log.WriteLine($"User requested CANCEL for job {_runningJobId}", LogType.Info);
            _control?.RequestStop();
            MigrationJobRunner?.Stop();
            _runningJobId = string.Empty;
        }
    }

    /// <summary>
    /// Forceful pause: record pause intent <em>and</em> tear down the
    /// pipeline immediately. The runner's finally will read
    /// <see cref="JobCommand.PauseRequested"/> and write
    /// <see cref="JobStatus.Paused"/>. Use this when the user wants
    /// the work to stop now without waiting for the controlled-pause
    /// hand-off to settle.
    /// </summary>
    public void PauseImmediate()
    {
        lock (_stateLock)
        {
            if (!string.IsNullOrEmpty(_runningJobId))
                _log.WriteLine($"User requested IMMEDIATE PAUSE for job {_runningJobId}", LogType.Info);
            _control?.RequestPause();
            MigrationJobRunner?.Stop();
            _runningJobId = string.Empty;
        }
    }

    public void RequestControlledPause()
    {
        if (!string.IsNullOrEmpty(_runningJobId))
            _log.WriteLine($"User requested PAUSE for job {_runningJobId}", LogType.Info);
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
    /// Gets whether controlled pause is currently requested
    /// </summary>
    public bool IsControlledPauseRequested()
    {
        return _control?.Requested == JobCommand.PauseRequested;
    }

    public Task StartMigration(Job job, string sourceConnectionString, string targetConnectionString)
    {
        MigrationLog runLog;
        JobControl runControl;
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
            // MigrationJobRunner is published below, once its sessions
            // are open: it is built via an async factory that we cannot
            // await under this sync lock. Claiming _runningJobId here
            // is enough to make concurrent StartMigration calls see the
            // slot as taken; Pause/Stop arriving during the open window
            // record their intent on _control, which the runner will
            // observe as soon as StartAsync begins.
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
        job.Status = JobStatus.Running;

        var config = new AppSettings();
        SettingsManager.Load(config);

        // Background migration: stored so exceptions are observable and
        // the task can be awaited during shutdown if needed. The runner
        // is the sole writer of job.Status from this point on (it
        // observes _control.Requested in its finally to distinguish
        // Pause from Stop, and writes Faulted on exception).
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
                // Defensive: StartAsync handles its own failures and
                // writes job.Status in its finally. This catch only
                // exists to keep an unexpected escape (including any
                // failure during CreateAsync session acquisition) from
                // leaving the job stuck in Running.
                Console.WriteLine($"Migration unexpectedly threw for Job ID: {job.Id}: {ex}");
                runLog.WriteLine($"Migration unexpectedly threw: {ex}", LogType.Error);
                if (job.Status == JobStatus.Running)
                {
                    job.Status = JobStatus.Faulted;
                    _context.SaveMigrationJob(job);
                }
            }
            finally
            {
                // Retire per-job runtime state from the process-wide
                // singletons. Paused jobs intentionally keep their
                // connection-string entries so Resume-with-Existing
                // continues to work without re-prompting; terminal
                // jobs (Completed / Faulted / Cancelled) drop
                // everything.
                bool isTerminal = job.Status == JobStatus.Completed
                               || job.Status == JobStatus.Faulted
                               || job.Status == JobStatus.Cancelled;

                if (runner != null)
                {
                    await runner.DisposeAsync();
                }

                lock (_stateLock)
                {
                    _context.ActiveRunner = null;
                    MigrationJobRunner = null;
                    _control?.Dispose();
                    _control = null;
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
