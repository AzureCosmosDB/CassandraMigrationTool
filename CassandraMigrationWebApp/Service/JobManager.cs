using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.DataTransfer;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.CassandraDriver;

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
        // Back-compat alias. New code paths should call RequestCancel()
        // directly so the recorded user intent is unambiguous.
        RequestCancel();
    }

    /// <summary>
    /// User-initiated permanent stop. Records cancel intent so the
    /// run-finished finally block writes <see cref="JobStatus.Cancelled"/>.
    /// </summary>
    public void RequestCancel()
    {
        lock (_stateLock)
        {
            if (string.IsNullOrEmpty(_runningJobId))
                return;
            _log.WriteLine($"User requested CANCEL for job {_runningJobId}", LogType.Info);
            _cancelRequested = true;
            try { _migrationCts?.Cancel(); }
            catch (ObjectDisposedException) { /* finally already retired the job */ }
            MigrationJobRunner?.Stop();
        }
    }

    /// <summary>
    /// User-initiated cutover on an Online/CDC job. Records cutover
    /// intent so the run-finished finally block writes
    /// <see cref="JobStatus.Completed"/> (terminal). Today the
    /// pipeline drain is the same as Cancel — there is no separate
    /// CDC-handshake yet — but the intent flag ensures the persisted
    /// status and the UI label both reflect "cutover" rather than
    /// "cancel".
    /// </summary>
    public void RequestCutover()
    {
        lock (_stateLock)
        {
            if (string.IsNullOrEmpty(_runningJobId))
                return;
            _log.WriteLine($"User requested CUTOVER for job {_runningJobId}", LogType.Info);
            _cutoverRequested = true;
            try { _migrationCts?.Cancel(); }
            catch (ObjectDisposedException) { /* finally already retired the job */ }
            MigrationJobRunner?.Stop();
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

    /// <summary>
    /// True between the user clicking CANCEL and the runner exiting.
    /// Honoured by the run-finished finally to write
    /// <see cref="JobStatus.Cancelled"/>.
    /// </summary>
    private volatile bool _cancelRequested;

    /// <summary>
    /// True between the user clicking CUTOVER and the runner exiting.
    /// Honoured by the run-finished finally to write
    /// <see cref="JobStatus.Completed"/>.
    /// </summary>
    private volatile bool _cutoverRequested;

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

    /// <summary>
    /// Single source-of-truth for what a job looks like to the UI.
    /// Combines runtime intent flags (pause / cancel / cutover) with
    /// the persisted <see cref="JobStatus"/> so every page derives
    /// the same label and badge from one place. Callers MUST not
    /// look at <see cref="Job.Status"/> or
    /// <see cref="IsProcessRunning(string)"/> directly when rendering
    /// status; use this instead.
    /// </summary>
    public LiveJobStatus GetLiveStatus(Job? job)
    {
        if (job == null) return LiveJobStatus.NotStarted;

        // Process-live state takes priority over persisted Status —
        // the runner is the source of truth while it's draining, and
        // the intent flags reflect what the user just asked for.
        if (IsProcessRunning(job.Id ?? string.Empty))
        {
            if (_cutoverRequested) return LiveJobStatus.CuttingOver;
            if (_cancelRequested)  return LiveJobStatus.Cancelling;
            if (_pauseRequested)   return LiveJobStatus.Pausing;
            return LiveJobStatus.Running;
        }

        // Runner is idle — fall back to persisted Status.
        switch (job.Status)
        {
            case JobStatus.Completed: return LiveJobStatus.Completed;
            case JobStatus.Cancelled: return LiveJobStatus.Cancelled;
            case JobStatus.Faulted:   return LiveJobStatus.Faulted;
            case JobStatus.Paused:    return LiveJobStatus.Paused;
            case JobStatus.Running:
                // Persisted Running with no live runner = process
                // crashed or host recycled mid-run before the finally
                // block could normalise. Surface as Interrupted so the
                // operator knows it's resumable, not "Stopped".
                return LiveJobStatus.Interrupted;
            case JobStatus.Pending:
                return HasMadeProgress(job)
                    ? LiveJobStatus.Interrupted
                    : LiveJobStatus.NotStarted;
            default:
                return LiveJobStatus.NotStarted;
        }
    }

    private static bool HasMadeProgress(Job job)
    {
        if (job.Tables == null) return false;
        foreach (var mu in job.Tables)
            if (mu.CopyRowsCopied > 0) return true;
        return false;
    }

    public Task StartMigration(Job job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate)
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

            // Reset all user-intent flags for the new run. Stale flags
            // from a prior Pause/Cancel/Cutover cycle would otherwise
            // be honoured by this run's finally block.
            _pauseRequested = false;
            _cancelRequested = false;
            _cutoverRequested = false;

            _log = CreateLog();
            _log.Initialize(job.Id);
            _log.SetJob(job);
            bool isResume = job.Status == JobStatus.Paused
                || job.Status == JobStatus.Pending
                || job.Status == JobStatus.Faulted;
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
            if (other != null && other.Status == JobStatus.Running)
            {
                other.Status = JobStatus.Pending;
                _context.SaveMigrationJob(other);
            }
        }

        _context.ActiveMigrationJobId = job.Id;
        job.Status = JobStatus.Running;
        if (!job.StartedOn.HasValue)
            job.StartedOn = DateTime.UtcNow;
        // Persist Running before launching the runner so list views
        // and other consumers see the new status immediately, not
        // only after the run completes (when finally writes the final
        // status). This is the only synchronous Status write outside
        // the finally block — required to keep disk == in-memory.
        _context.SaveMigrationJob(job);

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
                // Expand wildcards (e.g. "socialmedia.*") by connecting to source
                if (job.Tables.Count == 0
                    || job.Tables.Any(m => m.TableName == "*"))
                {
                    await ExpandWildcardTablesAsync(job, namespacesToMigrate);
                }

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
                // Single-source-of-truth for the final persisted Status.
                // Precedence (most-significant wins):
                //   1. Real failure (unhandled exception or per-table fault)
                //      → Faulted, regardless of any user intent. If the
                //        operator clicked Pause AFTER the run had already
                //        faulted (race), we still want the badge to read
                //        Faulted so the failure isn't hidden.
                //   2. Cutover intent → Completed (terminal).
                //   3. Cancel intent  → Cancelled (terminal).
                //   4. Pause intent   → Paused.
                //   5. Runner cleanly set Completed (offline finalize) →
                //      keep it.
                //   6. Otherwise Status was still Running → Pending
                //      (interrupted; operator can resume).
                bool tableFailed = job.Tables?.Any(
                    mu => mu.SourceStatus ==
                        TableStatus.Failed) ?? false;
                bool wasPauseRequested = _pauseRequested;
                bool wasCancelRequested = _cancelRequested;
                bool wasCutoverRequested = _cutoverRequested;
                _pauseRequested = false;
                _cancelRequested = false;
                _cutoverRequested = false;

                if (unhandledException != null || tableFailed)
                {
                    job.Status = JobStatus.Faulted;
                }
                else if (wasCutoverRequested)
                {
                    job.Status = JobStatus.Completed;
                }
                else if (wasCancelRequested)
                {
                    job.Status = JobStatus.Cancelled;
                }
                else if (wasPauseRequested)
                {
                    job.Status = JobStatus.Paused;
                }
                else if (job.Status == JobStatus.Running)
                {
                    job.Status = JobStatus.Pending;
                }
                // else: runner set Completed via offline finalize — preserve.

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

    private async Task ExpandWildcardTablesAsync(Job job, string namespacesToMigrate)
    {
        if (string.IsNullOrWhiteSpace(namespacesToMigrate)) return;

        var entries = namespacesToMigrate
            .Split(new[] { ',', '\n', '\r', ';' })
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrEmpty(s));

        List<TableMigration> expandedUnits = new List<TableMigration>();

        foreach (var fullName in entries)
        {
            // CQL-aware split that tolerates quoted identifiers
            // (e.g. '"My-KS".*' or 'foo."Mixed_Table-1"'). We do the
            // split manually instead of going through
            // CqlIdentifier.SplitQualifiedName because that helper
            // rejects the wildcard '*' on the table side; here we
            // need to allow it.
            string keyspace, table;
            try
            {
                (keyspace, table) = SplitKeyspaceAllowingWildcard(fullName);
            }
            catch (ArgumentException)
            {
                continue;
            }
            if (string.IsNullOrEmpty(keyspace) || string.IsNullOrEmpty(table))
                continue;

            if (table == "*")
            {
                // Connect to source and list all tables in this keyspace
                try
                {
                    var session = CassandraMigrationProcessor.CassandraDriver.CassandraClientFactory
                        .CreateSourceSession(_log, job);
                    try
                    {
                        var tables = await CassandraMigrationProcessor.CassandraDriver.CassandraQueries
                            .ListTablesAsync(session, keyspace);
                        foreach (var tableName in tables)
                        {
                            // Validate table is accessible with retry for 429s
                            bool accessible = false;
                            for (int att = 1; att <= 10; att++)
                            {
                                try
                                {
                                    var probe = new Cassandra.SimpleStatement(
                                        $"SELECT * FROM \"{keyspace}\".\"{tableName}\" WHERE COSMOS_CHANGEFEED_FROM_START() = true");
                                    probe.SetPageSize(1);
                                    probe.SetAutoPage(false);
                                    probe.SetReadTimeoutMillis(15_000);
                                    session.Execute(probe);
                                    accessible = true;
                                    break;
                                }
                                catch (Exception vex)
                                {
                                    if (CassandraMigrationProcessor.Infrastructure.ExceptionClassifier.IsThrottle(vex) && att < 10)
                                    {
                                        int delaySec = Math.Min(att * 3, 30);
                                        Thread.Sleep(delaySec * 1000);
                                        continue;
                                    }
                                    _log.WriteLine($"Skipping {keyspace}.{tableName}: {vex.Message}", LogType.Warning);
                                }
                            }
                            if (!accessible) continue;

                            var mu = new TableMigration(
                                job, keyspace, tableName);
                            mu.SourceStatus = TableStatus.OK;
                            expandedUnits.Add(mu);
                        }
                    }
                    finally
                    {
                        CassandraMigrationProcessor.Infrastructure.MigrationUtilities
                            .SafeDisposeSession(session, $"JobManager table discovery session ({keyspace})");
                    }
                }
                catch (Exception ex)
                {
                    _log.WriteLine($"Failed to discover tables in keyspace {keyspace}: {ex.Message}", LogType.Error);
                }
            }
            else
            {
                var mu = new TableMigration(
                    job, keyspace, table);
                mu.SourceStatus = TableStatus.OK;
                expandedUnits.Add(mu);
            }
        }

        if (expandedUnits.Count > 0)
        {
            // Clear any wildcard entries
            job.Tables?.RemoveAll(m => m.TableName == "*");
            UnitStore.AddMigrationUnits(expandedUnits, job, _log);
        }
    }

    public string GetRunningJobId() => _runningJobId;

    public bool IsProcessRunning(string id)
    {
        return !string.IsNullOrEmpty(_runningJobId) && _runningJobId == id;
    }

    /// <summary>
    /// Quote-aware split of 'keyspace.table' where the table side may be
    /// the literal wildcard '*'. Both sides are returned in bare form
    /// (surrounding "..." stripped, ""-escapes resolved).
    /// </summary>
    private static (string keyspace, string table) SplitKeyspaceAllowingWildcard(string fullName)
    {
        if (string.IsNullOrWhiteSpace(fullName))
            throw new ArgumentException("Empty entry", nameof(fullName));

        int pos = 0;
        string keyspace = CqlIdentifier.ReadIdentifier(fullName, ref pos);
        if (pos >= fullName.Length || fullName[pos] != '.')
            throw new ArgumentException(
                $"Expected '.' between keyspace and table in '{fullName}'", nameof(fullName));
        pos++; // skip '.'

        string table;
        if (pos < fullName.Length && fullName[pos] == '*'
            && (pos + 1 == fullName.Length))
        {
            table = "*";
        }
        else
        {
            table = CqlIdentifier.ReadIdentifier(fullName, ref pos);
            if (pos != fullName.Length)
                throw new ArgumentException(
                    $"Unexpected trailing characters in '{fullName}'", nameof(fullName));
        }

        return (keyspace, table);
    }

    #endregion
}
