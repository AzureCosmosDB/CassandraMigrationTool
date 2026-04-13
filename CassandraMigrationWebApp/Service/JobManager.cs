using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CassandraMigrationProcessor;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.DataTransfer;
using CassandraMigrationProcessor.Context;

namespace CassandraMigrationWebApp.Service;
public class JobManager
{
    private MigrationWorker? MigrationWorker { get; set; }
    private MigrationLog _log;
    private CancellationTokenSource? _migrationCts;
    private string _runningJobId = string.Empty;
    private readonly object _stateLock = new();
    private Task? _migrationTask;

    private DateTime _lastJobHeartBeat = DateTime.MinValue;
    private string _lastJobID = string.Empty;
    private readonly IConfiguration _configuration;
    private readonly MigrationContextService _ctx;
    private string? _webAppBaseUrl = null;

    public JobManager(IConfiguration configuration, MigrationContextService ctx)
    {
        _configuration = configuration;
        _ctx = ctx;
        _log = CreateLog();

        MigrationJobContext.Initialize(_configuration);

        MigrationUtilities.LogToFile("JobManager initialized");
    }

    private MigrationLog CreateLog()
    {
        var log = new MigrationLog();
        if (_ctx.LogStore != null)
            log.SetStorage(MigrationJobContext.CreateLogStorageCallbacks(_ctx.LogStore));
        return log;
    }

    #region Configuration Management

    /// <summary>
    /// Updates the WebAppBaseUrl from browser context. Called from Index.razor on first load.
    /// </summary>
    public void UpdateWebAppBaseUrlFromBrowser(string baseUri)
    {
        if (string.IsNullOrEmpty(baseUri))
            return;

        _webAppBaseUrl = baseUri.TrimEnd('/');
        MigrationUtilities.LogToFile($"WebAppBaseUrl updated from browser: {_webAppBaseUrl}");
    }

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
                var mu = _ctx.GetUnit(mub.Id, mj.Id);
                if (mu != null)
                    units.Add(mu);
            }
        }
        return units;
    }


    public Job? GetMigrationJobById(string id, bool active = true)
    {
        return _ctx.GetJob(id);
    }

    public List<string> GetMigrationIds()
    {
        return _ctx.JobIndex.MigrationJobIds;
    }

    public void ClearJobFiles(string jobId)
    {
        _ctx.JobIndex.MigrationJobIds?.Remove(jobId);
        _ctx.SaveJobList();

        Task.Run(() =>
        {
            _ctx.Store.Delete($"{Path.Combine(JobStore.JobsFolder, jobId)}");
            _ctx.LogStore.DeleteLogs(jobId);

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

    #region Migration Worker Management

    public void StopMigration()
    {
        lock (_stateLock)
        {
            _migrationCts?.Cancel();
            MigrationWorker?.Stop();
            _runningJobId = string.Empty;
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
        return _ctx.ControlledPauseRequested;
    }

    public Task StartMigration(Job job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, CassandraMigrationProcessor.Models.JobType jobType, bool trackChangeStreams)
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
            _log.Init(job.Id);
            _log.SetJob(job);
            MigrationWorker = new MigrationWorker(_log);
            _migrationCts = new CancellationTokenSource();
            _runningJobId = job.Id;
        }

        _ctx.SourceConnectionString[job.Id] = sourceConnectionString;
        _ctx.TargetConnectionString[job.Id] = targetConnectionString;

        // Clear Running status on all other jobs so stale flags don't
        // cause unwanted auto-resume after an app recycle.
        foreach (var otherId in GetMigrationIds())
        {
            if (otherId == job.Id) continue;
            var other = GetMigrationJobById(otherId);
            if (other != null && other.Status == JobStatus.Running)
            {
                other.Status = JobStatus.Pending;
                _ctx.SaveJob(other);
            }
        }

        _ctx.ActiveMigrationJobId = job.Id;
        job.Status = JobStatus.Running;

        var config = new AppSettings();
        SettingsManager.Load(config);

        // Background migration: stored so exceptions are observable and
        // the task can be awaited during shutdown if needed.
        _migrationTask = Task.Run(async () =>
        {
            try
            {
                MigrationUtilities.LogToFile($"Task.Run started for job {job.Id}");

                // Expand wildcards (e.g. "socialmedia.*") by connecting to source
                if (job.Tables.Count == 0
                    || job.Tables.Any(m => m.TableName == "*"))
                {
                    MigrationUtilities.LogToFile($"Expanding wildcards for job {job.Id}, namespaces={namespacesToMigrate}");
                    await ExpandWildcardTablesAsync(job, namespacesToMigrate);
                    MigrationUtilities.LogToFile($"After expand: {job.Tables.Count} units");
                }

                MigrationUtilities.LogToFile($"Calling MigrationWorker.StartAsync for job {job.Id}");
                await MigrationWorker.StartAsync(job, config, _migrationCts.Token);
                MigrationUtilities.LogToFile($"MigrationWorker.StartAsync completed for job {job.Id}");
            }
            catch (Exception ex)
            {
                MigrationUtilities.LogToFile($"Migration failed for Job ID: {job.Id}: {ex}");
                Console.WriteLine($"Migration failed for Job ID: {job.Id}: {ex}");
                _log.WriteLine($"Migration failed: {ex}", LogType.Error);
            }
            finally
            {
                // Determine final status
                if (_ctx.ControlledPauseRequested)
                {
                    job.Status = JobStatus.Paused;
                    _ctx.ResetControlledPause();
                }
                else if (job.Status == JobStatus.Running)
                {
                    bool hasFailed = job.Tables?.Any(
                        mu => mu.SourceStatus ==
                            TableStatus.Failed) ?? false;
                    if (hasFailed)
                        job.Status = JobStatus.Faulted;
                    else
                        job.Status = JobStatus.Pending;
                }

                _ctx.SaveJob(job);
                _runningJobId = string.Empty;
            }
        });

        MigrationUtilities.LogToFile($"Started migration task for Job ID: {job.Id}");
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
            int dotIdx = fullName.IndexOf('.');
            if (dotIdx <= 0 || dotIdx == fullName.Length - 1) continue;

            string keyspace = fullName.Substring(0, dotIdx).Trim();
            string table = fullName.Substring(dotIdx + 1).Trim();

            if (table == "*")
            {
                // Connect to source and list all tables in this keyspace
                try
                {
                    using (var session = CassandraMigrationProcessor.CassandraDriver.CassandraClientFactory
                        .CreateSourceSession(_log, job, keyspace))
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
                                job, keyspace, tableName,
                                new List<CopyChunk>());
                            mu.SourceStatus = TableStatus.OK;
                            expandedUnits.Add(mu);
                        }
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
                    job, keyspace, table,
                    new List<CopyChunk>());
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

    #endregion
}
