using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using CassandraMigrationProcessor;
using CassandraMigrationProcessor.Helpers;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using CassandraMigrationProcessor.Context;

namespace CassandraMigrationWebApp.Service
{

    public class JobManager
    {
        private MigrationWorker? MigrationWorker { get; set; }
        private MigrationLog _log = new MigrationLog();
        private CancellationTokenSource? _migrationCts;
        private string _runningJobId = string.Empty;
        private readonly object _stateLock = new();
        private Task? _migrationTask;

        private DateTime _lastJobHeartBeat = DateTime.MinValue;
        private string _lastJobID = string.Empty;
        private readonly IConfiguration _configuration;
        private string? _webAppBaseUrl = null;
        private readonly SemaphoreSlim _syncBackLock = new SemaphoreSlim(1, 1);

        public JobManager(IConfiguration configuration)
        {
            _configuration = configuration;

            MigrationJobContext.Initialize(_configuration);

            MigrationHelper.LogToFile("JobManager initialized");
        }




        #region _configuration Management

        /// <summary>
        /// Updates the WebAppBaseUrl from browser context. Called from Index.razor on first load.
        /// </summary>
        public void UpdateWebAppBaseUrlFromBrowser(string baseUri)
        {
            if (string.IsNullOrEmpty(baseUri))
                return;

            _webAppBaseUrl = baseUri.TrimEnd('/');
            MigrationHelper.LogToFile($"WebAppBaseUrl updated from browser: {_webAppBaseUrl}");
        }

        public bool UpdateConfig(CassandraMigrationProcessor.Models.MigrationSettings updated_config, out string errorMessage)
        {
            if (updated_config == null)
            {
                errorMessage = "Migration settings cannot be null.";
                return false;
            }
            // Save the updated config
            return updated_config.Save(out errorMessage);
        }

        public CassandraMigrationProcessor.Models.MigrationSettings GetConfig()
        {
            MigrationSettings config = new MigrationSettings();
            config.Load();
            return config;
        }

        #endregion 
        #region Job Management

        public List<MigrationUnit> GetMigrationUnits(MigrationJob mj)
        {
            var units = new List<MigrationUnit>();
            if (mj?.Tables != null)
            {
                foreach (var mub in mj.Tables)
                {
                    var mu = MigrationJobContext.GetMigrationUnit(mub.Id, mj.Id);
                    if (mu != null)
                        units.Add(mu);
                }
            }
            return units;
        }


        public MigrationJob? GetMigrationJobById(string id, bool active = true)
        {
            var job = MigrationJobContext.GetMigrationJob(id);
            return job;
        }




        public List<string> GetMigrationIds()
        {

            return MigrationJobContext.JobList.MigrationJobIds;
        }

        public void ClearJobFiles(string jobId)
        {
            MigrationJobContext.JobList.MigrationJobIds?.Remove(jobId);
            MigrationJobContext.SaveJobList();

            Task.Run(() =>
            {
                MigrationJobContext.Store.Delete($"{Path.Combine(JobStore.JobsFolder, jobId)}");
                MigrationJobContext.Store.DeleteLogs(jobId);

                string dumpPath = Path.Combine(WorkingFolderResolver.GetWorkingFolder(), "cassandradump", jobId);
                if (Directory.Exists(dumpPath))
                    Directory.Delete(dumpPath, true);
            });
        }

        #endregion 
        #region MigrationLog Management

        public List<LogObject> GetMonitorMessages(string id)
        {
            //verbose messages are only there for active jobs so fetch from MigrationLog.
            if (IsProcessRunning(id))
                return _log.GetMonitorMessages() ?? new List<LogObject>();
            else
                return new List<LogObject>();
        }

        public bool DidMigrationJobExitRecently(string jobId)
        {
            if (jobId != _lastJobID) return false;

            if (System.DateTime.UtcNow.AddSeconds(-10) > _lastJobHeartBeat)
            {
                _lastJobID = string.Empty;
                return false; ///hear beat can be max 10 seconds old
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

            //If migration worker is not running, get the MigrationLog bucket from the file.Its static  
            isLiveLog = false;
            MigrationLog MigrationLog = new MigrationLog();
            return MigrationLog.ReadLogFile(id, out fileName) ?? new LogBucket { Logs = new List<LogObject>() };
        }

        public int GetLogCount(string jobId)
        {
            MigrationLog MigrationLog = new MigrationLog();
            return MigrationLog.GetLogCount(jobId);
        }

        public byte[] DownloadLogPage(string jobId, int pageNumber, int pageSize)
        {
            if (string.IsNullOrWhiteSpace(jobId) || pageNumber < 1 || pageSize < 1)
                return Array.Empty<byte>();

            int skip = (pageNumber - 1) * pageSize;
            MigrationLog MigrationLog = new MigrationLog();
            return MigrationLog.DownloadLogsPaginated(jobId, skip, pageSize);
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
        /// Checks if controlled pause is applicable for the given job type and current job state
        /// Controlled pause is only applicable during bulk copy phase, not during change stream processing
        /// </summary>
        public bool IsControlledPauseApplicable(JobType jobType, CassandraMigrationProcessor.Models.MigrationJob? job = null)
        {
            // Controlled pause is only applicable for CqlCopy jobs during bulk copy phase
            if (jobType != JobType.CqlCopy)
            {
                return false;
            }

            // If job is provided, check if bulk copy (offline phase) is still ongoing
            if (job != null)
            {
                // Check if all units have completed their copy phase
                if (job.Tables != null && job.Tables.All(mu => mu.CopyComplete))
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
            return MigrationJobContext.ControlledPauseRequested;
        }

        public Task CancelMigration(string id)
        {

            var migration = MigrationJobContext.GetMigrationJob(id);
            if (migration != null)
            {
                migration.Status = JobStatus.Cancelled;
                MigrationJobContext.SaveMigrationJob(migration);
            }
            // Also stop the running pipeline so it doesn't
            // finish and mark the job as completed
            StopMigration();
            return Task.CompletedTask;
        }

        public Task StartMigration(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, CassandraMigrationProcessor.Models.JobType jobType, bool trackChangeStreams)
        {
            lock (_stateLock)
            {
                if (!string.IsNullOrEmpty(_runningJobId))
                {
                    _log.WriteLine(
                        $"Job {_runningJobId} already running," +
                        $" cannot start {job.Id}",
                        LogType.Warning);
                    return Task.CompletedTask;
                }

                _log = new MigrationLog();
                _log.Init(job.Id);
                _log.SetJob(job);
                MigrationWorker = new MigrationWorker(_log);
                _migrationCts = new CancellationTokenSource();
                _runningJobId = job.Id;
            }

            MigrationJobContext.SourceConnectionString[job.Id] = sourceConnectionString;
            MigrationJobContext.TargetConnectionString[job.Id] = targetConnectionString;

            // Clear IsStarted on all other jobs so stale flags don't
            // cause unwanted auto-resume after an app recycle.
            foreach (var otherId in GetMigrationIds())
            {
                if (otherId == job.Id) continue;
                var other = GetMigrationJobById(otherId);
                if (other != null && other.Status == JobStatus.Running)
                {
                    other.Status = JobStatus.Pending;
                    MigrationJobContext.SaveMigrationJob(other);
                }
            }

            MigrationJobContext.ActiveMigrationJobId = job.Id;
            job.Status = JobStatus.Running;

            var config = new MigrationSettings();
            config.Load();

            // Background migration: stored so exceptions are observable and
            // the task can be awaited during shutdown if needed.
            _migrationTask = Task.Run(async () =>
            {
                try
                {
                    MigrationHelper.LogToFile($"Task.Run started for job {job.Id}");

                    // Expand wildcards (e.g. "socialmedia.*") by connecting to source
                    if (job.Tables == null || job.Tables.Count == 0
                        || job.Tables.Any(m => m.TableName == "*"))
                    {
                        MigrationHelper.LogToFile($"Expanding wildcards for job {job.Id}, namespaces={namespacesToMigrate}");
                        ExpandWildcardTables(job, namespacesToMigrate);
                        MigrationHelper.LogToFile($"After expand: {job.Tables?.Count ?? 0} units");
                    }

                    MigrationHelper.LogToFile($"Calling MigrationWorker.StartAsync for job {job.Id}");
                    await MigrationWorker.StartAsync(job, config, _migrationCts.Token);
                    MigrationHelper.LogToFile($"MigrationWorker.StartAsync completed for job {job.Id}");
                }
                catch (Exception ex)
                {
                    MigrationHelper.LogToFile($"Migration failed for Job ID: {job.Id}: {ex}");
                    Console.WriteLine($"Migration failed for Job ID: {job.Id}: {ex}");
                    _log.WriteLine($"Migration failed: {ex}", LogType.Error);
                }
                finally
                {
                    // Determine final status
                    if (MigrationJobContext.ControlledPauseRequested)
                    {
                        job.Status = JobStatus.Paused;
                        MigrationJobContext.ResetControlledPause();
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

                    MigrationJobContext.SaveMigrationJob(job);
                    _runningJobId = string.Empty;
                }
            });

            MigrationHelper.LogToFile($"Started migration task for Job ID: {job.Id}");
            Console.WriteLine($"Started migration for Job ID: {job.Id}");

            return Task.CompletedTask;
        }

        private void ExpandWildcardTables(MigrationJob job, string namespacesToMigrate)
        {
            if (string.IsNullOrWhiteSpace(namespacesToMigrate)) return;

            var entries = namespacesToMigrate
                .Split(new[] { ',', '\n', '\r', ';' })
                .Select(s => s.Trim())
                .Where(s => !string.IsNullOrEmpty(s));

            List<MigrationUnit> expandedUnits = new List<MigrationUnit>();

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
                        using (var session = CassandraMigrationProcessor.Helpers.Cassandra.CassandraClientFactory
                            .CreateSourceSession(_log, job, keyspace))
                        {
                            var tables = CassandraMigrationProcessor.Helpers.Cassandra.CassandraHelper
                                .ListTables(session, keyspace);
                            foreach (var tableName in tables)
                            {
                                // Validate table is accessible with retry for 429s
                                bool accessible = false;
                                for (int att = 1; att <= 10; att++)
                                {
                                    try
                                    {
                                        var probe = new Cassandra.SimpleStatement(
                                            $"SELECT * FROM \"{keyspace}\".\"{tableName}\"" +
                                            " WHERE COSMOS_CHANGEFEED_FROM_START() = true");
                                        probe.SetPageSize(1);
                                        probe.SetAutoPage(false);
                                        probe.SetReadTimeoutMillis(15_000);
                                        session.Execute(probe);
                                        accessible = true;
                                        break;
                                    }
                                    catch (Exception vex)
                                    {
                                        if (CassandraMigrationProcessor.Helpers.ExceptionClassifier.IsThrottle(vex) && att < 10)
                                        {
                                            int delaySec = Math.Min(att * 3, 30);
                                            Thread.Sleep(delaySec * 1000);
                                            continue;
                                        }
                                        _log.WriteLine($"Skipping {keyspace}.{tableName}: {vex.Message}", LogType.Warning);
                                    }
                                }
                                if (!accessible) continue;

                                var mu = new MigrationUnit(
                                    job, keyspace, tableName,
                                    new List<MigrationChunk>());
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
                    var mu = new MigrationUnit(
                        job, keyspace, table,
                        new List<MigrationChunk>());
                    mu.SourceStatus = TableStatus.OK;
                    expandedUnits.Add(mu);
                }
            }

            if (expandedUnits.Count > 0)
            {
                // Clear any wildcard entries
                job.Tables?.RemoveAll(m => m.TableName == "*");
                MigrationHelper.AddMigrationUnits(expandedUnits, job, _log);
            }
        }

        public string GetRunningJobId()
        {
            return _runningJobId;
        }


        public bool IsProcessRunning(string id)
        {
            return !string.IsNullOrEmpty(_runningJobId) && _runningJobId == id;
        }

        #endregion

    }
}

