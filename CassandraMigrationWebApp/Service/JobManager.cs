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
        private Log _log = new Log();
        private CancellationTokenSource? _migrationCts;
        private string _runningJobId = string.Empty;
        private readonly object _stateLock = new();

        private DateTime _lastJobHeartBeat = DateTime.MinValue;
        private string _lastJobID = string.Empty;
        private readonly IConfiguration _configuration;
        private string? _webAppBaseUrl = null;
        private readonly SemaphoreSlim _syncBackLock = new SemaphoreSlim(1, 1);

        public JobManager(IConfiguration configuration)
        {
            _configuration = configuration;

            MigrationJobContext.Initialize(_configuration);

            Helper.LogToFile("JobManager initialized");
        }



        
        #region _configuration Management

        /// <summary>
        /// Updates the WebAppBaseUrl from browser context. Called from Index.razor on first load.
        /// </summary>
        public void UpdateWebAppBaseUrlFromBrowser(string baseUri)
        {
            try
            {
                if (string.IsNullOrEmpty(baseUri))
                    return;

                // Remove trailing slash if present
                _webAppBaseUrl = baseUri.TrimEnd('/');
                
                Helper.LogToFile($"WebAppBaseUrl updated from browser: {_webAppBaseUrl}");
            }
            catch (Exception ex)
            {
                Helper.LogToFile($"Error updating WebAppBaseUrl from browser. Details: {ex}");
            }
        }

        public bool UpdateConfig(CassandraMigrationProcessor.MigrationSettings updated_config, out string errorMessage)
        {
            if (updated_config == null)
            {
                errorMessage = "Migration settings cannot be null.";
                return false;
            }
            // Save the updated config
            return updated_config.Save(out errorMessage);
        }

        public CassandraMigrationProcessor.MigrationSettings GetConfig()
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
            if (mj?.MigrationUnitBasics != null)
            {
                foreach (var mub in mj.MigrationUnitBasics)
                {
                    var mu = MigrationJobContext.GetMigrationUnit(mub.Id, mj.Id);
                    if (mu != null)
                        units.Add(mu);
                }
            }
            return units;
        }

        
        public MigrationJob? GetMigrationJobById(string id, bool active =true)
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
;
            try
            {
                Task.Run(() =>
                {
                    MigrationJobContext.Store.DeleteDocument($"{Path.Combine("migrationjobs", jobId)}");
                    MigrationJobContext.Store.DeleteLogs(jobId);
                    //clearing  dumped files

                    string dumpPath = Path.Combine(Helper.GetWorkingFolder(), "cassandradump", jobId);
                    if (Directory.Exists(dumpPath))
                        Directory.Delete(dumpPath, true);

                });
            }
            catch
            {
            }

            
        }

        #endregion 
        #region Log Management

        public List<LogObject> GetMonitorMessages(string id)
        {
            //verbose messages are only there for active jobs so fetch from log.
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
            //Check if migration worker is initialized and active. Return log bucket if it is.
            LogBucket? bucket = null;
            if (IsProcessRunning(id))
            {
                bucket = _log.GetCurentLogBucket(id);
                _lastJobHeartBeat = DateTime.UtcNow;
                _lastJobID = id;
                isLiveLog = true;
                fileName = string.Empty;
                return bucket ?? new LogBucket { Logs = new List<LogObject>() };
            }

            //If migration worker is not running, get the log bucket from the file.Its static  
            isLiveLog = false;
            Log log = new Log();
            return log.ReadLogFile(id, out fileName) ?? new LogBucket { Logs = new List<LogObject>() };
        }

        public int GetLogCount(string jobId)
        {
            Log log = new Log();
            return log.GetLogCount(jobId);
        }

        public byte[] DownloadLogPage(string jobId, int pageNumber, int pageSize)
        {
            if (string.IsNullOrWhiteSpace(jobId) || pageNumber < 1 || pageSize < 1)
                return Array.Empty<byte>();

            int skip = (pageNumber - 1) * pageSize;
            Log log = new Log();
            return log.DownloadLogsPaginated(jobId, skip, pageSize);
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
        public bool IsControlledPauseApplicable(JobType jobType, CassandraMigrationProcessor.MigrationJob? job = null)
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
                if (job.MigrationUnitBasics != null && job.MigrationUnitBasics.All(mu => mu.CopyComplete))
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

        public Task StartMigration(MigrationJob job, string sourceConnectionString, string targetConnectionString, string namespacesToMigrate, CassandraMigrationProcessor.Models.JobType jobType,bool trackChangeStreams)
        {
            _log = new Log();
            _log.Init(job.Id);
            _log.SetJob(job);
            MigrationWorker = new MigrationWorker(_log);
            _migrationCts = new CancellationTokenSource();
            _runningJobId = job.Id;
            
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

            Console.WriteLine(
                $"StartMigration: job.MaxFeedRangeParallelism={job.MaxFeedRangeParallelism}, " +
                $"job.ParallelThreads={job.ParallelThreads}, " +
                $"config.MaxFeedRangeParallelism={config.MaxFeedRangeParallelism}");

            // Fire-and-forget: UI should not block on long-running migration
            _ = Task.Run(async () =>
            {
                try
                {
                    Helper.LogToFile($"Task.Run started for job {job.Id}");

                    // Expand wildcards (e.g. "socialmedia.*") by connecting to source
                    if (job.MigrationUnitBasics == null || job.MigrationUnitBasics.Count == 0
                        || job.MigrationUnitBasics.Any(m => m.TableName == "*"))
                    {
                        Helper.LogToFile($"Expanding wildcards for job {job.Id}, namespaces={namespacesToMigrate}");
                        ExpandWildcardTables(job, namespacesToMigrate);
                        Helper.LogToFile($"After expand: {job.MigrationUnitBasics?.Count ?? 0} units");
                    }

                    Helper.LogToFile($"Calling MigrationWorker.StartAsync for job {job.Id}");
                    await MigrationWorker.StartAsync(job, config, _migrationCts.Token);
                    Helper.LogToFile($"MigrationWorker.StartAsync completed for job {job.Id}");
                }
                catch (Exception ex)
                {
                    Helper.LogToFile($"Migration failed for Job ID: {job.Id}: {ex}");
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
                        // Still running = no explicit completion/cancel
                        bool hasFailed = job.MigrationUnitBasics?.Any(
                            mu => mu.SourceStatus ==
                                CollectionStatus.Failed) ?? false;
                        if (hasFailed)
                            job.Status = JobStatus.Faulted;
                        else
                            job.Status = JobStatus.Pending;
                    }

                    MigrationJobContext.SaveMigrationJob(job);
                    _runningJobId = string.Empty;
                }
            });
            
            Helper.LogToFile($"Started migration (fire-and-forget) for Job ID: {job.Id}");
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

                string ks = fullName.Substring(0, dotIdx).Trim();
                string tbl = fullName.Substring(dotIdx + 1).Trim();

                if (tbl == "*")
                {
                    // Connect to source and list all tables in this keyspace
                    try
                    {
                        Console.WriteLine($"Discovering tables in keyspace: {ks}");
                        using (var session = CassandraMigrationProcessor.Helpers.Cassandra.CassandraClientFactory
                            .CreateSourceSession(_log, job, ks))
                        {
                            var tables = CassandraMigrationProcessor.Helpers.Cassandra.CassandraHelper
                                .ListTables(session, ks);
                            Console.WriteLine($"Found {tables.Count} tables in {ks}");
                            foreach (var tableName in tables)
                            {
                                // Validate table is accessible with retry for 429s
                                bool accessible = false;
                                for (int att = 1; att <= 10; att++)
                                {
                                    try
                                    {
                                        var probe = new Cassandra.SimpleStatement(
                                            $"SELECT * FROM \"{ks}\".\"{tableName}\"" +
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
                                        bool isThrottle = vex.Message?.Contains("429") == true
                                            || vex.Message?.Contains("rate", StringComparison.OrdinalIgnoreCase) == true
                                            || vex.Message?.Contains("TooMany", StringComparison.OrdinalIgnoreCase) == true;
                                        if (isThrottle && att < 10)
                                        {
                                            int delaySec = Math.Min(att * 3, 30);
                                            Console.WriteLine($"  Probe {ks}.{tableName} throttled (attempt {att}/10), retrying in {delaySec}s...");
                                            Thread.Sleep(delaySec * 1000);
                                            continue;
                                        }
                                        Console.WriteLine($"  Skipping {ks}.{tableName} — not accessible: {vex.GetType().Name}: {vex.Message}");
                                        _log.WriteLine($"Skipping {ks}.{tableName}: {vex.Message}", LogType.Warning);
                                    }
                                }
                                if (!accessible) continue;

                                var mu = new MigrationUnit(
                                    job, ks, tableName,
                                    new List<MigrationChunk>());
                                mu.SourceStatus = CollectionStatus.OK;
                                expandedUnits.Add(mu);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to discover tables in {ks}: {ex}");
                        _log.WriteLine($"Failed to discover tables in keyspace {ks}: {ex.Message}", LogType.Error);
                    }
                }
                else
                {
                    var mu = new MigrationUnit(
                        job, ks, tbl,
                        new List<MigrationChunk>());
                    mu.SourceStatus = CollectionStatus.OK;
                    expandedUnits.Add(mu);
                }
            }

            if (expandedUnits.Count > 0)
            {
                // Clear any wildcard entries
                job.MigrationUnitBasics?.RemoveAll(m => m.TableName == "*");
                Helper.AddMigrationUnits(expandedUnits, job, _log);
                Console.WriteLine($"Added {expandedUnits.Count} tables to job");
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

