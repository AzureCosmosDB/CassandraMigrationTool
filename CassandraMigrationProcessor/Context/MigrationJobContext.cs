using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using CassandraMigrationProcessor.Helpers.JobManagement;
using CassandraMigrationProcessor.Persistence;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using static CassandraMigrationProcessor.JobList;

namespace CassandraMigrationProcessor.Context
{
    public static class MigrationJobContext
    {
        private static readonly object _writeMULock = new object();
        private static readonly object _writeJobLock = new object();
        private static readonly object _writeJobListLock = new object();

        private static ConcurrentDictionary<string, MigrationJob>
            MigrationJobs { get; set; } = new();

        private static MigrationJob? _cachedCurrentlyActiveJob = null;
        private static Log _log;

        public static ActiveMigrationUnitsCache MigrationUnitsCache
        { get; set; }

        /// <summary>
        /// In-memory storage for source connection strings, keyed by job ID.
        /// </summary>
        public static ConcurrentDictionary<string, string> SourceConnectionString
        { get; set; } = new();

        /// <summary>
        /// In-memory storage for target connection strings, keyed by job ID.
        /// </summary>
        public static ConcurrentDictionary<string, string> TargetConnectionString
        { get; set; } = new();

        /// <summary>
        /// In-memory set of job IDs that should auto-start when
        /// the viewer page opens. Cleared after the job starts.
        /// Never persisted to disk.
        /// </summary>
        public static ConcurrentDictionary<string, byte> PendingAutoStartJobIds
        { get; set; } = new();

        /// <summary>
        /// Always false for Cassandra migration (no legacy driver concept).
        /// </summary>
        public static bool IsLegacyDriver => false;

        public static string ActiveMigrationJobId { get; set; }

        private static volatile bool _controlledPauseRequested;
        public static bool ControlledPauseRequested
            => _controlledPauseRequested;

        public static JobList JobList { get; private set; }

        public static void ResetControlledPause()
        {
            AddVerboseLog("Resetting controlled pause request.");
            _controlledPauseRequested = false;
        }

        public static void RequestControlledPause(string location)
        {
            if (_log == null)
                throw new Exception("Log not initialized.");

            _log.WriteLine(
                $"{location} caused controlled pause.", LogType.Warning);
            _controlledPauseRequested = true;
        }

        public static void UpdateLogLevel(
            LogType level, MigrationJob job)
        {
            if (CurrentlyActiveJob == null
                || CurrentlyActiveJob.IsCancelled
                || CurrentlyActiveJob.IsCompleted)
            {
                job.LogLevel = level;
                SaveMigrationJob(job);
            }
            else
            {
                CurrentlyActiveJob.LogLevel = level;
                SaveMigrationJob(CurrentlyActiveJob);
            }
        }

        public static void AddVerboseLog(string message)
        {
            if (_log == null
                || CurrentlyActiveJob == null
                || CurrentlyActiveJob.IsCancelled
                || CurrentlyActiveJob.IsCompleted)
                return;

            _log?.WriteLine(message, LogType.Verbose);
        }

        public static void ResetJobState()
        {
            _controlledPauseRequested = false;
            MigrationUnitsCache = new ActiveMigrationUnitsCache();
            _log = null;
        }

        public static void InitializeLog(Log log)
        {
            if (_log == null) { _log = log; }
            AddVerboseLog("Initialized MigrationJobContext log.");
        }

        public static MigrationJob? CurrentlyActiveJob
        {
            get
            {
                if (_cachedCurrentlyActiveJob != null
                    && !string.IsNullOrEmpty(ActiveMigrationJobId)
                    && _cachedCurrentlyActiveJob.Id
                        == ActiveMigrationJobId)
                {
                    return _cachedCurrentlyActiveJob;
                }

                if (!string.IsNullOrEmpty(ActiveMigrationJobId))
                {
                    _cachedCurrentlyActiveJob =
                        LoadMigrationJob(ActiveMigrationJobId);
                    if (MigrationUnitsCache == null)
                        MigrationUnitsCache =
                            new ActiveMigrationUnitsCache();
                    return _cachedCurrentlyActiveJob;
                }

                return null;
            }
        }

        public static PersistenceStorage? Store { get; private set; }
        public static string? AppId { get; set; }

        public static void Initialize(IConfiguration configuration)
        {
            Helper.LogToFile("MigrationJobContext.Initialize started");

            bool isLocal = true;
            var stateStoreCSorPath = string.Empty;
            var appId = string.Empty;
            try
            {
                bool.TryParse(
                    configuration["StateStore:UseLocalDisk"],
                    out isLocal);
                stateStoreCSorPath =
                    configuration["StateStore:ConnectionStringOrPath"];
                appId = configuration["StateStore:AppID"];
                AppId = appId;
            }
            catch { }

            Store = new DiskPersistence();
            var localPath =
                string.IsNullOrEmpty(stateStoreCSorPath)
                ? Helper.GetWorkingFolder()
                : stateStoreCSorPath;
            Store.Initialize(localPath, string.Empty);

            JobList = LoadJobList(
                out bool notFound, out string errorMessage);
            if (notFound && JobList == null)
            {
                JobList = new JobList();
                JobList.MigrationJobIds = new List<string>();
            }
            else if (JobList == null
                && !string.IsNullOrEmpty(errorMessage))
            {
                throw new InvalidOperationException(
                    $"Error initializing Job List: {errorMessage}");
            }
            JobList.Persist();
        }

        private static MigrationJob? LoadMigrationJob(string jobId)
        {
            if (MigrationJobs.TryGetValue(jobId, out var cached))
                return cached;

            try
            {
                var filePath = Path.Combine(
                    "migrationjobs", jobId, "jobdefinition.json");
                var json = Store.ReadDocument(filePath);
                var loadedObject =
                    JsonConvert.DeserializeObject<MigrationJob>(json);
                if (loadedObject == null)
                    return null;
                MigrationJobs[jobId] = loadedObject;
                return loadedObject;
            }
            catch { return null; }
        }

        public static MigrationJob? GetMigrationJob(string jobId)
        {
            if (jobId == ActiveMigrationJobId
                && CurrentlyActiveJob != null)
                return CurrentlyActiveJob;

            return LoadMigrationJob(jobId);
        }

        public static List<MigrationJob> PopulateMigrationJobs(
            List<string> ids)
        {
            List<MigrationJob> jobs = new();
            foreach (var id in ids)
            {
                var job = GetMigrationJob(id);
                if (job != null) jobs.Add(job);
            }
            return jobs;
        }

        public static bool SaveMigrationUnit(
            MigrationUnit mu, bool updateParent)
        {
            try
            {
                if (mu == null) return false;

                if (CurrentlyActiveJob != null)
                    mu.ParentJob = CurrentlyActiveJob;

                if (mu.ParentJob != null && updateParent)
                    mu.UpdateParentJob();

                lock (_writeMULock) { mu.Persist(); }

                if (CurrentlyActiveJob != null && updateParent)
                {
                    lock (_writeJobLock)
                    {
                        CurrentlyActiveJob.Persist();
                    }
                }

                if (MigrationUnitsCache != null)
                    MigrationUnitsCache.UpdateMigrationUnit(mu);

                return true;
            }
            catch { return false; }
        }

        private static JobList LoadJobList(
            out bool notFound, out string errorMessage)
        {
            errorMessage = string.Empty;
            notFound = false;
            string path = "migrationjobs\\joblist.json";

            try
            {
                for (int i = 0; i < 5; i++)
                {
                    try
                    {
                        if (!Store.DocumentExists(path))
                        {
                            notFound = true;
                            errorMessage = "Job list not found.";
                        }
                        else
                        {
                            string json = Store.ReadDocument(path);
                            var obj = JsonConvert
                                .DeserializeObject<JobList>(json);
                            if (obj?.MigrationJobIds != null)
                            {
                                JobList = obj;
                                return JobList;
                            }
                        }
                    }
                    catch (JsonException ex)
                    {
                        errorMessage =
                            $"Error deserializing: {ex}";
                    }
                    finally
                    {
                        Task.Delay(200).Wait();
                    }
                }
                errorMessage = "Error loading migration jobs.";
                return null;
            }
            catch (Exception ex)
            {
                errorMessage = $"Error: {ex}";
                return null;
            }
        }

        public static bool SaveMigrationJob(MigrationJob job)
        {
            try
            {
                if (job != null)
                {
                    lock (_writeJobLock)
                    {
                        job.Persist();
                        MigrationJobs[job.Id] = job;
                        if (!string.IsNullOrEmpty(ActiveMigrationJobId)
                            && job.Id == ActiveMigrationJobId)
                        {
                            _cachedCurrentlyActiveJob = job;
                        }
                    }
                }
                return true;
            }
            catch { return false; }
        }

        public static bool SaveJobList()
        {
            try
            {
                if (JobList != null)
                {
                    lock (_writeJobListLock)
                    {
                        JobList.Persist();
                    }
                }
                return true;
            }
            catch { return false; }
        }

        public static MigrationUnit GetMigrationUnit(
            string key, string jobId = null)
        {
            if (string.IsNullOrEmpty(jobId)
                && CurrentlyActiveJob != null)
            {
                jobId = CurrentlyActiveJob.Id;
            }

            if (MigrationUnitsCache == null)
                return GetMigrationUnitFromStorage(jobId, key);
            else
                return MigrationUnitsCache
                    .GetMigrationUnit(key, jobId);
        }

        public static MigrationUnit GetMigrationUnitFromStorage(
            string jobId, string unitId)
        {
            AddVerboseLog(
                $"GetMigrationUnit: jobId={jobId}, unitId={unitId}");
            try
            {
                var filePath = Path.Combine(
                    "migrationjobs", jobId, $"{unitId}.json");
                string json = Store.ReadDocument(filePath);
                return JsonConvert
                    .DeserializeObject<MigrationUnit>(json);
            }
            catch { return null; }
        }

        public static void ClearCurrentlyActiveJobCache()
        {
            _cachedCurrentlyActiveJob = null;
        }
    }
}
