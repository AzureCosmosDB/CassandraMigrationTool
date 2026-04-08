using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using CassandraMigrationProcessor.Helpers.JobManagement;
using CassandraMigrationProcessor.Persistence;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using static CassandraMigrationProcessor.JobList;

namespace CassandraMigrationProcessor.Context
{
    // TODO: Convert to injectable singleton service.
    // Currently static for backward compatibility with
    // the processor library which lacks DI support.
    public static class MigrationJobContext
    {
        private static readonly object _writeJobListLock = new object();
        private static Log _log;

        public static MigrationUnitCache MigrationUnitsCache
        { get; set; }

        /// <summary>
        /// In-memory storage for source connection strings, keyed by job ID.
        /// In-memory only. Never persisted to disk.
        /// Cleared on app restart — user must re-enter on resume.
        /// </summary>
        public static ConcurrentDictionary<string, string> SourceConnectionString
        { get; set; } = new();

        /// <summary>
        /// In-memory storage for target connection strings, keyed by job ID.
        /// In-memory only. Never persisted to disk.
        /// Cleared on app restart — user must re-enter on resume.
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
                || CurrentlyActiveJob.Status == JobStatus.Cancelled
                || CurrentlyActiveJob.Status == JobStatus.Completed)
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
                || CurrentlyActiveJob.Status == JobStatus.Cancelled
                || CurrentlyActiveJob.Status == JobStatus.Completed)
                return;

            _log?.WriteLine(message, LogType.Verbose);
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
                if (JobStore.CachedActiveJob != null
                    && !string.IsNullOrEmpty(ActiveMigrationJobId)
                    && JobStore.CachedActiveJob.Id
                        == ActiveMigrationJobId)
                {
                    return JobStore.CachedActiveJob;
                }

                if (!string.IsNullOrEmpty(ActiveMigrationJobId))
                {
                    JobStore.CachedActiveJob =
                        JobStore.LoadJob(ActiveMigrationJobId);
                    if (MigrationUnitsCache == null)
                        MigrationUnitsCache =
                            new MigrationUnitCache();
                    return JobStore.CachedActiveJob;
                }

                return null;
            }
        }

        public static IPersistenceStorage? Store { get; private set; }
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
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] Initialize config read failed: {ex.Message}");
            }

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
            SaveJobList();
        }

        // -- Facade: delegates to JobStore --

        public static MigrationJob? GetMigrationJob(string jobId)
            => JobStore.GetJob(jobId);

        public static List<MigrationJob> PopulateMigrationJobs(
            List<string> ids)
            => JobStore.GetAllJobs(ids);

        public static bool SaveMigrationJob(MigrationJob job)
            => JobStore.SaveJob(job);

        public static void ClearCurrentlyActiveJobCache()
            => JobStore.ClearCache();

        // -- Facade: delegates to UnitStore --

        public static bool SaveMigrationUnit(
            MigrationUnit mu, bool updateParent)
            => UnitStore.SaveUnit(mu, updateParent);

        public static MigrationUnit GetMigrationUnit(
            string key, string jobId = null)
            => UnitStore.GetUnit(key, jobId);

        public static MigrationUnit GetMigrationUnitFromStorage(
            string jobId, string unitId)
            => UnitStore.GetFromStorage(jobId, unitId);

        // -- JobList (stays here: global state) --

        private static JobList LoadJobList(
            out bool notFound, out string errorMessage)
        {
            errorMessage = string.Empty;
            notFound = false;
            string path = $"{JobStore.JobsFolder}\\joblist.json";

            try
            {
                for (int i = 0; i < 5; i++)
                {
                    try
                    {
                        if (!Store.Exists(path))
                        {
                            notFound = true;
                            errorMessage = "Job list not found.";
                        }
                        else
                        {
                            string json = Store.Read(path);
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

        public static bool SaveJobList()
        {
            try
            {
                if (JobList != null)
                {
                    lock (_writeJobListLock)
                    {
                        var filePath = Path.Combine(
                            JobStore.JobsFolder, "joblist.json");
                        string json =
                            JsonConvert.SerializeObject(
                                JobList, Formatting.Indented);
                        Store.Write(filePath, json);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] SaveJobList failed: {ex.Message}");
                return false;
            }
        }
    }
}