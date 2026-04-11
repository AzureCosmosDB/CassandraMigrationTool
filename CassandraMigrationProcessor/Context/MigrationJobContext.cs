using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Persistence;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Context
{
    public static class MigrationJobContext
    {
        private static readonly object _writeJobListLock = new object();
        private static MigrationLog _log;

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

        public static string ActiveMigrationJobId{ get; set; }

        private static volatile bool _controlledPauseRequested;
        public static bool ControlledPauseRequested
            => _controlledPauseRequested;

        public static JobRegistry JobRegistry { get; private set; }

        public static void ResetControlledPause()
        {
            AddVerboseLog("Resetting controlled pause request.");
            _controlledPauseRequested = false;
        }

        public static void RequestControlledPause(string location)
        {
            if (_log == null)
                throw new Exception("MigrationLog not initialized.");

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

        public static LogStorageCallbacks CreateLogStorageCallbacks(
            Persistence.IPersistenceStorage store)
        {
            return new LogStorageCallbacks
            {
                ReadLogs = id =>
                {
                    var bucket = store.ReadLogs(id, out var backupFile);
                    return (bucket, backupFile);
                },
                PushLogEntry = (jobId, logObj) =>
                    store.PushLogEntry(jobId, logObj),
                ExportLogsAsBytes = (id, top, bottom) =>
                    store.ExportLogsAsBytes(id, top, bottom),
                GetLogCount = id => store.GetLogCount(id),
                DownloadLogsPaginated = (id, skip, take) =>
                    store.DownloadLogsPaginated(id, skip, take),
            };
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
            MigrationUtilities.LogToFile("MigrationJobContext.Initialize started");

            var stateStoreCSorPath = string.Empty;
            var appId = string.Empty;
            try
            {
                stateStoreCSorPath =
                    configuration["StateStore:ConnectionStringOrPath"];
                appId = configuration["StateStore:AppID"];
                AppId = appId;
                DataDirectoryResolver.SetAppId(appId);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] Initialize config read failed: {ex.Message}");
            }

            Store = new DiskPersistence();
            var localPath =
                string.IsNullOrEmpty(stateStoreCSorPath)
                ? DataDirectoryResolver.GetWorkingFolder()
                : stateStoreCSorPath;
            Store.Initialize(localPath);

            JobRegistry = LoadJobList(
                out bool notFound, out string errorMessage);
            if (notFound && JobRegistry == null)
            {
                JobRegistry = new JobRegistry();
                JobRegistry.MigrationJobIds = new List<string>();
            }
            else if (JobRegistry == null
                && !string.IsNullOrEmpty(errorMessage))
            {
                throw new InvalidOperationException(
                    $"Error initializing Job List: {errorMessage}");
            }
            SaveJobList();
        }

        // Facade: delegates to JobStore

        public static MigrationJob? GetMigrationJob(string jobId)
            => JobStore.GetJob(jobId);

        // Facade: delegates to JobStore
        public static List<MigrationJob> PopulateMigrationJobs(
            List<string> ids)
            => JobStore.GetAllJobs(ids);

        // Facade: delegates to JobStore
        public static bool SaveMigrationJob(MigrationJob job)
            => JobStore.SaveJob(job);

        // Facade: delegates to JobStore
        public static void ClearCurrentlyActiveJobCache()
            => JobStore.ClearCache();

        // Facade: delegates to UnitStore
        public static bool SaveMigrationUnit(
            MigrationUnit mu, bool updateParent)
            => UnitStore.SaveUnit(mu, updateParent);

        // Facade: delegates to UnitStore
        public static MigrationUnit GetMigrationUnit(
            string key, string jobId = null)
            => UnitStore.GetUnit(key, jobId);

        // Facade: delegates to UnitStore
        public static MigrationUnit GetMigrationUnitFromStorage(
            string jobId, string unitId)
            => UnitStore.GetFromStorage(jobId, unitId);

        // -- JobRegistry (stays here: global state) --

        private static JobRegistry LoadJobList(
            out bool notFound, out string errorMessage)
        {
            errorMessage = string.Empty;
            notFound = false;
            string path = $"{JobStore.JobsFolder}\\JobRegistry.json";

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
                                .DeserializeObject<JobRegistry>(json);
                            if (obj != null)
                            {
                                JobRegistry = obj;
                                return JobRegistry;
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
            return MigrationUtilities.SafeExecute(() =>
            {
                if (JobRegistry != null)
                {
                    lock (_writeJobListLock)
                    {
                        var filePath = Path.Combine(
                            JobStore.JobsFolder, "JobRegistry.json");
                        string json =
                            JsonConvert.SerializeObject(
                                JobRegistry, Formatting.Indented);
                        Store.Write(filePath, json);
                    }
                }
                return true;
            }, false, "SaveJobList");
        }
    }
}