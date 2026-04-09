using System.Collections.Concurrent;
using CassandraMigrationProcessor;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers.JobManagement;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Persistence;

namespace CassandraMigrationWebApp.Service
{
    /// <summary>
    /// Thin wrapper around the static <see cref="MigrationJobContext"/>,
    /// <see cref="JobStore"/>, and <see cref="UnitStore"/> classes.
    /// Registered as a singleton in DI so that controllers, Razor pages,
    /// and services can depend on it via constructor/property injection
    /// instead of reaching for static members directly.
    /// The processor library continues to use the static classes unchanged.
    /// </summary>
    public class MigrationContextService
    {
        // -- Job operations --

        public MigrationJob? GetJob(string jobId)
            => MigrationJobContext.GetMigrationJob(jobId);

        public bool SaveJob(MigrationJob job)
            => MigrationJobContext.SaveMigrationJob(job);

        public List<MigrationJob> PopulateJobs(List<string> ids)
            => MigrationJobContext.PopulateMigrationJobs(ids);

        public void ClearActiveJobCache()
            => MigrationJobContext.ClearCurrentlyActiveJobCache();

        // -- Unit operations --

        public MigrationUnit? GetUnit(string key, string? jobId = null)
            => MigrationJobContext.GetMigrationUnit(key, jobId);

        public bool SaveUnit(MigrationUnit mu, bool updateParent)
            => MigrationJobContext.SaveMigrationUnit(mu, updateParent);

        public MigrationUnit? GetUnitFromStorage(string jobId, string unitId)
            => MigrationJobContext.GetMigrationUnitFromStorage(jobId, unitId);

        public bool RemoveUnit(MigrationUnitBasic mub)
            => UnitStore.RemoveUnit(mub);

        // -- Job list --

        public JobList JobList => MigrationJobContext.JobList;

        public bool SaveJobList()
            => MigrationJobContext.SaveJobList();

        // -- Active job --

        public MigrationJob? CurrentlyActiveJob
            => MigrationJobContext.CurrentlyActiveJob;

        public string? ActiveMigrationJobId
        {
            get => MigrationJobContext.ActiveMigrationJobId;
            set => MigrationJobContext.ActiveMigrationJobId = value!;
        }

        // -- Controlled pause --

        public bool ControlledPauseRequested
            => MigrationJobContext.ControlledPauseRequested;

        public void RequestControlledPause(string location)
            => MigrationJobContext.RequestControlledPause(location);

        public void ResetControlledPause()
            => MigrationJobContext.ResetControlledPause();

        // -- Persistence --

        public IPersistenceStorage? Store
            => MigrationJobContext.Store;

        // -- Connection strings (in-memory only) --

        public ConcurrentDictionary<string, string> SourceConnectionString
            => MigrationJobContext.SourceConnectionString;

        public ConcurrentDictionary<string, string> TargetConnectionString
            => MigrationJobContext.TargetConnectionString;

        // -- Auto-start flags --

        public ConcurrentDictionary<string, byte> PendingAutoStartJobIds
            => MigrationJobContext.PendingAutoStartJobIds;

        // -- Logging --

        public void UpdateLogLevel(LogType level, MigrationJob job)
            => MigrationJobContext.UpdateLogLevel(level, job);

        public void AddVerboseLog(string message)
            => MigrationJobContext.AddVerboseLog(message);

        // -- Cache --

        public MigrationUnitCache? MigrationUnitsCache
            => MigrationJobContext.MigrationUnitsCache;
    }
}
