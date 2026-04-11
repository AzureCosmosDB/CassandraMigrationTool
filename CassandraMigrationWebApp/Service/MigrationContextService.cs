using System.Collections.Concurrent;
using CassandraMigrationProcessor.Context;
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

        public Job? GetJob(string jobId)
            => MigrationJobContext.GetMigrationJob(jobId);

        public bool SaveJob(Job job)
            => MigrationJobContext.SaveMigrationJob(job);

        public List<Job> PopulateJobs(List<string> ids)
            => MigrationJobContext.PopulateMigrationJobs(ids);

        // -- Unit operations --

        public TableMigration? GetUnit(string key, string? jobId = null)
            => MigrationJobContext.GetMigrationUnit(key, jobId);

        public bool RemoveUnit(TableMigrationSummary mub)
            => UnitStore.RemoveUnit(mub);

        // -- Job list --

        public JobIndex JobIndex => MigrationJobContext.JobIndex;

        public bool SaveJobList()
            => MigrationJobContext.SaveJobList();

        // -- Active job --

        public Job? CurrentlyActiveJob
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

        public void UpdateLogLevel(LogType level, Job job)
            => MigrationJobContext.UpdateLogLevel(level, job);

        public void AddVerboseLog(string message)
            => MigrationJobContext.AddVerboseLog(message);

        // -- Cache --

        public TableMigrationCache? MigrationUnitsCache
            => MigrationJobContext.MigrationUnitsCache;
    }
}
