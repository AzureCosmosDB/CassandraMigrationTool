using System.Collections.Concurrent;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Persistence;

namespace CassandraMigrationWebApp.Service;
/// <summary>
/// Thin wrapper around <see cref="MigrationJobContext"/>,
/// <see cref="JobStore"/>, and <see cref="UnitStore"/> classes.
/// Registered as a singleton in DI so that controllers, Razor pages,
/// and services can depend on it via constructor/property injection
/// instead of reaching for static members directly.
/// The processor library continues to use MigrationJobContext.Instance.
/// </summary>
public class MigrationContextService
{
    private readonly MigrationJobContext _context;

    public MigrationContextService(MigrationJobContext context)
    {
        _context = context;
    }

    // -- Job operations --

    public Job? GetJob(string jobId)
        => _context.GetMigrationJob(jobId);

    public bool SaveJob(Job job)
        => _context.SaveMigrationJob(job);

    public List<Job> PopulateJobs(List<string> ids)
        => _context.PopulateMigrationJobs(ids);

    // -- Unit operations --

    public TableMigration? GetUnit(string key, string? jobId = null)
        => _context.GetMigrationUnit(key, jobId);

    public bool RemoveUnit(TableMigrationSummary mub)
        => UnitStore.RemoveUnit(mub);

    // -- Job list --

    public JobIndex JobIndex => _context.JobIndex;

    public bool SaveJobList()
        => _context.SaveJobList();

    // -- Active job --

    public Job? CurrentlyActiveJob
        => _context.CurrentlyActiveJob;

    public string? ActiveMigrationJobId
    {
        get => _context.ActiveMigrationJobId;
        set => _context.ActiveMigrationJobId = value!;
    }

    // -- Controlled pause --

    public bool ControlledPauseRequested
        => _context.ControlledPauseRequested;

    public void RequestControlledPause(string location)
        => _context.RequestControlledPause(location);

    public void ResetControlledPause()
        => _context.ResetControlledPause();

    // -- Persistence --

    public IDocumentStorage? Store
        => _context.Store;

    public ILogStorage? LogStore
        => _context.LogStore;

    // -- Connection strings (in-memory only) --

    public ConcurrentDictionary<string, string> SourceConnectionString
        => _context.SourceConnectionString;

    public ConcurrentDictionary<string, string> TargetConnectionString
        => _context.TargetConnectionString;

    // -- Auto-start flags --

    public ConcurrentDictionary<string, byte> PendingAutoStartJobIds
        => _context.PendingAutoStartJobIds;

    // -- Logging --

    public void UpdateLogLevel(LogType level, Job job)
        => _context.UpdateLogLevel(level, job);

    public void AddVerboseLog(string message)
        => _context.AddVerboseLog(message);

    // -- Cache --

    public TableMigrationCache? MigrationUnitsCache
        => _context.MigrationUnitsCache;
}
