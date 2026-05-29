using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Persistence;
using System.Collections.Concurrent;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Context;

/// <summary>
/// Process-wide ambient state for the migration host: wires up persistence
/// (<see cref="IDocumentStorage"/> + <see cref="ILogStorage"/>), tracks the
/// currently active job, holds the <see cref="JobIndex"/>, and exposes
/// thin facades over <see cref="JobStore"/> / <see cref="UnitStore"/>.
/// </summary>
public class MigrationJobContext
{
    /// <summary>
    /// Static accessor for backward compatibility in the Processor
    /// library (no DI container). Set automatically by Initialize().
    /// </summary>
    public static MigrationJobContext Instance { get; private set; }

    private readonly object _writeJobListLock = new object();

    public TableMigrationCache MigrationUnitsCache
    { get; set; }

    /// <summary>
    /// The single process-wide cache of per-job source/target
    /// connection credentials. In-memory only; never persisted to
    /// disk. Cleared on app restart — user must re-enter on resume —
    /// and per-job entries are removed by <see cref="RetireJob"/>
    /// when a job reaches a terminal state.
    /// <para>
    /// In the target architecture (see
    /// <c>docs/TargetArchitecture.md</c>) this is the only cross-job
    /// dictionary that survives at the app level; every other piece
    /// of per-job state moves into a per-run <see cref="DataTransfer.MigrationJobRunner"/>.
    /// </para>
    /// </summary>
    public ConnectionCredentialCache Credentials { get; }
        = new ConnectionCredentialCache();

    /// <summary>
    /// In-memory set of job IDs that should auto-start when
    /// the viewer page opens. Cleared after the job starts.
    /// Never persisted to disk.
    /// </summary>
    public ConcurrentDictionary<string, byte> PendingAutoStartJobIds
    { get; set; } = new();

    private volatile string _activeMigrationJobId;
    public string ActiveMigrationJobId
    {
        get => _activeMigrationJobId;
        set => _activeMigrationJobId = value;
    }

    /// <summary>
    /// The job runner that currently owns the active migration, or
    /// <c>null</c> when no job is running. Set by the host
    /// (<c>JobManager.StartMigration</c>) once the runner is
    /// constructed and cleared in the host's finally block after
    /// the run returns. All per-run state — pipeline, coordinators,
    /// CTS — lives on this instance, so dropping the reference is
    /// sufficient to retire the run; no separate scrub of
    /// process-wide dictionaries is needed.
    /// </summary>
    /// <remarks>
    /// In the target architecture (see
    /// <c>docs/TargetArchitecture.md</c>) this property graduates to
    /// <c>AppHost.ActiveRunner</c> and is the canonical "what's
    /// running right now" hook the future <c>JobSupervisor</c> will
    /// read.
    /// </remarks>
    public DataTransfer.MigrationJobRunner? ActiveRunner { get; set; }

    public JobIndex JobIndex { get; private set; }

    /// <summary>
    /// Symmetric teardown for a job whose background run has just
    /// returned. Without this primitive the per-job entries in the
    /// process-wide singletons (connection strings, unit cache, loaded
    /// job, active-job pointer, auto-start hint) accumulate across job
    /// lifetimes — credentials linger in memory after the job that owned
    /// them has completed, and stale <c>ActiveMigrationJobId</c> /
    /// <c>JobStore.CachedActiveJob</c> entries cause the next job to
    /// observe state that belonged to its predecessor.
    /// <para>
    /// Always-on cleanup runs for every retirement (Paused included):
    /// drop the auto-start hint, and if this job is the currently active
    /// one, clear <see cref="ActiveMigrationJobId"/> and the active-job
    /// cache so subsequent reads do not short-circuit through stale data.
    /// </para>
    /// <para>
    /// Terminal-only cleanup runs when <paramref name="isTerminal"/> is
    /// true (Completed / Faulted / Cancelled): credentials are removed
    /// from <see cref="Credentials"/>, the unit cache is evicted
    /// for this job, and the loaded-job entry in <see cref="JobStore"/>
    /// is dropped so the next read comes from disk.
    /// </para>
    /// <para>
    /// Paused jobs intentionally retain their entries in
    /// <see cref="Credentials"/> and the unit cache so "Resume with
    /// Existing Connection Strings" continues to work without
    /// re-prompting.
    /// </para>
    /// </summary>
    public void RetireJob(string jobId, bool isTerminal)
    {
        if (string.IsNullOrEmpty(jobId)) return;

        PendingAutoStartJobIds.TryRemove(jobId, out _);

        if (string.Equals(ActiveMigrationJobId, jobId, StringComparison.Ordinal))
        {
            ActiveMigrationJobId = string.Empty;
            JobStore.ClearCache();
        }

        if (isTerminal)
        {
            Credentials.Forget(jobId);
            MigrationUnitsCache?.RemoveAllForJob(jobId);
            JobStore.EvictFromCache(jobId);
        }
    }

    public void UpdateLogLevel(
        LogType level, Job job)
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

    public LogStorageCallbacks CreateLogStorageCallbacks(
        Persistence.ILogStorage store)
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

    public Job? CurrentlyActiveJob
    {
        get
        {
            EnsureActiveJobLoaded();
            return JobStore.CachedActiveJob;
        }
    }

    /// <summary>
    /// Idempotent: loads the active job and primes the unit cache on
    /// first access. Previously a side effect of the
    /// <c>CurrentlyActiveJob</c> getter — a property read should not
    /// mutate global state or lose a racy second instantiation of the
    /// unit cache.
    /// </summary>
    private readonly object _activeJobLoadLock = new object();
    private void EnsureActiveJobLoaded()
    {
        var activeId = ActiveMigrationJobId;
        if (string.IsNullOrEmpty(activeId)) return;

        if (JobStore.CachedActiveJob != null
            && JobStore.CachedActiveJob.Id == activeId
            && MigrationUnitsCache != null)
            return;

        lock (_activeJobLoadLock)
        {
            if (JobStore.CachedActiveJob == null
                || JobStore.CachedActiveJob.Id != activeId)
            {
                JobStore.CachedActiveJob = JobStore.LoadJob(activeId);
            }
            if (MigrationUnitsCache == null)
                MigrationUnitsCache = new TableMigrationCache();
        }
    }

    public IDocumentStorage? Store { get; private set; }
    public ILogStorage? LogStore { get; private set; }
    public string? AppId { get; set; }

    public void Initialize(IConfiguration configuration)
    {
        Instance = this;

        var stateStoreCSorPath = ReadConfig(configuration);
        InitializePersistence(stateStoreCSorPath);
        BootstrapJobList();
    }

    private string ReadConfig(IConfiguration configuration)
    {
        // Configuration access failure indicates a misconfigured host
        // (missing IConfiguration provider, etc.). Silently falling back
        // to an empty path would point persistence at the wrong store
        // and let resume target the wrong job state. Fail fast instead.
        var path = configuration["StateStore:ConnectionStringOrPath"];
        var appId = configuration["StateStore:AppID"];
        AppId = appId;
        DataDirectoryResolver.SetAppId(appId);
        return path ?? string.Empty;
    }

    private void InitializePersistence(string stateStoreCSorPath)
    {
        var persistence = new DiskPersistence();
        var localPath =
            string.IsNullOrEmpty(stateStoreCSorPath)
            ? DataDirectoryResolver.GetWorkingFolder()
            : stateStoreCSorPath;
        persistence.Initialize(localPath);

        Store = persistence;
        LogStore = persistence;
    }

    private void BootstrapJobList()
    {
        JobIndex = LoadJobList(
            out bool notFound, out string errorMessage);
        if (notFound && JobIndex == null)
        {
            JobIndex = new JobIndex();
            JobIndex.MigrationJobIds = new List<string>();
        }
        else if (JobIndex == null
            && !string.IsNullOrEmpty(errorMessage))
        {
            throw new InvalidOperationException(
                $"Error initializing Job List: {errorMessage}");
        }
        SaveJobList();
    }

    // Facade: delegates to JobStore

    public Job? GetMigrationJob(string jobId)
        => JobStore.GetJob(jobId);

    // Facade: delegates to JobStore
    public List<Job> GetJobsById(
        List<string> ids)
        => JobStore.GetAllJobs(ids);

    // Facade: delegates to JobStore
    public bool SaveMigrationJob(Job job)
        => JobStore.SaveJob(job);

    // Facade: delegates to JobStore
    public void ClearCurrentlyActiveJobCache()
        => JobStore.ClearCache();

    // Facade: delegates to UnitStore
    public bool SaveMigrationUnit(
        TableMigration mu, bool updateParent)
        => UnitStore.SaveUnit(mu, updateParent);

    // Facade: delegates to UnitStore
    public TableMigration GetMigrationUnit(
        string key, string jobId = null)
        => UnitStore.GetUnit(key, jobId);

    // Facade: delegates to UnitStore
    public TableMigration GetMigrationUnitFromStorage(
        string jobId, string unitId)
        => UnitStore.GetFromStorage(jobId, unitId);

    // -- JobIndex (stays here: global state) --

    private JobIndex LoadJobList(
        out bool notFound, out string errorMessage)
    {
        errorMessage = string.Empty;
        notFound = false;
        string path = $"{JobStore.JobsFolder}\\JobRegistry.json";

        for (int i = 0; i < 5; i++)
        {
            var (result, found, error) = TryLoadJobListOnce(path);
            if (result != null)
            {
                JobIndex = result;
                return result;
            }
            if (!found)
            {
                notFound = true;
                errorMessage = error;
                return null;
            }
            if (!string.IsNullOrEmpty(error))
                errorMessage = error;

            Thread.Sleep(200);
        }

        errorMessage = "Error loading migration jobs.";
        return null;
    }

    /// <summary>
    /// Attempts a single load of the job list from disk.
    /// Returns (result, fileExists, errorMessage).
    /// </summary>
    private (JobIndex? result, bool fileExists, string error) TryLoadJobListOnce(string path)
    {
        try
        {
            if (!Store.Exists(path))
                return (null, false, "Job list not found.");

            string json = Store.Read(path);
            var obj = JsonConvert.DeserializeObject<JobIndex>(json);
            return (obj, true, string.Empty);
        }
        catch (JsonException ex)
        {
            return (null, true, $"Error deserializing: {ex}");
        }
        catch (Exception ex)
        {
            return (null, true, $"Error: {ex}");
        }
    }

    public bool SaveJobList()
    {
        return MigrationUtilities.SafeExecute(() =>
        {
            if (JobIndex != null)
            {
                lock (_writeJobListLock)
                {
                    var filePath = Path.Combine(
                        JobStore.JobsFolder, "JobRegistry.json");
                    string json =
                        JsonConvert.SerializeObject(
                            JobIndex, Formatting.Indented);
                    Store.Write(filePath, json);
                }
            }
            return true;
        }, false, "SaveJobList");
    }
}
