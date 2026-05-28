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
    /// In-memory storage for source connection strings, keyed by job ID.
    /// In-memory only. Never persisted to disk.
    /// Cleared on app restart — user must re-enter on resume.
    /// </summary>
    public ConcurrentDictionary<string, string> SourceConnectionString
    { get; set; } = new();

    /// <summary>
    /// In-memory storage for target connection strings, keyed by job ID.
    /// In-memory only. Never persisted to disk.
    /// Cleared on app restart — user must re-enter on resume.
    /// </summary>
    public ConcurrentDictionary<string, string> TargetConnectionString
    { get; set; } = new();

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

    private volatile bool _controlledPauseRequested;
    public bool ControlledPauseRequested
        => _controlledPauseRequested;

    // Subscribers (e.g. JobPipeline) register to react to a pause
    // request synchronously — workers waiting on BulkDrainSignal
    // otherwise stay blocked because pause is a soft flag and never
    // trips their CancellationToken.
    public event Action PauseRequested;

    public JobIndex JobIndex { get; private set; }

    public void ResetControlledPause()
    {
        _controlledPauseRequested = false;
    }

    public void RequestControlledPause()
    {
        _controlledPauseRequested = true;
        PauseRequested?.Invoke();
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
