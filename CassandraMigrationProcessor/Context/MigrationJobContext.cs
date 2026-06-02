using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Persistence;
using System.Collections.Concurrent;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Context;

/// <summary>
/// One attempt at reading <c>JobRegistry.json</c> from disk. Returned
/// by <see cref="MigrationJobContext.TryLoadJobListOnce"/> and consumed
/// by the retry loop in <see cref="MigrationJobContext.LoadJobList"/>.
/// </summary>
/// <param name="Result">Parsed registry, or <c>null</c> on miss/error.</param>
/// <param name="FileExists">
/// <c>true</c> when the file is present but unreadable (retry-eligible);
/// <c>false</c> when it has not been written yet (terminal — no retry).
/// </param>
/// <param name="Error">Human-readable failure message; empty on success.</param>
internal sealed record JobListLoadAttempt(
    JobIndex? Result, bool FileExists, string Error);

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

    /// <summary>
    /// Per-run cache of <see cref="TableMigration"/> documents. Resolves
    /// to <c>null</c> when no job is running. Owned by
    /// <see cref="DataTransfer.MigrationJobRunner.MigrationUnitsCache"/>;
    /// its lifetime matches the run.
    /// </summary>
    public TableMigrationCache? MigrationUnitsCache
        => ActiveRunner?.MigrationUnitsCache;

    /// <summary>
    /// The single process-wide cache of per-job source/target
    /// connection credentials. In-memory only; never persisted to
    /// disk. Cleared on app restart — user must re-enter on resume —
    /// and per-job entries are removed by <see cref="RetireJob"/>
    /// when a job reaches a terminal state.
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
    /// constructed; cleared in the host's finally block. All per-run
    /// state lives on this instance, so dropping the reference is
    /// sufficient to retire the run.
    /// </summary>
    public DataTransfer.MigrationJobRunner? ActiveRunner { get; set; }

    public JobIndex JobIndex { get; private set; }

    /// <summary>
    /// Symmetric teardown for a job whose background run has just
    /// returned. Always drops the auto-start hint and (if this is the
    /// active job) clears <see cref="ActiveMigrationJobId"/> and the
    /// active-job cache. Terminal retirements (Completed / Faulted /
    /// Cancelled) also evict credentials and the unit cache; Paused
    /// jobs retain them so "Resume with Existing Connection Strings"
    /// works without re-prompting.
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
            JobStore.EvictFromCache(jobId);
        }
    }

    public void UpdateLogLevel(
        LogType level, Job job)
    {
        // When a live run owns the job document, write the level
        // through that instance so the runner sees the change on its
        // next log call. Otherwise the caller's copy is the
        // authoritative one (job already terminal, or no live run).
        var target = CurrentlyActiveJob is null
            || CurrentlyActiveJob.Status == JobStatus.Cancelled
            || CurrentlyActiveJob.Status == JobStatus.Completed
                ? job
                : CurrentlyActiveJob;
        target.LogLevel = level;
        SaveMigrationJob(target);
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
    /// Idempotent: loads the active job on first access without
    /// mutating global state from a property getter.
    /// </summary>
    private readonly object _activeJobLoadLock = new object();
    private void EnsureActiveJobLoaded()
    {
        var activeId = ActiveMigrationJobId;
        if (string.IsNullOrEmpty(activeId)) return;

        if (JobStore.CachedActiveJob != null
            && JobStore.CachedActiveJob.Id == activeId)
            return;

        lock (_activeJobLoadLock)
        {
            if (JobStore.CachedActiveJob == null
                || JobStore.CachedActiveJob.Id != activeId)
            {
                JobStore.CachedActiveJob = JobStore.LoadJob(activeId);
            }
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
        // Fail fast on configuration access failure rather than
        // silently pointing persistence at the wrong store.
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

    // Facade: delegates to UnitStore
    public bool SaveMigrationUnit(
        TableMigration mu, bool updateParent)
        => UnitStore.SaveUnit(mu, updateParent);

    // Facade: delegates to UnitStore
    public TableMigration GetMigrationUnit(
        string key, string jobId = null)
        => UnitStore.GetUnit(key, jobId);

    // -- JobIndex (stays here: global state) --

    private JobIndex LoadJobList(
        out bool notFound, out string errorMessage)
    {
        errorMessage = string.Empty;
        notFound = false;

        for (int i = 0; i < 5; i++)
        {
            var attempt = TryLoadJobListOnce(JobStore.JobRegistryPath);
            if (attempt.Result != null)
            {
                JobIndex = attempt.Result;
                return attempt.Result;
            }
            if (!attempt.FileExists)
            {
                notFound = true;
                errorMessage = attempt.Error;
                return null;
            }
            if (!string.IsNullOrEmpty(attempt.Error))
                errorMessage = attempt.Error;

            Thread.Sleep(200);
        }

        errorMessage = "Error loading migration jobs.";
        return null;
    }

    /// <summary>
    /// Attempts a single load of the job list from disk. Returns a
    /// <see cref="JobListLoadAttempt"/>: <c>Result</c> is the parsed
    /// <see cref="JobIndex"/> or <c>null</c>; <c>FileExists</c>
    /// distinguishes "not yet written" from "present but unreadable"
    /// (the retry loop only retries the latter); <c>Error</c> carries
    /// the human-readable failure message when <c>Result</c> is null.
    /// </summary>
    private JobListLoadAttempt TryLoadJobListOnce(string path)
    {
        try
        {
            if (!Store.Exists(path))
                return new JobListLoadAttempt(null, false, "Job list not found.");

            var loaded = JsonStore.Read<JobIndex>(path);
            return new JobListLoadAttempt(loaded, true, string.Empty);
        }
        catch (JsonException ex)
        {
            return new JobListLoadAttempt(null, true, $"Error deserializing: {ex}");
        }
        catch (Exception ex)
        {
            return new JobListLoadAttempt(null, true, $"Error: {ex}");
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
                    JsonStore.Write(JobStore.JobRegistryPath, JobIndex);
                }
            }
            return true;
        }, false, "SaveJobList");
    }
}
