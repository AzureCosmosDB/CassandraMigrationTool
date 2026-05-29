using System.Collections.Concurrent;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
namespace CassandraMigrationProcessor.Context;

/// <summary>
/// Persistence gateway for <see cref="Job"/> definitions: reads, writes, and
/// in-memory caches per-job <c>jobdefinition.json</c> documents under the
/// shared document store. Owns no migration logic.
/// </summary>
public static class JobStore
{
    public const string JobsFolder = "migrationjobs";
    private const string JobDefinitionFile = "jobdefinition.json";

    private static readonly object _writeJobLock = new object();
    private static readonly object _cacheLock = new();

    private static ConcurrentDictionary<string, Job>
        _jobs = new();

    private static Job? _cachedActiveJob = null;

    internal static Job? CachedActiveJob
    {
        get { lock (_cacheLock) { return _cachedActiveJob; } }
        set { lock (_cacheLock) { _cachedActiveJob = value; } }
    }

    /// <summary>
    /// Build the canonical path to a job definition file.
    /// </summary>
    internal static string GetJobDefinitionPath(string jobId) =>
        Path.Combine(JobsFolder, jobId, JobDefinitionFile);

    /// <summary>
    /// Serialize a job and persist it to storage (caller must hold _writeJobLock).
    /// </summary>
    private static void SerializeAndPersist(Job job)
    {
        JsonStore.Write(GetJobDefinitionPath(job.Id), job);
    }

    internal static Job? LoadJob(string jobId)
    {
        if (_jobs.TryGetValue(jobId, out var cached))
            return cached;

        return MigrationUtilities.SafeExecute(() =>
        {
            var loadedObject = JsonStore.Read<Job>(
                GetJobDefinitionPath(jobId));
            if (loadedObject == null)
                return null;
            _jobs[jobId] = loadedObject;
            return loadedObject;
        }, (Job?)null, $"LoadJob({jobId})");
    }

    /// <summary>Retrieves a job by ID, preferring the active in-memory job if it matches.</summary>
    public static Job? GetJob(string jobId)
    {
        if (jobId == MigrationJobContext.Instance.ActiveMigrationJobId
            && MigrationJobContext.Instance.CurrentlyActiveJob != null)
            return MigrationJobContext.Instance.CurrentlyActiveJob;

        return LoadJob(jobId);
    }

    /// <summary>Loads and returns all jobs matching the given IDs.</summary>
    public static List<Job> GetAllJobs(List<string> ids)
    {
        List<Job> jobs = new();
        foreach (var id in ids)
        {
            var job = GetJob(id);
            if (job != null) jobs.Add(job);
        }
        return jobs;
    }

    /// <summary>Persists a job to disk and updates the in-memory cache.</summary>
    public static bool SaveJob(Job job)
    {
        return MigrationUtilities.SafeExecute(() =>
        {
            lock (_writeJobLock)
            {
                SerializeAndPersist(job);
                _jobs[job.Id] = job;
                if (!string.IsNullOrEmpty(
                        MigrationJobContext.Instance
                            .ActiveMigrationJobId)
                    && job.Id
                        == MigrationJobContext.Instance
                            .ActiveMigrationJobId)
                {
                    lock (_cacheLock)
                    {
                        _cachedActiveJob = job;
                    }
                }
            }
            return true;
        }, false, "SaveJob");
    }

    internal static void PersistActiveJobUnderLock()
    {
        var job = MigrationJobContext.Instance.CurrentlyActiveJob;
        if (job != null)
        {
            lock (_writeJobLock)
            {
                SerializeAndPersist(job);
            }
        }
    }

    /// <summary>Clears the cached active job reference.</summary>
    public static void ClearCache()
    {
        lock (_cacheLock)
        {
            _cachedActiveJob = null;
        }
    }
}
