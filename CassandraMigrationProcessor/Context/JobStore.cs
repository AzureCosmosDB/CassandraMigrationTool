using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
namespace CassandraMigrationProcessor.Context;
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
        var filePath = GetJobDefinitionPath(job.Id);
        string json = JsonConvert.SerializeObject(
            job, Formatting.Indented);
        MigrationJobContext.Store.Write(filePath, json);
    }

    internal static Job? LoadJob(string jobId)
    {
        if (_jobs.TryGetValue(jobId, out var cached))
            return cached;

        return MigrationUtilities.SafeExecute(() =>
        {
            var filePath = GetJobDefinitionPath(jobId);
            var json = MigrationJobContext.Store.Read(
                filePath);
            var loadedObject =
                JsonConvert.DeserializeObject<Job>(json);
            if (loadedObject == null)
                return null;
            _jobs[jobId] = loadedObject;
            return loadedObject;
        }, (Job?)null, $"LoadJob({jobId})");
    }

    public static Job? GetJob(string jobId)
    {
        if (jobId == MigrationJobContext.ActiveMigrationJobId
            && MigrationJobContext.CurrentlyActiveJob != null)
            return MigrationJobContext.CurrentlyActiveJob;

        return LoadJob(jobId);
    }

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

    public static bool SaveJob(Job job)
    {
        return MigrationUtilities.SafeExecute(() =>
        {
            lock (_writeJobLock)
            {
                SerializeAndPersist(job);
                _jobs[job.Id] = job;
                if (!string.IsNullOrEmpty(
                        MigrationJobContext
                            .ActiveMigrationJobId)
                    && job.Id
                        == MigrationJobContext
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
        var job = MigrationJobContext.CurrentlyActiveJob;
        if (job != null)
        {
            lock (_writeJobLock)
            {
                SerializeAndPersist(job);
            }
        }
    }

    public static void ClearCache()
    {
        lock (_cacheLock)
        {
            _cachedActiveJob = null;
        }
    }
}
