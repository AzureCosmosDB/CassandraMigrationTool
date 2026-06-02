using System.Collections.Concurrent;

namespace CassandraMigrationProcessor.Context;

/// <summary>
/// Per-job source/target connection-string pair held only in memory;
/// never persisted to disk.
/// </summary>
public sealed record JobCredentials(
    string SourceConnectionString,
    string TargetConnectionString);

/// <summary>
/// Process-wide cache of per-job connection credentials.
/// </summary>
public sealed class ConnectionCredentialCache
{
    private readonly ConcurrentDictionary<string, JobCredentials> _byJobId = new();

    /// <summary>
    /// Stores (or replaces) the credentials for a job.
    /// </summary>
    public void Remember(
        string jobId,
        string sourceConnectionString,
        string targetConnectionString)
    {
        if (string.IsNullOrEmpty(jobId)) return;
        _byJobId[jobId] = new JobCredentials(
            sourceConnectionString ?? string.Empty,
            targetConnectionString ?? string.Empty);
    }

    /// <summary>
    /// True iff credentials for <paramref name="jobId"/> are cached.
    /// </summary>
    public bool TryGet(string jobId, out JobCredentials creds)
    {
        if (string.IsNullOrEmpty(jobId))
        {
            creds = new JobCredentials(string.Empty, string.Empty);
            return false;
        }
        return _byJobId.TryGetValue(jobId, out creds!);
    }

    /// <summary>
    /// Returns the source connection string, or empty if not cached.
    /// </summary>
    public string GetSource(string jobId)
        => TryGet(jobId, out var c) ? c.SourceConnectionString : string.Empty;

    /// <summary>
    /// Returns the target connection string, or empty if not cached.
    /// </summary>
    public string GetTarget(string jobId)
        => TryGet(jobId, out var c) ? c.TargetConnectionString : string.Empty;

    /// <summary>
    /// Updates only the source half of the cached credentials,
    /// preserving any existing target.
    /// </summary>
    public void SetSource(string jobId, string sourceConnectionString) =>
        Mutate(jobId, existing => existing with
        {
            SourceConnectionString = sourceConnectionString ?? string.Empty
        });

    /// <summary>
    /// Updates only the target half of the cached credentials for
    /// <paramref name="jobId"/>, preserving any existing source.
    /// </summary>
    public void SetTarget(string jobId, string targetConnectionString) =>
        Mutate(jobId, existing => existing with
        {
            TargetConnectionString = targetConnectionString ?? string.Empty
        });

    /// <summary>
    /// Applies <paramref name="mutate"/> to the cached credentials for
    /// <paramref name="jobId"/>, seeding an empty record when the job
    /// has no entry yet. Centralises the AddOrUpdate dance shared by
    /// <see cref="SetSource"/> and <see cref="SetTarget"/>.
    /// </summary>
    private void Mutate(string jobId, Func<JobCredentials, JobCredentials> mutate)
    {
        if (string.IsNullOrEmpty(jobId)) return;
        _byJobId.AddOrUpdate(
            jobId,
            _ => mutate(new JobCredentials(string.Empty, string.Empty)),
            (_, existing) => mutate(existing));
    }

    /// <summary>
    /// Removes cached credentials. Idempotent.
    /// </summary>
    public void Forget(string jobId)
    {
        if (string.IsNullOrEmpty(jobId)) return;
        _byJobId.TryRemove(jobId, out _);
    }
}
