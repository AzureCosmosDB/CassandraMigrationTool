using System.Collections.Concurrent;

namespace CassandraMigrationProcessor.Context;

/// <summary>
/// Per-job source/target connection-string pair. Held only in memory;
/// never persisted to disk. Passwords/AAD tokens reside in this record
/// while a job is in the host's catalog so the operator can press
/// "Resume with Existing Connection Strings" without re-entering them.
/// </summary>
public sealed record JobCredentials(
    string SourceConnectionString,
    string TargetConnectionString);

/// <summary>
/// The single process-wide cache of per-job connection credentials.
/// <para>
/// In the target architecture (see <c>docs/TargetArchitecture.md</c>)
/// this is the <b>only</b> cross-job dictionary that survives at the
/// app level. Every other piece of per-job state moves into a per-run
/// <c>JobRunner</c> object whose lifetime is the job's lifetime and
/// whose disposal collapses the entire per-job state subtree.
/// </para>
/// <para>
/// Today this cache is held by <see cref="MigrationJobContext"/> for
/// continuity with the existing <c>SourceConnectionString</c> /
/// <c>TargetConnectionString</c> properties; once <c>AppHost</c> is
/// introduced the cache moves there unchanged.
/// </para>
/// </summary>
public sealed class ConnectionCredentialCache
{
    private readonly ConcurrentDictionary<string, JobCredentials> _byJobId = new();

    /// <summary>
    /// Stores (or replaces) the credentials for a job. Subsequent
    /// <see cref="TryGet"/> calls for the same id return these
    /// credentials until <see cref="Forget"/> evicts them or the
    /// process restarts.
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
    /// True iff credentials for <paramref name="jobId"/> are present in
    /// the cache. Returns the credentials in <paramref name="creds"/>
    /// when true; default record otherwise.
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
    /// Returns the source connection string for <paramref name="jobId"/>,
    /// or empty string if no credentials are cached. Convenience for
    /// existing call sites that read individual fields rather than
    /// the pair.
    /// </summary>
    public string GetSource(string jobId)
        => TryGet(jobId, out var c) ? c.SourceConnectionString : string.Empty;

    /// <summary>
    /// Returns the target connection string for <paramref name="jobId"/>,
    /// or empty string if no credentials are cached.
    /// </summary>
    public string GetTarget(string jobId)
        => TryGet(jobId, out var c) ? c.TargetConnectionString : string.Empty;

    /// <summary>
    /// Updates only the source half of the cached credentials for
    /// <paramref name="jobId"/>, preserving any existing target. Used
    /// by the viewer's lazy-init path that falls back to
    /// <c>SourceContactPoint</c> and caches it for the rest of the
    /// session without forcing the caller to also know the target.
    /// </summary>
    public void SetSource(string jobId, string sourceConnectionString)
    {
        if (string.IsNullOrEmpty(jobId)) return;
        _byJobId.AddOrUpdate(
            jobId,
            _ => new JobCredentials(sourceConnectionString ?? string.Empty, string.Empty),
            (_, existing) => existing with
            {
                SourceConnectionString = sourceConnectionString ?? string.Empty
            });
    }

    /// <summary>
    /// Updates only the target half of the cached credentials for
    /// <paramref name="jobId"/>, preserving any existing source.
    /// </summary>
    public void SetTarget(string jobId, string targetConnectionString)
    {
        if (string.IsNullOrEmpty(jobId)) return;
        _byJobId.AddOrUpdate(
            jobId,
            _ => new JobCredentials(string.Empty, targetConnectionString ?? string.Empty),
            (_, existing) => existing with
            {
                TargetConnectionString = targetConnectionString ?? string.Empty
            });
    }

    /// <summary>
    /// Removes any cached credentials for <paramref name="jobId"/>.
    /// Idempotent; safe to call when nothing is cached. Invoked by
    /// <see cref="MigrationJobContext.RetireJob"/> on terminal job
    /// states so credentials do not linger in RAM after the job that
    /// owned them has finished.
    /// </summary>
    public void Forget(string jobId)
    {
        if (string.IsNullOrEmpty(jobId)) return;
        _byJobId.TryRemove(jobId, out _);
    }
}
