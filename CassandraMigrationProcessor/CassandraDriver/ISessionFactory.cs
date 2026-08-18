using Cassandra;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Creates worker-owned target sessions. Source sessions are job-owned and
/// passed directly to readers, so their lifetime cannot be confused with the
/// per-worker target-session lifetime.
/// </summary>
public interface ISessionFactory
{
    /// <summary>Mint a new keyspace-agnostic target-cluster session. Async because
    /// target credential discovery may go through ARM.</summary>
    Task<ISession> CreateTargetSessionAsync();
}

/// <summary>
/// Limits simultaneous target-session opens while retaining one target
/// session per worker. This prevents high-worker jobs from creating a
/// connection storm during startup.
/// </summary>
public sealed class GatedTargetSessionFactory : ISessionFactory
{
    private readonly ISessionFactory _inner;
    private readonly SemaphoreSlim _creationGate = new(2, 2);

    public GatedTargetSessionFactory(ISessionFactory inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public async Task<ISession> CreateTargetSessionAsync()
    {
        await _creationGate.WaitAsync().ConfigureAwait(false);
        try
        {
            return await _inner.CreateTargetSessionAsync()
                .ConfigureAwait(false);
        }
        finally
        {
            _creationGate.Release();
        }
    }
}

/// <summary>
/// Default <see cref="ISessionFactory"/> bound to a single
/// <see cref="Job"/>. Delegates to <see cref="CassandraClientFactory"/>
/// so the connection-construction policy stays in one place.
/// </summary>
public sealed class JobTargetSessionFactory : ISessionFactory
{
    private readonly MigrationLog _log;
    private readonly Job _job;

    public JobTargetSessionFactory(MigrationLog log, Job job)
    {
        _log = log;
        _job = job;
    }

    public Task<ISession> CreateTargetSessionAsync()
        => CassandraClientFactory.CreateTargetSessionAsync(_log, _job);
}
