using Cassandra;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Creates worker-owned sessions. The consumer determines the session role;
/// job-owned shared sessions are passed directly instead of using this factory.
/// </summary>
public interface ISessionFactory
{
    /// <summary>Mint a new keyspace-agnostic session.</summary>
    Task<ISession> CreateSessionAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Limits simultaneous session opens. This prevents high-worker jobs from
/// creating a connection storm during startup.
/// </summary>
public sealed class GatedSessionFactory : ISessionFactory, IDisposable
{
    private const int MaxConcurrentSessionCreations = 20;

    private readonly ISessionFactory _inner;
    private readonly SemaphoreSlim _creationGate = new(
        MaxConcurrentSessionCreations,
        MaxConcurrentSessionCreations);

    public GatedSessionFactory(ISessionFactory inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public async Task<ISession> CreateSessionAsync(CancellationToken cancellationToken)
    {
        await _creationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await _inner.CreateSessionAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _creationGate.Release();
        }
    }

    public void Dispose() => _creationGate.Dispose();
}

/// <summary>
/// Default <see cref="ISessionFactory"/> bound to a single
/// <see cref="Job"/>. Delegates to <see cref="CassandraClientFactory"/>
/// so the connection-construction policy stays in one place.
/// </summary>
public sealed class JobSessionFactory : ISessionFactory
{
    private readonly MigrationLog _log;
    private readonly Job _job;

    public JobSessionFactory(MigrationLog log, Job job)
    {
        _log = log;
        _job = job;
    }

    public async Task<ISession> CreateSessionAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return await CassandraClientFactory.CreateTargetSessionAsync(_log, _job)
            .ConfigureAwait(false);
    }
}
