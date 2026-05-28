using Cassandra;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Per-job session factory. Encapsulates everything required to mint a
/// new source or target <see cref="ISession"/> (job credentials, logger,
/// optional token refresh manager) so that consumers — primarily
/// <see cref="DataTransfer.PageReader"/> and
/// <see cref="DataTransfer.PageWriter"/> — depend on a single
/// abstraction instead of being threaded the raw <see cref="Job"/> and
/// <see cref="TokenRefreshManager"/> separately. This keeps worker-side
/// classes focused on data movement and makes it possible to swap the
/// connection wiring in tests without touching their constructors.
/// </summary>
public interface ISessionFactory
{
    /// <summary>Mint a new keyspace-agnostic source-cluster session.</summary>
    ISession CreateSourceSession();

    /// <summary>Mint a new keyspace-agnostic target-cluster session. Async because
    /// target credential discovery may go through ARM.</summary>
    Task<ISession> CreateTargetSessionAsync();
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
    private readonly TokenRefreshManager? _tokenRefreshManager;

    public JobSessionFactory(MigrationLog log, Job job, TokenRefreshManager? tokenRefreshManager)
    {
        _log = log;
        _job = job;
        _tokenRefreshManager = tokenRefreshManager;
    }

    public ISession CreateSourceSession()
        => CassandraClientFactory.CreateSourceSession(_log, _job, _tokenRefreshManager);

    public Task<ISession> CreateTargetSessionAsync()
        => CassandraClientFactory.CreateTargetSessionAsync(_log, _job);
}
