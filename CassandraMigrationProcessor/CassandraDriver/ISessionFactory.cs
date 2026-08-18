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
    /// <summary>
    /// Whether sessions returned by this factory are owned by the caller.
    /// Shared job sessions are owned by the migration runner instead.
    /// </summary>
    bool CallerOwnsSourceSession => true;
    bool CallerOwnsTargetSession => true;

    /// <summary>Mint a new keyspace-agnostic source-cluster session.</summary>
    ISession CreateSourceSession();

    /// <summary>Mint a new keyspace-agnostic target-cluster session. Async because
    /// target credential discovery may go through ARM.</summary>
    Task<ISession> CreateTargetSessionAsync();
}

/// <summary>
/// Exposes the runner's job-wide source session while retaining per-worker
/// target sessions. Sharing the source avoids the Cosmos metadata connection
/// storm; independent target sessions preserve the writer capacity required
/// by high-concurrency bulk jobs.
/// </summary>
public sealed class SharedSourceSessionFactory : ISessionFactory
{
    private readonly ISession _sourceSession;
    private readonly ISessionFactory _targetSessionFactory;
    private readonly SemaphoreSlim _targetSessionCreationGate = new(2, 2);

    public SharedSourceSessionFactory(ISession sourceSession, ISessionFactory targetSessionFactory)
    {
        _sourceSession = sourceSession ?? throw new ArgumentNullException(nameof(sourceSession));
        _targetSessionFactory = targetSessionFactory
            ?? throw new ArgumentNullException(nameof(targetSessionFactory));
    }

    public bool CallerOwnsSourceSession => false;

    public ISession CreateSourceSession() => _sourceSession;

    public async Task<ISession> CreateTargetSessionAsync()
    {
        await _targetSessionCreationGate.WaitAsync().ConfigureAwait(false);
        try
        {
            return await _targetSessionFactory.CreateTargetSessionAsync()
                .ConfigureAwait(false);
        }
        finally
        {
            _targetSessionCreationGate.Release();
        }
    }
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
