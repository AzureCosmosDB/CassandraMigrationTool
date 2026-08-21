using Cassandra;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Creates worker-owned target sessions for a job while limiting simultaneous
/// opens to prevent a connection storm during startup.
/// </summary>
internal sealed class JobSessionFactory
{
    private const int MaxConcurrentSessionCreations = 20;

    private readonly MigrationLog _log;
    private readonly Job _job;
    private readonly SemaphoreSlim _creationGate = new(
        MaxConcurrentSessionCreations,
        MaxConcurrentSessionCreations);

    public JobSessionFactory(MigrationLog log, Job job)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _job = job ?? throw new ArgumentNullException(nameof(job));
    }

    public async Task<ISession> CreateSessionAsync(CancellationToken cancellationToken)
    {
        await _creationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await CassandraClientFactory.CreateTargetSessionAsync(
                _log, _job).ConfigureAwait(false);
        }
        finally
        {
            _creationGate.Release();
        }
    }
}
