using Cassandra;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Thin instance wrapper around the static CassandraClientFactory,
/// implementing ICassandraSessionFactory for dependency injection and testability.
/// </summary>
public class CassandraSessionFactory : ICassandraSessionFactory
{
    public ISession CreateSourceSession(MigrationLog log, ConnectionOptions connection, string keyspace)
        => CassandraClientFactory.CreateSourceSession(log, connection, keyspace);

    public ISession CreateTargetSession(MigrationLog log, ConnectionOptions connection, string keyspace)
        => CassandraClientFactory.CreateTargetSession(log, connection, keyspace);

    public ISession CreateSourceSession(MigrationLog log, Job job, string keyspace, TokenRefreshManager? tokenRefreshManager = null)
        => CassandraClientFactory.CreateSourceSession(log, job, keyspace, tokenRefreshManager);

    public ISession CreateTargetSession(MigrationLog log, Job job, string keyspace)
        => CassandraClientFactory.CreateTargetSession(log, job, keyspace);
}
