using Cassandra;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.CassandraDriver;

public interface ICassandraSessionFactory
{
    ISession CreateSourceSession(MigrationLog log, ConnectionOptions connection, string keyspace);
    ISession CreateTargetSession(MigrationLog log, ConnectionOptions connection, string keyspace);
    ISession CreateSourceSession(MigrationLog log, Job job, string keyspace, TokenRefreshManager? tokenRefreshManager = null);
    ISession CreateTargetSession(MigrationLog log, Job job, string keyspace);
}
