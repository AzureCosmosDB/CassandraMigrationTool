using Cassandra;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Infrastructure;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.CassandraDriver;

public interface ICassandraSessionFactory
{
    ISession CreateSourceSession(MigrationLog log, ConnectionOptions connection, string keyspace);
    ISession CreateTargetSession(MigrationLog log, ConnectionOptions connection, string keyspace);
    ISession CreateSourceSession(MigrationLog log, Job job, string keyspace, TokenRefreshManager? tokenRefreshManager = null);
    Task<ISession> CreateTargetSessionAsync(MigrationLog log, Job job, string keyspace);
}
