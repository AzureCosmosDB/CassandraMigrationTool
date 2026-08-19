using Cassandra;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.CassandraDriver;

internal sealed record SourceSessionSettings(
    string ContactPoint,
    int Port,
    string Username,
    int MaxConnectionsPerHost);

public interface ICredentialSessionFactory
{
    ISession CreateSession(string credential);
}

internal sealed class SourceSessionFactory : ICredentialSessionFactory
{
    private readonly MigrationLog _log;
    private readonly SourceSessionSettings _settings;

    public SourceSessionFactory(
        MigrationLog log,
        SourceSessionSettings settings)
    {
        _log = log;
        _settings = settings;
    }

    public ISession CreateSession(string credential)
    {
        return CassandraClientFactory.CreateSourceSessionWithCredential(
            _log, _settings, credential);
    }
}
