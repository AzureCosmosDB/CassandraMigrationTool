using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System.Collections.Concurrent;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Coordinates source UDT registration across every reader in a job. Each
/// session/keyspace pair is registered once; refreshed sessions get their own
/// registration entry.
/// </summary>
internal sealed class SourceUdtRegistrationCache
{
    private readonly ConcurrentDictionary<(ISession Session, string Keyspace), Lazy<Task>>
        _registrations = new();

    public Task EnsureRegisteredAsync(
        ISession session,
        string keyspace,
        WorkerLog log)
    {
        return _registrations.GetOrAdd(
            (session, keyspace),
            key => new Lazy<Task>(
                () => RegisterAsync(key.Session, key.Keyspace, log),
                LazyThreadSafetyMode.ExecutionAndPublication))
            .Value;
    }

    private static async Task RegisterAsync(
        ISession session,
        string keyspace,
        WorkerLog log)
    {
        try
        {
            var allUdts = await SchemaManager.GetUserDefinedTypesAsync(
                session, keyspace);
            await DynamicUdtRegistrar.RegisterAsync(
                session, keyspace, allUdts);
        }
        catch (Exception ex)
        {
            log.WriteLine(
                $"FATAL: UDT mapping registration on source failed for {keyspace}: {ex.Message}",
                LogType.Error);
            throw;
        }
    }
}
