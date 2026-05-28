using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;

internal class SchemaMigrator
{
    private readonly MigrationLog _log;

    public SchemaMigrator(MigrationLog log)
    {
        _log = log;
    }

    public record SchemaResult(
        List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> Columns);

    public async Task<SchemaResult?> SyncAsync(ISession sourceSession, ISession targetSession, TableContext context)
    {
        var columns = await SchemaManager.SyncSchemaAsync(
            sourceSession, targetSession,
            context.KeyspaceName, context.TableName,
            context.TargetKeyspaceName, context.TargetTableName);

        if (columns.Count == 0)
        {
            _log.WriteLine($"No columns for {context.KeyspaceName}.{context.TableName}", LogType.Error);
            return null;
        }

        return new SchemaResult(columns);
    }
}
