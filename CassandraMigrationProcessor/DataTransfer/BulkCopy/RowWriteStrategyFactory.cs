using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Single entry point for picking and constructing the right
/// <see cref="IRowWriteStrategy"/> for a target table. The factory is
/// pure dispatch: it builds the shared per-worker
/// <see cref="RetryPolicy"/> and routes to the variant whose factory
/// owns its own statement preparation:
/// <list type="bullet">
///   <item><see cref="RegularRowWriteStrategy.CreateAsync"/> prepares
///         the INSERT;</item>
///   <item><see cref="CounterRowWriteStrategy.CreateAsync"/> prepares
///         the UPDATE and the read-modify-write SELECT-by-PK.</item>
/// </list>
/// </summary>
internal static class RowWriteStrategyFactory
{
    public static async Task<IRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession, WorkerConfig config, int maxWriteRetries)
    {
        // One policy per worker, shared across rows and across strategy
        // variants. Linear 500ms × attempt matches the historical schedule.
        var retryPolicy = RetryPolicy.Linear(maxWriteRetries, TimeSpan.FromMilliseconds(500));

        if (CassandraQueries.IsCounterTable(config.Columns))
            return await CounterRowWriteStrategy.CreateAsync(log, targetSession, config, retryPolicy);

        return await RegularRowWriteStrategy.CreateAsync(log, targetSession, config, retryPolicy);
    }

    /// <summary>
    /// Builds the bind-order → source-index map used by every strategy:
    /// for each bind slot, the index into <see cref="WorkerConfig.Columns"/>
    /// that holds the source value. Lives here so both strategies share
    /// one implementation.
    /// </summary>
    public static int[] BuildBindOrderToSourceIndex(
        IReadOnlyList<string> bindOrder,
        IReadOnlyList<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> sourceColumns)
    {
        var sourceIndexByName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < sourceColumns.Count; i++)
            sourceIndexByName[sourceColumns[i].Name] = i;
        var map = new int[bindOrder.Count];
        for (int i = 0; i < bindOrder.Count; i++)
            map[i] = sourceIndexByName[bindOrder[i]];
        return map;
    }
}

