using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Single entry point for building the right <see cref="IRowWriteStrategy"/>
/// for a target table. Owns the shared per-worker write setup:
/// <list type="bullet">
///   <item>prepares the INSERT/UPDATE statement via
///         <see cref="CassandraQueries.PrepareInsertAsync"/> (which also
///         tells us whether the table is a counter table);</item>
///   <item>builds the bind-order → source-index map shared by every
///         strategy;</item>
///   <item>routes to <see cref="RegularRowWriteStrategy"/> for plain
///         tables and <see cref="CounterRowWriteStrategy"/> for counter
///         tables, the latter doing additional async prep (the
///         read-modify-write SELECT-by-PK).</item>
/// </list>
/// PageWriter consumes only the resulting <see cref="IRowWriteStrategy"/>
/// and no longer knows anything about prepared statements or counter
/// detection.
/// </summary>
internal static class RowWriteStrategyFactory
{
    public static async Task<IRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession, WorkerConfig config, int maxWriteRetries)
    {
        var (ps, bindOrder, isCounterTable, counterColumns) = await CassandraQueries.PrepareInsertAsync(
            targetSession, config.Context.TargetKeyspaceName, config.Context.TargetTableName, config.Columns);

        var sourceIndexByName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < config.Columns.Count; i++)
            sourceIndexByName[config.Columns[i].Name] = i;
        var bindOrderToSourceIndex = new int[bindOrder.Count];
        for (int i = 0; i < bindOrder.Count; i++)
            bindOrderToSourceIndex[i] = sourceIndexByName[bindOrder[i]];

        // One policy per worker, shared across rows and across strategy
        // variants. Linear 500ms × attempt matches the historical schedule.
        var retryPolicy = RetryPolicy.Linear(maxWriteRetries, TimeSpan.FromMilliseconds(500));

        if (isCounterTable)
        {
            return await CounterRowWriteStrategy.CreateAsync(log, targetSession, ps, bindOrderToSourceIndex,
                bindOrder, config.Context.TargetKeyspaceName, config.Context.TargetTableName,
                counterColumns, retryPolicy);
        }

        return new RegularRowWriteStrategy(log, targetSession, ps, bindOrderToSourceIndex,
            retryPolicy);
    }
}
