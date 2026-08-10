using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Single entry point for picking and constructing the right
/// <see cref="IRowWriteStrategy"/> for a target table. The factory is
/// pure dispatch: it builds the shared per-worker
/// <see cref="RetryPolicy"/> and routes to the variant whose factory
/// owns its own statement preparation:
/// <list type="bullet">
///   <item><see cref="RegularRowWriteStrategy.CreateAsync"/> prepares
///         the INSERT JSON (metadata-preserving path, UseJsonCopy=true);</item>
///   <item><see cref="TypedRegularRowWriteStrategy.CreateAsync"/> prepares
///         the typed VALUES INSERT (fast binary path, UseJsonCopy=false);</item>
///   <item><see cref="CounterRowWriteStrategy.CreateAsync"/> prepares
///         the UPDATE and the read-modify-write SELECT-by-PK.</item>
/// </list>
/// </summary>
internal static class RowWriteStrategyFactory
{
    /// <summary>
    /// Picks and constructs the right strategy for the table shape. Used
    /// by both bulk copy (<see cref="PageWriter"/>) and change-feed
    /// replay so both paths share identical per-row write semantics.
    /// </summary>
    public static async Task<IRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession,
        List<CassandraColumn> columns,
        string targetKeyspace, string targetTable, int maxWriteRetries,
        bool isCounterTable,
        ConsistencyLevel targetWriteConsistencyLevel,
        bool preserveCellTtl,
        bool useJsonCopy)
    {
        // Simulated run: target is a NullSession that cannot prepare.
        // Skip strategy preparation entirely and count rows as written.
        if (targetSession is NullSession)
            return new SimulatedRowWriteStrategy();

        // One policy per worker, shared across rows and across strategy
        // variants. Linear 500ms × attempt matches the historical schedule.
        var retryPolicy = RetryPolicy.Linear(maxWriteRetries, TimeSpan.FromMilliseconds(500));

        if (isCounterTable)
            // Counters cannot honour USING TIMESTAMP / USING TTL —
            // Cassandra forbids both on counter UPDATEs. The counter
            // strategy uses its own prepared UPDATE without the clause.
            return await CounterRowWriteStrategy.CreateAsync(log, targetSession, columns, targetKeyspace, targetTable, retryPolicy);

        if (!useJsonCopy)
            // Fast binary copy path: typed prepared INSERT bound from the
            // SELECT * row. No TTL/writetime preservation (validated
            // incompatible with per-cell preservation in PipelineConfig).
            return await TypedRegularRowWriteStrategy.CreateAsync(
                log, targetSession, columns, targetKeyspace, targetTable,
                retryPolicy, targetWriteConsistencyLevel);

        return await RegularRowWriteStrategy.CreateAsync(
            log, targetSession, columns, targetKeyspace, targetTable,
            retryPolicy, targetWriteConsistencyLevel, preserveCellTtl);
    }

    /// <summary>
    /// Builds the bind-order → source-index map used by every strategy:
    /// for each bind slot, the index into <see cref="TableResources.Columns"/>
    /// that holds the source value. Lives here so both strategies share
    /// one implementation.
    /// </summary>
    public static int[] BuildBindOrderToSourceIndex(
        IReadOnlyList<string> bindOrder,
        IReadOnlyList<CassandraColumn> sourceColumns)
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
