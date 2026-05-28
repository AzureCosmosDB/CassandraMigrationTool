using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Models;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CassandraMigrationProcessor.CassandraDriver;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Per-table state carried on each <see cref="Partition"/> so that a
/// shared (job-wide) worker pool can process partitions from any table
/// without re-binding workers to a single table at construction.
/// Created once per table by <see cref="WorkerExecutor"/>.
///
/// Per-feed-range checkpoint state lives on each
/// <see cref="Partition.State"/> (and is persisted via
/// <see cref="TableMigration.Partitions"/>) — TableResources only
/// tracks the table-wide totals workers need on hot paths.
/// </summary>
internal sealed class TableResources
{
    public TableCopySpec Spec { get; }
    public List<CassandraColumn> Columns { get; }
    public CopyProgressTracker Tracker { get; }

    /// <summary>Total number of feed ranges for this table.</summary>
    public int TotalFeedRanges { get; }

    /// <summary>
    /// Number of partitions for this table that have transitioned
    /// Bulk → Replay (online) or completed (offline). Trips
    /// <see cref="BulkDrainSignal"/> when it reaches the total range count.
    /// Public field so workers can Interlocked.Increment it.
    /// </summary>
    public int BulkDrainedCount;

    /// <summary>
    /// Number of partitions for this table whose bulk copy has
    /// fully completed (offline final state, or bulk-drain in online
    /// mode). Maintained as an atomic counter by workers via
    /// <see cref="IncrementBulkCompleted"/> so the hot-path read in
    /// <see cref="PageWriter"/> stays O(1).
    /// </summary>
    private int _bulkCompletedCount;
    public int BulkCompletedCount => Volatile.Read(ref _bulkCompletedCount);

    /// <summary>
    /// Tripped when every partition for THIS table has either drained
    /// to Replay (online) or completed (offline). Per-table because
    /// each table has its own drain handoff moment.
    /// </summary>
    public TaskCompletionSource BulkDrainSignal { get; } =
        new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    public string TableId => $"{Spec.KeyspaceName}.{Spec.TableName}";

    public TableResources(
        TableCopySpec spec,
        List<CassandraColumn> columns,
        CopyProgressTracker tracker,
        int totalFeedRanges)
    {
        Spec = spec;
        Columns = columns;
        Tracker = tracker;
        TotalFeedRanges = totalFeedRanges;
    }

    /// <summary>
    /// Atomically increments the bulk-completed counter. Called by
    /// workers when a partition's bulk drain finishes.
    /// </summary>
    public int IncrementBulkCompleted()
        => Interlocked.Increment(ref _bulkCompletedCount);
}
