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

    /// <summary>
    /// True when the source table has at least one counter column.
    /// Cached at construction — Cassandra forbids mixing counter and
    /// non-counter regular columns in the same table, so this is a
    /// fixed property of the schema and computing it per write would
    /// be wasted work.
    /// </summary>
    public readonly bool IsCounterTable;

    /// <summary>Total number of feed ranges for this table.</summary>
    public int TotalFeedRanges { get; }

    /// <summary>
    /// Number of partitions for this table whose bulk copy has
    /// fully finished — both online drain → replay and offline
    /// final completion count. Maintained as an atomic counter
    /// updated only via <see cref="OnPartitionBulkCompleted"/>
    /// so the hot-path read in <see cref="PageWriter"/> stays O(1).
    /// </summary>
    private int _bulkCompletedCount;
    public int BulkCompletedCount => Volatile.Read(ref _bulkCompletedCount);

    /// <summary>
    /// Tripped when every partition for THIS table has either drained
    /// to Replay (online) or completed (offline). Per-table because
    /// each table has its own drain handoff moment.
    /// </summary>
    public TaskCompletionSource BulkDrainSignal { get; } =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public string FullTableName => $"{Spec.KeyspaceName}.{Spec.TableName}";

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
        IsCounterTable = CassandraQueries.IsCounterTable(columns);
    }

    /// <summary>
    /// Notification raised by a <see cref="Partition"/> when its
    /// bulk phase has just finished (online handoff or offline
    /// final). Atomic increment + signal trip — invoked by the
    /// partition itself, NEVER by workers directly. This is the
    /// single point where per-partition state is rolled up into
    /// the table-wide drain signal.
    /// </summary>
    public void OnPartitionBulkCompleted()
    {
        int n = Interlocked.Increment(ref _bulkCompletedCount);
        if (n >= TotalFeedRanges)
            BulkDrainSignal.TrySetResult();
    }
}
