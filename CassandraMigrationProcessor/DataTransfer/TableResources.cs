using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.CassandraDriver;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Per-table state carried on each <see cref="Partition"/> so a
/// shared (job-wide) worker pool can process partitions from any
/// table. Created once per (table, chunk) during the discovery phase.
/// Per-feed-range Snapshot state lives on each
/// <see cref="Partition.Snapshot"/>; TableResources only tracks the
/// table-wide totals workers need on hot paths.
/// </summary>
public sealed class TableResources
{
    public TableCopySpec Spec { get; }
    public List<CassandraColumn> Columns { get; }
    public CopyProgressTracker Tracker { get; }

    /// <summary>
    /// True when the source table has at least one counter column.
    /// Cassandra forbids mixing counter and non-counter regular
    /// columns, so this is a fixed schema property.
    /// </summary>
    public readonly bool IsCounterTable;

    public int TotalFeedRanges { get; }

    /// <summary>
    /// Count of partitions whose bulk copy has finished (online
    /// drain→replay and offline final both count). Atomic O(1) read.
    /// </summary>
    private int _bulkCompletedCount;
    public int BulkCompletedCount => Volatile.Read(ref _bulkCompletedCount);

    /// <summary>
    /// Tripped when every partition for THIS table has drained to
    /// Replay (online) or completed (offline).
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
    /// Raised by a <see cref="Partition"/> when its bulk phase has
    /// just finished. Atomic increment + signal trip. Invoked by the
    /// partition itself, never by workers directly.
    /// </summary>
    public void OnPartitionBulkCompleted()
    {
        int n = Interlocked.Increment(ref _bulkCompletedCount);
        if (n >= TotalFeedRanges)
            BulkDrainSignal.TrySetResult();
    }
}
