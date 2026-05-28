using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Models;
using System.Collections.Generic;
using System.Threading.Tasks;
using CassandraMigrationProcessor.CassandraDriver;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Per-table state carried on each <see cref="Partition"/> so that a
/// shared (job-wide) worker pool can process partitions from any table
/// without re-binding workers to a single table at construction.
/// Created once per table by <see cref="WorkerExecutor"/>.
/// </summary>
internal sealed class TableResources
{
    public TableCopySpec Spec { get; }
    public List<CassandraColumn> Columns { get; }
    public CopyProgressTracker Tracker { get; }
    public RangeState Ranges { get; }
    /// <summary>
    /// Number of partitions for this table that have transitioned
    /// Bulk → Replay (online) or completed (offline). Trips
    /// <see cref="BulkDrainSignal"/> when it reaches the total range count.
    /// Public field so workers can Interlocked.Increment it.
    /// </summary>
    public int BulkDrainedCount;
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
        RangeState ranges)
    {
        Spec = spec;
        Columns = columns;
        Tracker = tracker;
        Ranges = ranges;
    }
}
