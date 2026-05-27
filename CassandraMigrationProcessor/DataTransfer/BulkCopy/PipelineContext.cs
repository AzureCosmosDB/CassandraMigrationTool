using CassandraMigrationProcessor.Models;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;
internal record WorkerConfig(
    ConnectionOptions SourceConnection,
    ConnectionOptions TargetConnection,
    List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> Columns,
    TableContext Context,
    bool EnableReplay,
    int ReplayCooldownMs);

internal record RangeState(
    HashSet<string> Completed,
    Dictionary<string, string?> Checkpoints,
    List<string> FeedRanges);

/// <summary>
/// Non-progress pipeline flags. Row counters live in
/// <see cref="CopyProgressTracker"/> (single source of truth).
/// Kept as class: FatalErrorFlag needs Interlocked/ref access.
/// </summary>
internal class PipelineCounters
{
    public int FatalErrorFlag;
    public ConcurrentBag<TaskResult> WorkerErrors { get; } = new();

    /// <summary>
    /// Count of partitions that have transitioned Bulk → Replay (online
    /// mode only). Once this equals the total feed range count, the
    /// bulk-copy phase is logically complete and
    /// <see cref="BulkDrainSignal"/> is tripped so the caller can mark
    /// the table CopyComplete and continue, while workers stay alive
    /// tailing the change feed.
    /// </summary>
    public int BulkPhaseDrainedCount;

    /// <summary>
    /// Tripped when every partition has either drained-to-Replay (online)
    /// or completed (offline). For online jobs, WorkerExecutor awaits
    /// this signal instead of pool completion so the table can be marked
    /// CopyComplete while the worker pool continues tailing the change
    /// feed indefinitely.
    /// </summary>
    public TaskCompletionSource BulkDrainSignal { get; } =
        new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
}

public record ProgressConfig(
    int ChunkIndex,
    double InitialPercent,
    double ContributionFactor,
    long TotalRowCount);

/// <summary>
/// Shared state passed to each worker.
/// </summary>
internal record PipelineContext(
    Channel<Partition> PartitionPool,
    WorkerConfig Worker,
    RangeState Ranges,
    PipelineCounters Counters,
    CopyProgressTracker Tracker)
{
    // Convenience accessors to reduce Law of Demeter violations
    public string KeyspaceName => Worker.Context.KeyspaceName;
    public string TableName => Worker.Context.TableName;
    public string TargetKeyspaceName => Worker.Context.TargetKeyspaceName;
    public string TargetTableName => Worker.Context.TargetTableName;
    public ConnectionOptions SourceConnection => Worker.SourceConnection;
    public ConnectionOptions TargetConnection => Worker.TargetConnection;
    public bool EnableReplay => Worker.EnableReplay;
}
