using CassandraMigrationProcessor.Models;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Channels;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Per-job worker configuration. Table identifiers, columns, and
/// per-table state live on <see cref="TableResources"/> attached to each
/// <see cref="Partition"/> so a single shared worker pool can service
/// partitions from any table.
/// </summary>
internal record WorkerConfig(
    ConnectionOptions SourceConnection,
    ConnectionOptions TargetConnection,
    bool EnableReplay,
    int ReplayCooldownMs);

/// <summary>Per-table feed-range bookkeeping owned by the table's
/// <see cref="TableResources"/>.</summary>
internal record RangeState(
    HashSet<string> Completed,
    Dictionary<string, string?> Checkpoints,
    List<string> FeedRanges);

/// <summary>
/// Job-level pipeline flags. Per-table progress lives on
/// <see cref="TableResources.Tracker"/>.
/// </summary>
internal class PipelineCounters
{
    public int FatalErrorFlag;
    public ConcurrentBag<TaskResult> WorkerErrors { get; } = new();
}

public record ProgressConfig(
    int ChunkIndex,
    double InitialPercent,
    double ContributionFactor,
    long TotalRowCount);

/// <summary>
/// Shared (job-wide) state passed to every worker. Holds the single
/// partition channel that all tables seed into, the connection/replay
/// configuration, and global counters. Per-table state is resolved via
/// <see cref="Partition.Resources"/>.
/// </summary>
internal record PipelineContext(
    Channel<Partition> PartitionPool,
    WorkerConfig Worker,
    PipelineCounters Counters)
{
    public ConnectionOptions SourceConnection => Worker.SourceConnection;
    public ConnectionOptions TargetConnection => Worker.TargetConnection;
    public bool EnableReplay => Worker.EnableReplay;
}
