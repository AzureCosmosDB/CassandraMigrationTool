using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Channels;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Per-job worker configuration. Carries the <see cref="Job"/> so worker
/// sessions are built via the Job-aware factory overloads — that path
/// lazily fetches AAD tokens / ARM credentials and caches them onto the
/// job before any session is opened. Snapshotting <c>ConnectionOptions</c>
/// here would skip that step and crash workers with a null-password
/// error when source AAD is enabled.
/// </summary>
internal record WorkerConfig(
    Job Job,
    TokenRefreshManager? TokenRefreshManager,
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
    public Job Job => Worker.Job;
    public bool EnableReplay => Worker.EnableReplay;
}
