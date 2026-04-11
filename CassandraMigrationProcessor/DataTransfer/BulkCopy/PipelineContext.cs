using CassandraMigrationProcessor.Models;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Channels;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy
{
    internal record WorkerConfig(
        ConnectionOptions SourceConnection,
        ConnectionOptions TargetConnection,
        List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> Columns,
        TableContext Context);

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
    }

    /// <summary>
    /// Shared state passed to each worker.
    /// </summary>
    internal record PipelineContext(
        Channel<Partition> PartitionPool,
        WorkerConfig Worker,
        RangeState Ranges,
        PipelineCounters Counters,
        CopyProgressTracker Tracker);
}
