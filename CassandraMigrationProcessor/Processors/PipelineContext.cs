using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Channels;

namespace CassandraMigrationProcessor.Processors
{
    internal class WorkerConfig
    {
        public ConnectionOptions SourceConnection = null!;
        public ConnectionOptions TargetConnection = null!;
        public List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> Columns = null!;
        public ProcessorContext Context = null!;
    }

    internal class RangeState
    {
        public HashSet<string> Completed = null!;
        public Dictionary<string, string?> Checkpoints = null!;
        public List<string> FeedRanges = null!;
    }

    /// <summary>
    /// Non-progress pipeline flags. Row counters now live in
    /// <see cref="CopyProgressTracker"/> (single source of truth).
    /// </summary>
    internal class PipelineCounters
    {
        public int FatalErrorFlag;
        public ConcurrentBag<TaskResult> WorkerErrors = null!;
    }

    internal class ProgressState
    {
        public CopyProgressTracker Tracker = null!;
    }

    /// <summary>
    /// Shared mutable state passed to each worker.
    /// </summary>
    internal class PipelineContext
    {
        public Channel<Partition> PartitionPool = null!;
        public WorkerConfig Worker = null!;
        public RangeState Ranges = null!;
        public PipelineCounters Counters = null!;
        public ProgressState Progress = null!;
    }
}
