using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Channels;

namespace CassandraMigrationProcessor.Processors
{
    internal class WorkerConfig
    {
        public ConnectionOptions SourceConnection { get; }
        public ConnectionOptions TargetConnection { get; }
        public List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> Columns { get; }
        public ProcessorContext Context { get; }

        public WorkerConfig(ConnectionOptions source, ConnectionOptions target,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> columns,
            ProcessorContext context)
        {
            SourceConnection = source;
            TargetConnection = target;
            Columns = columns;
            Context = context;
        }
    }

    internal class RangeState
    {
        public HashSet<string> Completed { get; }
        public Dictionary<string, string?> Checkpoints { get; }
        public List<string> FeedRanges { get; }

        public RangeState(HashSet<string> completed,
            Dictionary<string, string?> checkpoints,
            List<string> feedRanges)
        {
            Completed = completed;
            Checkpoints = checkpoints;
            FeedRanges = feedRanges;
        }
    }

    /// <summary>
    /// Non-progress pipeline flags. Row counters live in
    /// <see cref="CopyProgressTracker"/> (single source of truth).
    /// </summary>
    internal class PipelineCounters
    {
        public int FatalErrorFlag;
        public ConcurrentBag<TaskResult> WorkerErrors { get; } = new();
    }

    /// <summary>
    /// Shared mutable state passed to each worker.
    /// </summary>
    internal class PipelineContext
    {
        public Channel<Partition> PartitionPool { get; }
        public WorkerConfig Worker { get; }
        public RangeState Ranges { get; }
        public PipelineCounters Counters { get; }
        public CopyProgressTracker Tracker { get; }

        public PipelineContext(Channel<Partition> partitionPool,
            WorkerConfig worker, RangeState ranges,
            PipelineCounters counters, CopyProgressTracker tracker)
        {
            PartitionPool = partitionPool;
            Worker = worker;
            Ranges = ranges;
            Counters = counters;
            Tracker = tracker;
        }
    }
}
