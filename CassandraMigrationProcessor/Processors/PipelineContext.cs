using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Channels;

namespace CassandraMigrationProcessor.Processors
{
    /// <summary>
    /// Progress-reporting parameters, constant for the lifetime of a pipeline run.
    /// </summary>
    internal class ProgressConfig
    {
        public double InitialPercent;
        public double ContributionFactor;
        public long TotalRowCount;
        public int ChunkIndex;
    }

    /// <summary>
    /// Shared mutable state passed to each worker. Concurrent fields
    /// (TotalRead, TotalWritten, etc.) use Interlocked/Volatile.
    /// </summary>
    internal class PipelineContext
    {
        public Channel<Partition> PartitionPool = null!;
        public List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> Columns = null!;
        public ConnectionOptions SourceConnection = null!;
        public ConnectionOptions TargetConnection = null!;
        public HashSet<string> Completed = null!;
        public Dictionary<string, string?> Checkpoints = null!;
        public List<string> FeedRanges = null!;
        public CopyProgressTracker Tracker = null!;
        public long TotalRead;
        public long TotalWritten;
        public long TotalFailed;
        public int FatalErrorFlag;
        public ConcurrentBag<TaskResult> WorkerErrors = null!;
        public ProcessorContext Context = null!;
        public MigrationUnit MigrationUnit = null!;
        public ProgressConfig Progress = null!;
        public long LastCheckpointTicks;
    }
}
