using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Helpers.JobManagement;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Processors
{
    internal partial class CopyProcessor
    {
        /// <summary>
        /// State of a feed range — its token and paging position.
        /// </summary>
        private record FeedRangeState(
            string FeedRange,
            byte[]? PagingState);

        private record ReadPage(
            List<object[]> Rows,
            string FeedRange,
            bool IsLastPage,
            long ReadTimeMs);

        private static string TruncRange(string r) =>
            r.Length > 30 ? r[..15] + "..." : r;

        private static bool IsRetriableWriteError(Exception ex)
        {
            var msg = ex.Message ?? string.Empty;
            var typeName = ex.GetType().Name;
            // Transient: throttling
            if (msg.Contains("429")
                || msg.Contains("TooManyRequests",
                    StringComparison.OrdinalIgnoreCase)
                || msg.Contains("rate",
                    StringComparison.OrdinalIgnoreCase))
                return true;
            // Transient: timeout
            if (ex is TimeoutException
                || ex is System.IO.IOException
                || msg.Contains("timeout",
                    StringComparison.OrdinalIgnoreCase)
                || typeName.Contains("Timeout"))
                return true;
            // Transient: network/connection
            if (ex is System.Net.Sockets.SocketException
                || typeName.Contains("NoHostAvailable")
                || typeName.Contains("BusyPool")
                || msg.Contains("connection",
                    StringComparison.OrdinalIgnoreCase)
                || msg.Contains("All hosts tried",
                    StringComparison.OrdinalIgnoreCase))
                return true;
            // Non-retriable: auth, schema, syntax errors
            return false;
        }

        /// <summary>
        /// Bundles the shared mutable state that is passed between
        /// <see cref="CopyWithFeedRangesAsync"/>, reader workers,
        /// and writer workers. Fields that are updated concurrently
        /// (e.g. TotalRead, TotalWritten) must be accessed with
        /// <see cref="Interlocked"/> or <see cref="Volatile"/>.
        /// </summary>
        private class PipelineContext
        {
            public Channel<FeedRangeState> WorkCh = null!;
            public Channel<ReadPage> DataCh = null!;
            public SemaphoreSlim WriteSem = null!;
            public PreparedStatement Ps = null!;
            public List<string> ColNames = null!;
            public HashSet<string> Completed = null!;
            public Dictionary<string, string?> Checkpoints = null!;
            public List<string> FeedRanges = null!;
            public CopyProgressTracker Tracker = null!;
            public long TotalRead;
            public long TotalWritten;
            public long TotalFailed;
            public int NonRetriableHitFlag;
            public ConcurrentBag<TaskResult> WorkerErrors = null!;
            public int ConfiguredPageSize;
            public int MaxInFlight;
            public ProcessorContext Ctx = null!;
            public MigrationUnit Mu = null!;
            public int ChunkIndex;
            public double InitialPercent;
            public double ContributionFactor;
            public long TotalRowCount;
            public long LastCheckpointTicks;
        }
    }
}
