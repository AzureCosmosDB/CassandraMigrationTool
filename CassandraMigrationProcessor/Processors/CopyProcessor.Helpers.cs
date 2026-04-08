using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;

namespace CassandraMigrationProcessor.Processors
{
    internal partial class CopyProcessor
    {
        private const int ReadTimeoutMs = 60_000;
        private const int MaxReadRetries = 3;
        private const int RetryDelayMs = 5000;

        private static string TruncRange(string r) =>
            r.Length > 30 ? r[..15] + "..." : r;

        private static bool IsRetriableWriteError(Exception ex)
        {
            var msg = ex.Message ?? string.Empty;
            var typeName = ex.GetType().Name;
            if (msg.Contains("429")
                || msg.Contains("TooManyRequests",
                    StringComparison.OrdinalIgnoreCase)
                || msg.Contains("rate",
                    StringComparison.OrdinalIgnoreCase))
                return true;
            if (ex is TimeoutException
                || ex is System.IO.IOException
                || msg.Contains("timeout",
                    StringComparison.OrdinalIgnoreCase)
                || typeName.Contains("Timeout"))
                return true;
            if (ex is System.Net.Sockets.SocketException
                || typeName.Contains("NoHostAvailable")
                || typeName.Contains("BusyPool")
                || msg.Contains("connection",
                    StringComparison.OrdinalIgnoreCase)
                || msg.Contains("All hosts tried",
                    StringComparison.OrdinalIgnoreCase))
                return true;
            return false;
        }

        /// <summary>
        /// Errors that should immediately fail the entire job
        /// (not just the current table). These indicate config
        /// or permissions problems that won't self-resolve.
        /// </summary>
        private static bool IsFatalError(Exception ex)
        {
            var msg = ex.Message ?? string.Empty;
            var typeName = ex.GetType().Name;
            if (typeName.Contains("Authentication")
                || typeName.Contains("Unauthorized")
                || msg.Contains("authentication",
                    StringComparison.OrdinalIgnoreCase)
                || msg.Contains("credentials",
                    StringComparison.OrdinalIgnoreCase))
                return true;
            if (typeName.Contains("InvalidQuery")
                || typeName.Contains("SyntaxError")
                || msg.Contains("unconfigured table",
                    StringComparison.OrdinalIgnoreCase)
                || msg.Contains("Unknown identifier",
                    StringComparison.OrdinalIgnoreCase))
                return true;
            return false;
        }

        /// <summary>
        /// Tracks a pending or completed read-write cycle.
        /// Forms a linked list per partition.
        /// </summary>
        private class WorkChunk
        {
            public byte[]? ContinuationToken { get; set; }
            public bool IsCompleted { get; set; }
            public WorkChunk? Next { get; set; }
        }

        /// <summary>
        /// Represents a feed range partition with its work
        /// chunk list. Passed through the partition pool channel.
        /// </summary>
        private class Partition
        {
            public string FeedRange { get; }
            public bool IsExhausted { get; set; }

            /// <summary>
            /// Latest paging state from the most recent read.
            /// Used by the next worker to continue reading.
            /// NOT the same as GetResumeToken() which returns
            /// the oldest unwritten checkpoint.
            /// </summary>
            public byte[]? LastPagingState { get; set; }

            private WorkChunk? _head;
            private WorkChunk? _tail;
            private readonly object _lock = new();

            public Partition(
                string feedRange, byte[]? initialPagingState)
            {
                FeedRange = feedRange;
                LastPagingState = initialPagingState;
                if (initialPagingState != null)
                {
                    _head = _tail = new WorkChunk
                    {
                        ContinuationToken = initialPagingState,
                        IsCompleted = true
                    };
                }
            }

            /// <summary>
            /// Add a new pending work chunk and trim completed
            /// chunks from the head. Returns the new chunk so
            /// the caller can mark it completed after writing.
            /// </summary>
            public WorkChunk AddChunkAndTrim(
                byte[]? continuationToken)
            {
                var chunk = new WorkChunk
                {
                    ContinuationToken = continuationToken
                };
                lock (_lock)
                {
                    while (_head != null && _head.IsCompleted)
                        _head = _head.Next;

                    if (_tail == null)
                        _head = _tail = chunk;
                    else
                    {
                        _tail.Next = chunk;
                        _tail = chunk;
                    }
                }
                return chunk;
            }

            /// <summary>
            /// Get the resume point: continuation token of the
            /// first non-completed chunk, or the last completed
            /// chunk's token if all are done.
            /// </summary>
            public byte[]? GetResumeToken()
            {
                lock (_lock)
                {
                    var node = _head;
                    while (node != null)
                    {
                        if (!node.IsCompleted)
                            return node.ContinuationToken;
                        node = node.Next;
                    }
                    return _tail?.ContinuationToken;
                }
            }

            /// <summary>
            /// Count of pending (non-completed) work chunks.
            /// </summary>
            public int PendingCount
            {
                get
                {
                    lock (_lock)
                    {
                        int count = 0;
                        var node = _head;
                        while (node != null)
                        {
                            if (!node.IsCompleted) count++;
                            node = node.Next;
                        }
                        return count;
                    }
                }
            }
        }

        /// <summary>
        /// Bundles the shared mutable state that is passed to
        /// each unified worker. Fields that are updated
        /// concurrently (e.g. TotalRead, TotalWritten) must
        /// be accessed with <see cref="Interlocked"/> or
        /// <see cref="Volatile"/>.
        /// </summary>
        private class PipelineContext
        {
            public Channel<Partition> PartitionPool = null!;
            public List<string> ColumnNames = null!;
            public List<(string Name, string Type,
                string Kind, string ClusteringOrder,
                int Position)> Columns = null!;
            public HashSet<string> Completed = null!;
            public Dictionary<string, string?> Checkpoints = null!;
            public List<string> FeedRanges = null!;
            public CopyProgressTracker Tracker = null!;
            public long TotalRead;
            public long TotalWritten;
            public long TotalFailed;
            public int FatalErrorFlag;
            public ConcurrentBag<TaskResult> WorkerErrors = null!;
            public int ConfiguredPageSize;
            public ProcessorContext Context = null!;
            public MigrationUnit MigrationUnit = null!;
            public MigrationJob Job = null!;
            public int ChunkIndex;
            public double InitialPercent;
            public double ContributionFactor;
            public long TotalRowCount;
            public long LastCheckpointTicks;
        }
    }
}
