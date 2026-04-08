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

            // Linked list of work chunks (head = oldest pending)
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
                    // Trim completed chunks from head
                    while (_head != null && _head.IsCompleted)
                        _head = _head.Next;

                    // Append new chunk
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
