using System.Collections.Generic;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Partition lifecycle phase. Bulk = draining the initial snapshot
/// via COSMOS_CHANGEFEED_FROM_START(); Replay = tailing post-drain
/// for ongoing changes using the same query + the last paging state
/// returned by Cosmos. Phase only flips Bulk → Replay, never back.
/// </summary>
internal enum PartitionPhase { Bulk, Replay }

/// <summary>
/// Represents a feed range partition with its work chunk list.
/// Uses LinkedList for clean node management.
/// </summary>
internal class Partition
{
    public string FeedRange { get; }
    public bool IsExhausted { get; private set; }
    public byte[]? LastPagingState { get; private set; }
    public PartitionPhase Phase { get; private set; }
    /// <summary>
    /// Per-table state for the table that owns this partition. Workers
    /// resolve all per-table data (Tracker, Ranges, identifiers, columns,
    /// drain signal) through this reference so a single shared worker
    /// pool can service partitions from any table.
    /// </summary>
    public TableResources Resources { get; }

    private readonly LinkedList<WorkChunk> _chunks = new();
    private readonly object _lock = new();

    public Partition(string feedRange, byte[]? initialPagingState, TableResources resources, PartitionPhase phase = PartitionPhase.Bulk)
    {
        FeedRange = feedRange;
        LastPagingState = initialPagingState;
        Phase = phase;
        Resources = resources;

        if (initialPagingState != null)
            _chunks.AddLast(new WorkChunk { ContinuationToken = initialPagingState, IsCompleted = true });
    }

    public void SetPageState(byte[]? pagingState, bool isExhausted)
    {
        lock (_lock)
        {
            LastPagingState = pagingState;
            if (isExhausted) IsExhausted = true;
        }
    }

    /// <summary>
    /// Transitions the partition from Bulk to Replay phase. Called by
    /// the worker on the first empty page after the snapshot drains.
    /// LastPagingState is preserved — it is the handoff anchor that
    /// replay polls forward from.
    /// </summary>
    public void TransitionToReplay()
    {
        lock (_lock)
        {
            Phase = PartitionPhase.Replay;
        }
    }

    public WorkChunk AddChunkAndTrim(byte[]? continuationToken)
    {
        var chunk = new WorkChunk { ContinuationToken = continuationToken };
        lock (_lock)
        {
            // Trim completed chunks from the front
            while (_chunks.First != null && _chunks.First.Value.IsCompleted)
                _chunks.RemoveFirst();

            _chunks.AddLast(chunk);
        }
        return chunk;
    }

    public byte[]? GetResumeToken()
    {
        lock (_lock)
        {
            foreach (var chunk in _chunks)
            {
                if (!chunk.IsCompleted) return chunk.ContinuationToken;
            }
            return _chunks.Last?.Value.ContinuationToken;
        }
    }
}
