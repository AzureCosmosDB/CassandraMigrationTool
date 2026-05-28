using System;
using System.Collections.Generic;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Partition lifecycle phase. Bulk = draining the initial snapshot
/// via COSMOS_CHANGEFEED_FROM_START(); Replay = tailing post-drain
/// for ongoing changes using the same query + the last paging state
/// returned by Cosmos. Phase only flips Bulk → Replay, never back.
/// </summary>
internal enum PartitionPhase { Bulk, Replay }

/// <summary>
/// Represents a feed range partition with its in-flight checkpoint
/// list. Uses LinkedList for clean node management. The partition
/// owns its persisted checkpoint state via <see cref="State"/> so
/// workers checkpoint through the partition directly (no round-trip
/// to TableMigration's dicts).
/// </summary>
internal class Partition
{
    public string FeedRange => State.FeedRange;
    public bool IsExhausted { get; private set; }
    public byte[]? LastPagingState { get; private set; }
    public PartitionPhase Phase { get; private set; }

    /// <summary>
    /// Persisted per-feed-range state — bulk + replay checkpoints
    /// and bulk-completed flag. Lives on
    /// <see cref="TableMigration.Partitions"/> and is mutated
    /// through this reference by the owning worker.
    /// </summary>
    public PartitionState State { get; }

    /// <summary>
    /// Per-table state for the table that owns this partition. Workers
    /// resolve all per-table data (Tracker, identifiers, columns,
    /// drain signal) through this reference so a single shared worker
    /// pool can service partitions from any table.
    /// </summary>
    public TableResources Resources { get; }

    private readonly LinkedList<WorkChunk> _chunks = new();
    private readonly object _lock = new();

    public Partition(PartitionState state, byte[]? initialPagingState, TableResources resources, PartitionPhase phase = PartitionPhase.Bulk)
    {
        State = state ?? throw new ArgumentNullException(nameof(state));
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

    // ── Checkpoint API ─────────────────────────────────────────
    // Workers checkpoint through these methods. The dict on
    // TableMigration is mutated in-place via the shared State
    // reference — no MigrationUnit dict round-trip.

    /// <summary>Persist the paging state for this range. Same field
    /// services bulk and replay because a partition is in exactly
    /// one phase at a time.</summary>
    public void SaveCheckpoint(byte[]? token)
    {
        if (token == null) return;
        State.ContinuationToken = Convert.ToBase64String(token);
    }

    /// <summary>
    /// Online bulk → replay handoff. Sets <see cref="PartitionState.BulkCompleted"/>
    /// while preserving the current <see cref="PartitionState.ContinuationToken"/>
    /// as the replay anchor. On the first writer the partition notifies
    /// <see cref="Resources"/> so the table-level counter and drain
    /// signal advance — workers never touch table-level state directly.
    /// </summary>
    public void HandoffToReplay()
    {
        if (State.BulkCompleted) return;
        State.BulkCompleted = true;
        Resources.OnPartitionBulkCompleted();
    }

    /// <summary>
    /// Offline final completion. Sets <see cref="PartitionState.BulkCompleted"/>
    /// and clears <see cref="PartitionState.ContinuationToken"/> so resume
    /// skips the range entirely. On the first writer the partition
    /// notifies <see cref="Resources"/>.
    /// </summary>
    public void CompleteOffline()
    {
        if (State.BulkCompleted) return;
        State.BulkCompleted = true;
        State.ContinuationToken = null;
        Resources.OnPartitionBulkCompleted();
    }
}
