using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Partition lifecycle phase. <c>Bulk</c> drains the initial snapshot
/// via COSMOS_CHANGEFEED_FROM_START(). <c>Replay</c> tails post-drain
/// for ongoing changes using the same query plus the last paging state
/// returned by Cosmos (online jobs only). <c>Done</c> is the terminal
/// state set by <see cref="Partition.CompleteOffline"/> (offline jobs)
/// — completed partitions are not recycled back into the pool. Phase
/// only advances; it never moves backward.
/// </summary>
public enum PartitionPhase { Bulk, Replay, Done }

/// <summary>
/// Represents a feed range partition with its in-flight Snapshot
/// list. Uses LinkedList for clean node management. The partition
/// owns its persisted Snapshot state via <see cref="Snapshot"/>
/// so workers Snapshot through the partition directly (no
/// round-trip to TableMigration's dicts).
/// </summary>
public sealed class Partition
{
    public string FeedRange => Snapshot.FeedRange;
    public byte[]? LastPagingState { get; private set; }
    public PartitionPhase Phase { get; private set; }

    /// <summary>
    /// Persisted per-feed-range Snapshot — bulk + replay state
    /// and bulk-completed flag. Lives on
    /// <see cref="TableMigration.Partitions"/> and is mutated
    /// through this reference by the owning worker.
    /// </summary>
    public PartitionSnapshot Snapshot { get; }

    /// <summary>
    /// Per-table resources shared by every partition belonging to the
    /// same table. Workers reach table-wide state (spec, columns,
    /// tracker, counters) through <c>partition.Table.X</c>.
    /// </summary>
    public TableResources Table { get; }

    // ── Read-retry budget ───────────────────────────────────────

    /// <summary>
    /// Count of consecutive cycle-level read-retry exhaustions on this
    /// partition (i.e. PageReader returned <c>null</c> from
    /// <see cref="PageReader.ReadAsync"/>). Reset to 0 on any successful
    /// read. <see cref="DataCopyWorker"/> uses this with
    /// <see cref="MaxConsecutiveReadRetryExhaustions"/> to convert a
    /// stuck-in-throttle partition into a job-wide fatal — transient
    /// back-pressure stays table-local, but a partition that never
    /// recovers must surface as a real failure.
    /// </summary>
    public int ConsecutiveReadRetryExhaustions { get; private set; }

    /// <summary>
    /// Upper bound on cycle-level read-retry exhaustions before we
    /// stop re-queueing the partition and trip the job fatal. Kept
    /// generous (5) so a real throttle storm gets several cool-off
    /// cycles before we give up.
    /// </summary>
    public const int MaxConsecutiveReadRetryExhaustions = 5;

    public int RecordReadRetryExhaustion()
    {
        ConsecutiveReadRetryExhaustions++;
        return ConsecutiveReadRetryExhaustions;
    }

    public void ResetReadRetryExhaustions()
    {
        if (ConsecutiveReadRetryExhaustions != 0)
            ConsecutiveReadRetryExhaustions = 0;
    }

    // ── Chunk tracking ──────────────────────────────────────────

    private readonly LinkedList<WorkChunk> _chunks = new();
    private readonly object _chunksLock = new();

    public Partition(PartitionSnapshot Snapshot, byte[]? initialPagingState, TableResources table, PartitionPhase phase = PartitionPhase.Bulk)
    {
        Snapshot = Snapshot ?? throw new ArgumentNullException(nameof(Snapshot));
        LastPagingState = initialPagingState;
        Phase = phase;
        Table = table;

        if (initialPagingState != null)
            _chunks.AddLast(new WorkChunk { ContinuationToken = initialPagingState, IsCompleted = true });
    }

    /// <summary>
    /// Update <see cref="LastPagingState"/> from a freshly-returned
    /// page. Null/empty tokens are no-ops so a tip-of-stream empty
    /// response does not clobber a valid prior anchor.
    /// </summary>
    public void SetLastPagingState(byte[]? pagingState)
    {
        if (pagingState is not { Length: > 0 }) return;
        lock (_chunksLock)
        {
            LastPagingState = pagingState;
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
        lock (_chunksLock)
        {
            Phase = PartitionPhase.Replay;
        }
    }

    public WorkChunk AddChunkAndTrim(byte[]? continuationToken)
    {
        var chunk = new WorkChunk { ContinuationToken = continuationToken };
        lock (_chunksLock)
        {
            while (_chunks.First != null && _chunks.First.Value.IsCompleted)
                _chunks.RemoveFirst();

            _chunks.AddLast(chunk);
        }
        return chunk;
    }

    public byte[]? GetResumeToken()
    {
        lock (_chunksLock)
        {
            foreach (var chunk in _chunks)
            {
                if (!chunk.IsCompleted) return chunk.ContinuationToken;
            }
            return _chunks.Last?.Value.ContinuationToken;
        }
    }

    // ── Snapshot API ─────────────────────────────────────────
    // Workers Snapshot through these methods. The dict on
    // TableMigration is mutated in-place via the shared Snapshot
    // reference — no round-trip through the owning table.

    /// <summary>Persist the paging state for this range. Same field
    /// services bulk and replay because a partition is in exactly
    /// one phase at a time. Null/empty tokens are no-ops so a
    /// tip-of-stream empty response does not clobber a valid prior
    /// anchor with a missing one — symmetric with
    /// <see cref="SetLastPagingState"/>.</summary>
    public void SaveCheckpoint(byte[]? token)
    {
        if (token is not { Length: > 0 }) return;
        Snapshot.ContinuationToken = Convert.ToBase64String(token);
    }

    /// <summary>
    /// Online bulk → replay handoff. Sets <see cref="PartitionSnapshot.BulkCompleted"/>
    /// while preserving the current <see cref="PartitionSnapshot.ContinuationToken"/>
    /// as the replay anchor. On the first writer the partition notifies
    /// <see cref="Table"/> so the table-level counter and drain
    /// signal advance — workers never touch table-level state directly.
    /// </summary>
    public void HandoffToReplay()
    {
        if (Snapshot.BulkCompleted) return;
        Snapshot.BulkCompleted = true;
        Table.OnPartitionBulkCompleted();
    }

    /// <summary>
    /// Offline final completion. Sets <see cref="PartitionSnapshot.BulkCompleted"/>,
    /// clears <see cref="PartitionSnapshot.ContinuationToken"/> so resume
    /// skips the range entirely, and advances <see cref="Phase"/> to
    /// <see cref="PartitionPhase.Done"/>. Notifies <see cref="Table"/>
    /// on the first writer.
    /// </summary>
    public void CompleteOffline()
    {
        if (Snapshot.BulkCompleted) return;
        Snapshot.BulkCompleted = true;
        Snapshot.ContinuationToken = null;
        lock (_chunksLock)
        {
            Phase = PartitionPhase.Done;
        }
        Table.OnPartitionBulkCompleted();
    }
}
