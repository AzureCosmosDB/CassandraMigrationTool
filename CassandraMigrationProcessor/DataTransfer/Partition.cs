using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using Newtonsoft.Json;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Partition lifecycle phase. Bulk = draining the initial snapshot
/// via COSMOS_CHANGEFEED_FROM_START(); Replay = tailing post-drain
/// for ongoing changes using the same query + the last paging state
/// returned by Cosmos. Phase only flips Bulk → Replay, never back.
/// </summary>
public enum PartitionPhase { Bulk, Replay }

/// <summary>
/// Represents a feed range partition with its in-flight checkpoint
/// list. Uses LinkedList for clean node management. The partition
/// owns its persisted checkpoint state via <see cref="Snapshot"/> so
/// workers checkpoint through the partition directly (no round-trip
/// to TableMigration's dicts).
/// </summary>
public sealed class Partition
{
    public string FeedRange => Snapshot.FeedRange;
    public bool IsExhausted { get; private set; }
    public byte[]? LastPagingState { get; private set; }
    public PartitionPhase Phase { get; private set; }

    /// <summary>
    /// Persisted per-feed-range snapshot — bulk + replay checkpoints
    /// and bulk-completed flag. Lives on
    /// <see cref="TableMigration.Partitions"/> and is mutated
    /// through this reference by the owning worker.
    /// </summary>
    public PartitionSnapshot Snapshot { get; }

    /// <summary>
    /// Per-table state for the table that owns this partition. Kept
    /// private so workers go through typed pass-through accessors
    /// (<see cref="Spec"/>, <see cref="Columns"/>, <see cref="Tracker"/>,
    /// <see cref="FullTableName"/>, <see cref="TotalFeedRanges"/>,
    /// <see cref="BulkCompletedCount"/>) rather than reaching into a
    /// shared bag. This keeps the worker → table coupling explicit and
    /// stops new code from grabbing the whole table object.
    /// </summary>
    private readonly TableResources _table;

    /// <summary>Source/target table identifiers and column metadata.</summary>
    public TableCopySpec Spec => _table.Spec;

    /// <summary>Ordered column list used to materialize rows and bind writes.</summary>
    public List<CassandraColumn> Columns => _table.Columns;

    /// <summary>Per-table progress / metrics sink.</summary>
    public CopyProgressTracker Tracker => _table.Tracker;

    /// <summary>Human-readable "keyspace.table" identifier for logs.</summary>
    public string FullTableName => _table.FullTableName;

    /// <summary>Total feed ranges across the owning table.</summary>
    public int TotalFeedRanges => _table.TotalFeedRanges;

    /// <summary>Feed ranges in the owning table whose bulk phase has completed.</summary>
    public int BulkCompletedCount => _table.BulkCompletedCount;

    /// <summary>True iff the owning table is a counter table (cached on the table).</summary>
    public bool IsCounterTable => _table.IsCounterTable;

    private readonly LinkedList<WorkChunk> _chunks = new();
    private readonly object _chunksLock = new();

    public Partition(PartitionSnapshot snapshot, byte[]? initialPagingState, TableResources table, PartitionPhase phase = PartitionPhase.Bulk)
    {
        Snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
        LastPagingState = initialPagingState;
        Phase = phase;
        _table = table;

        if (initialPagingState != null)
            _chunks.AddLast(new WorkChunk { ContinuationToken = initialPagingState, IsCompleted = true });
    }

    public void SetPageState(byte[]? pagingState, bool isExhausted)
    {
        lock (_chunksLock)
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

    // ── Checkpoint API ─────────────────────────────────────────
    // Workers checkpoint through these methods. The dict on
    // TableMigration is mutated in-place via the shared Snapshot
    // reference — no round-trip through the owning table.

    /// <summary>Persist the paging state for this range. Same field
    /// services bulk and replay because a partition is in exactly
    /// one phase at a time.</summary>
    public void SaveCheckpoint(byte[]? token)
    {
        if (token == null) return;
        Snapshot.ContinuationToken = Convert.ToBase64String(token);
    }

    /// <summary>
    /// Online bulk → replay handoff. Sets <see cref="PartitionSnapshot.BulkCompleted"/>
    /// while preserving the current <see cref="PartitionSnapshot.ContinuationToken"/>
    /// as the replay anchor. On the first writer the partition notifies
    /// <see cref="_table"/> so the table-level counter and drain
    /// signal advance — workers never touch table-level state directly.
    /// </summary>
    public void HandoffToReplay()
    {
        if (Snapshot.BulkCompleted) return;
        Snapshot.BulkCompleted = true;
        _table.OnPartitionBulkCompleted();
    }

    /// <summary>
    /// Offline final completion. Sets <see cref="PartitionSnapshot.BulkCompleted"/>
    /// and clears <see cref="PartitionSnapshot.ContinuationToken"/> so resume
    /// skips the range entirely. On the first writer the partition
    /// notifies <see cref="_table"/>.
    /// </summary>
    public void CompleteOffline()
    {
        if (Snapshot.BulkCompleted) return;
        Snapshot.BulkCompleted = true;
        Snapshot.ContinuationToken = null;
        _table.OnPartitionBulkCompleted();
    }

    /// <summary>
    /// Persisted per-feed-range checkpoint state for a table's bulk
    /// copy and (when online) change-feed replay. One instance per
    /// feed range, keyed in <see cref="TableMigration.Partitions"/>
    /// by the feed range JSON. Nested inside <see cref="Partition"/>
    /// because it is the persistence-shape projection of a partition's
    /// runtime state — workers mutate it through Partition's
    /// checkpoint API, never directly.
    /// </summary>
    public sealed class PartitionSnapshot
    {
        public string FeedRange { get; set; } = string.Empty;

        /// <summary>
        /// Base64-encoded paging state for whichever phase the
        /// range is currently in. While bulk is in progress this is
        /// the last successfully-written bulk page; on bulk drain
        /// (online) it carries forward as the replay handoff anchor;
        /// during replay it advances with each tail page. Offline
        /// completion clears it. <c>null</c> means: start of range.
        /// </summary>
        public string? ContinuationToken { get; set; }

        /// <summary>
        /// True once the bulk drain for this range reached an empty
        /// page. On resume, completed ranges are skipped (offline) or
        /// re-seeded directly into Replay (online), reading
        /// <see cref="ContinuationToken"/> as the replay anchor.
        /// </summary>
        public bool BulkCompleted { get; set; }

        /// <summary>Serialize this snapshot to a JSON string.</summary>
        public string Serialize() => JsonConvert.SerializeObject(this);

        /// <summary>Restore a snapshot from a JSON string produced by <see cref="Serialize"/>.</summary>
        public static PartitionSnapshot Deserialize(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
                throw new ArgumentException("Snapshot JSON must be non-empty.", nameof(json));
            return JsonConvert.DeserializeObject<PartitionSnapshot>(json)
                ?? throw new InvalidOperationException("Failed to deserialize PartitionSnapshot.");
        }
    }
}
