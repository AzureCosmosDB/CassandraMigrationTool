namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Persisted per-feed-range checkpoint state for a table's bulk
/// copy and (when online) change-feed replay. One instance per
/// feed range, keyed in <see cref="TableMigration.Partitions"/>
/// by the feed range JSON. Replaces three parallel dictionaries
/// (CopyFeedRangeCheckpoints + CompletedCopyFeedRanges +
/// FeedRangeContinuationTokens) that used to live directly on
/// <see cref="TableMigration"/>.
/// </summary>
public class PartitionState
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
}
