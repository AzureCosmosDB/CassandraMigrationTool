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
    /// Base64-encoded paging state from the last successfully
    /// written bulk page. <c>null</c> means: never checkpointed
    /// (resume from the start of the range), or bulk completed
    /// and the token was cleared.
    /// </summary>
    public string? CopyContinuationToken { get; set; }

    /// <summary>
    /// Base64-encoded paging state for the change-feed replay
    /// tail. Written by replay-phase workers. <c>null</c> means:
    /// replay never advanced past the bulk handoff.
    /// </summary>
    public string? ReplayContinuationToken { get; set; }

    /// <summary>
    /// True once the bulk drain for this range reached an empty
    /// page. On resume, completed ranges are skipped (offline) or
    /// re-seeded directly into Replay (online).
    /// </summary>
    public bool BulkCompleted { get; set; }
}
