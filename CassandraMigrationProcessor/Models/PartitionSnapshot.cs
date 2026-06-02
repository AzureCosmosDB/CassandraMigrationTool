using Newtonsoft.Json;

namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Persisted per-feed-range snapshot of bulk-copy and (when online)
/// change-feed replay progress. One instance per feed range, keyed
/// in <see cref="TableMigration.Partitions"/> by the feed range JSON.
/// Lives in <c>Models/</c> because it is the persistence-shape
/// projection; runtime mutation happens via
/// <c>DataTransfer.Partition</c>'s snapshot API.
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

    public string Serialize() => JsonConvert.SerializeObject(this);

    public static PartitionSnapshot Deserialize(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            throw new ArgumentException("PartitionSnapshot JSON must be non-empty.", nameof(json));
        return JsonConvert.DeserializeObject<PartitionSnapshot>(json)
            ?? throw new InvalidOperationException("Failed to deserialize PartitionSnapshot.");
    }
}
