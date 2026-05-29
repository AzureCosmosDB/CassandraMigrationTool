using CassandraMigrationProcessor.DataTransfer;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;

namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Lightweight per-table snapshot embedded in <see cref="Job.Tables"/>:
/// identity, status, and rolled-up copy / change-feed counters used by the UI.
/// </summary>
public class TableMigrationSummary
{
    // ── Identity ──

    [JsonIgnore]
    public Job? ParentJob;

    public string Id { get; set; } = string.Empty;
    public string JobId { get; set; } = string.Empty;
    public string KeyspaceName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string? TargetKeyspaceName { get; set; }
    public string? TargetTableName { get; set; }

    // ── Change Feed Summary ──

    public long ChangeFeedUpdatesInLastBatch { get; set; }
    public double ChangeFeedAvgReadLatencyInMS { get; set; }
    public double ChangeFeedAvgWriteLatencyInMS { get; set; }
    public DateTime? ChangeFeedLastChecked { get; set; }

    // ── Bulk Copy Summary ──

    public double CopyPercent { get; set; }
    public bool CopyComplete { get; set; }
    public long CopyRowsCopied { get; set; }
    public double CopyRowsPerSecond { get; set; }
    public long TotalRowCount { get; set; }

    public TableStatus SourceStatus { get; set; }

    // Skip tracking for max retries exceeded
    public bool SkippedDueToMaxRetries { get; set; } = false;
    public string? FailedOperation { get; set; } = null;

    public string GetEffectiveTargetKeyspaceName()
    {
        return string.IsNullOrWhiteSpace(TargetKeyspaceName)
            ? KeyspaceName : TargetKeyspaceName;
    }

    public string GetEffectiveTargetTableName()
    {
        return string.IsNullOrWhiteSpace(TargetTableName)
            ? TableName : TargetTableName;
    }

    /// <summary>
    /// True when this table summary represents a row we should still
    /// touch during migration. <see cref="TableStatus.OK"/> and
    /// <see cref="TableStatus.Failed"/> both qualify — Failed tables
    /// are retried on resume. Only <see cref="TableStatus.NotFound"/>
    /// (e.g. dropped on the source) is excluded.
    /// </summary>
    [JsonIgnore]
    public bool IsValid =>
        SourceStatus == TableStatus.OK
        || SourceStatus == TableStatus.Failed;
}

/// <summary>
/// Full per-table migration document (persisted as <c>{unitId}.json</c>): extends
/// the summary with bulk-copy phase, per-feed-range <see cref="Partition.PartitionSnapshot"/>,
/// change-feed checkpoints, and live counters.
/// </summary>
public class TableMigration : TableMigrationSummary
{
    // ── Bulk Copy State ──

    public BulkCopyPhase BulkCopyPhase { get; set; } = BulkCopyPhase.NotStarted;

    public DateTime? BulkCopyStartedOn { get; set; }
    public DateTime? BulkCopyEndedOn { get; set; }

    /// <summary>
    /// True once the bulk copy of this table has fully drained from
    /// the source. Used to skip the bulk-drain wait on offline
    /// resume after a process restart. <c>null</c> / <c>false</c>
    /// means the table still needs a bulk pass.
    /// </summary>
    public bool? BulkDownloaded { get; set; }

    /// <summary>
    /// Bulk-copy failed row count. Mirrors
    /// <see cref="TableMigrationSummary.CopyRowsCopied"/> (which
    /// holds the success count) — together they form the totals
    /// surfaced by <see cref="GetProcessedTotals"/>.
    /// </summary>
    public long TargetFailedRowCount { get; set; }

    public long EstimatedRowCount { get; set; }
    public long ActualRowCount { get; set; }
    public long SourceCountDuringCopy { get; set; }

    /// <summary>
    /// Per-feed-range partition state — bulk checkpoint,
    /// replay checkpoint, and bulk-completed flag — keyed by
    /// feed range JSON. Each runtime <see cref="DataTransfer.Partition"/>
    /// holds a reference to its <see cref="Partition.PartitionSnapshot"/>
    /// entry, so workers checkpoint through the partition directly.
    /// </summary>
    public Dictionary<string, Partition.PartitionSnapshot> Partitions { get; set; } = new();

    // ── Change Feed State ──

    public DateTime? ChangeFeedStartedOn { get; set; }
    public string? ChangeFeedContinuationToken { get; set; }

    /// <summary>
    /// Change feed start time captured BEFORE bulk copy
    /// begins. Used as the COSMOS_CHANGEFEED_START_TIME()
    /// anchor so changes during copy are not lost.
    /// </summary>
    public string? ChangeFeedStartToken { get; set; }

    // ── Change Feed Counters (Interlocked for thread safety) ──

    internal long _changeFeedInsertEvents;
    // Reserved for FFCF: currently always 0 (insert-only pipeline)
    internal long _changeFeedDeleteEvents;
    // Reserved for FFCF: currently always 0 (insert-only pipeline)
    internal long _changeFeedUpdateEvents;
    internal long _changeFeedErrors;
    internal long _changeFeedRowsInserted;
    // Reserved for FFCF: currently always 0 (insert-only pipeline)
    internal long _changeFeedRowsDeleted;
    // Reserved for FFCF: currently always 0 (insert-only pipeline)
    internal long _changeFeedRowsUpdated;
    internal long _changeFeedUpdatesInLastBatch;

    public long ChangeFeedInsertEvents
    {
        get => Interlocked.Read(ref _changeFeedInsertEvents);
        set => Interlocked.Exchange(
            ref _changeFeedInsertEvents, value);
    }
    // Reserved for FFCF: currently always 0 (insert-only pipeline)
    public long ChangeFeedDeleteEvents
    {
        get => Interlocked.Read(ref _changeFeedDeleteEvents);
        set => Interlocked.Exchange(
            ref _changeFeedDeleteEvents, value);
    }
    // Reserved for FFCF: currently always 0 (insert-only pipeline)
    public long ChangeFeedUpdateEvents
    {
        get => Interlocked.Read(ref _changeFeedUpdateEvents);
        set => Interlocked.Exchange(
            ref _changeFeedUpdateEvents, value);
    }
    public long ChangeFeedErrors
    {
        get => Interlocked.Read(ref _changeFeedErrors);
        set => Interlocked.Exchange(
            ref _changeFeedErrors, value);
    }

    public long ChangeFeedRowsInserted
    {
        get => Interlocked.Read(ref _changeFeedRowsInserted);
        set => Interlocked.Exchange(
            ref _changeFeedRowsInserted, value);
    }
    // Reserved for FFCF: currently always 0 (insert-only pipeline)
    public long ChangeFeedRowsDeleted
    {
        get => Interlocked.Read(ref _changeFeedRowsDeleted);
        set => Interlocked.Exchange(
            ref _changeFeedRowsDeleted, value);
    }
    // Reserved for FFCF: currently always 0 (insert-only pipeline)
    public long ChangeFeedRowsUpdated
    {
        get => Interlocked.Read(ref _changeFeedRowsUpdated);
        set => Interlocked.Exchange(
            ref _changeFeedRowsUpdated, value);
    }

    // ── Constructor ──

    public TableMigration(
        Job job,
        string keyspaceName,
        string tableName)
    {
        this.Id = GenerateId(keyspaceName, tableName);
        this.KeyspaceName = keyspaceName;
        this.TableName = tableName;
        this.TargetKeyspaceName = keyspaceName;
        this.TargetTableName = tableName;
        if (job != null)
        {
            this.JobId = job.Id;
            this.ParentJob = job;
        }
    }

    /// <summary>
    /// Stable deterministic id for a (keyspace, table) pair: first 16
    /// hex chars of SHA-256("keyspace.table").
    /// </summary>
    public static string GenerateId(string keyspaceName, string tableName)
    {
        using var sha = SHA256.Create();
        byte[] hashBytes = sha.ComputeHash(
            Encoding.UTF8.GetBytes($"{keyspaceName}.{tableName}"));
        return BitConverter.ToString(hashBytes)
            .Replace("-", "").Substring(0, 16).ToLower();
    }

    /// <summary>
    /// Aggregates bulk-copy totals: <c>(Total, Inserted, Failed)</c>.
    /// Inserted comes from <see cref="TableMigrationSummary.CopyRowsCopied"/>
    /// (the running success counter maintained by
    /// <see cref="DataTransfer.CopyProgressTracker"/>); Failed comes
    /// from <see cref="TargetFailedRowCount"/>.
    /// </summary>
    public (long Total, long Inserted, long Failed) GetProcessedTotals()
    {
        long inserted = CopyRowsCopied;
        long failed = TargetFailedRowCount;
        return (inserted + failed, inserted, failed);
    }

    // Migrates legacy per-table CopyChunks JSON onto the inline
    // fields above. JsonExtensionData captures the unknown
    // "CopyChunks" property when loading older job docs; the
    // OnDeserialized callback folds the first chunk's values onto
    // BulkDownloaded / CopyRowsCopied / TargetFailedRowCount /
    // SourceCountDuringCopy, then removes the entry so subsequent
    // saves emit only the new shape.

    [JsonExtensionData]
#pragma warning disable CS0649 // Populated via reflection by Newtonsoft.Json [JsonExtensionData].
    private IDictionary<string, JToken>? _legacyFields;
#pragma warning restore CS0649

    [OnDeserialized]
    private void MigrateLegacyCopyChunks(StreamingContext _)
    {
        if (_legacyFields == null) return;
        if (!_legacyFields.TryGetValue("CopyChunks", out var token)) return;
        if (token is JArray arr && arr.Count > 0 && arr[0] is JObject c)
        {
            BulkDownloaded ??= c["IsDownloaded"]?.ToObject<bool?>();
            if (CopyRowsCopied == 0)
            {
                long v = c["TargetInsertedRowCount"]?.ToObject<long>() ?? 0;
                if (v > 0) CopyRowsCopied = v;
            }
            if (TargetFailedRowCount == 0)
            {
                long v = c["TargetFailedRowCount"]?.ToObject<long>() ?? 0;
                if (v > 0) TargetFailedRowCount = v;
            }
            if (SourceCountDuringCopy == 0)
            {
                long v = c["SourceQueryRowCount"]?.ToObject<long>() ?? 0;
                if (v > 0) SourceCountDuringCopy = v;
            }
        }
        _legacyFields.Remove("CopyChunks");
    }
}
