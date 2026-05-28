using Newtonsoft.Json;
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
    /// (e.g. dropped on the source) is excluded. Replaces
    /// <c>MigrationUtilities.IsMigrationUnitValid</c>.
    /// </summary>
    [JsonIgnore]
    public bool IsValid =>
        SourceStatus == TableStatus.OK
        || SourceStatus == TableStatus.Failed;
}

/// <summary>
/// Full per-table migration document (persisted as <c>{unitId}.json</c>): extends
/// the summary with bulk-copy phase, per-feed-range <see cref="PartitionState"/>,
/// change-feed checkpoints, and live counters.
/// </summary>
public class TableMigration : TableMigrationSummary
{
    // ── Bulk Copy State ──

    public BulkCopyPhase BulkCopyPhase { get; set; } = BulkCopyPhase.NotStarted;

    public DateTime? BulkCopyStartedOn { get; set; }
    public DateTime? BulkCopyEndedOn { get; set; }

    public List<CopyChunk> CopyChunks { get; set; } = new();

    public long EstimatedRowCount { get; set; }
    public long ActualRowCount { get; set; }
    public long SourceCountDuringCopy { get; set; }

    /// <summary>
    /// Per-feed-range partition state — bulk checkpoint,
    /// replay checkpoint, and bulk-completed flag — keyed by
    /// feed range JSON. Replaces the three parallel dicts that
    /// used to live here (CopyFeedRangeCheckpoints +
    /// CompletedCopyFeedRanges + FeedRangeContinuationTokens).
    /// Each runtime <see cref="DataTransfer.Partition"/> holds a
    /// reference to its <see cref="PartitionState"/> entry, so
    /// workers checkpoint through the partition directly instead
    /// of reaching back through the MigrationUnit dicts.
    /// </summary>
    public Dictionary<string, PartitionState> Partitions { get; set; } = new();

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
        string tableName,
        List<CopyChunk> CopyChunks)
    {
        this.Id = GenerateId(keyspaceName, tableName);
        this.KeyspaceName = keyspaceName;
        this.TableName = tableName;
        this.TargetKeyspaceName = keyspaceName;
        this.TargetTableName = tableName;
        this.CopyChunks = CopyChunks;
        if (job != null)
        {
            this.JobId = job.Id;
            this.ParentJob = job;
        }
    }

    /// <summary>
    /// Stable deterministic id for a (keyspace, table) pair: first 16
    /// hex chars of SHA-256("keyspace.table"). Lives here because the
    /// id is a TableMigration concern and was previously buried in
    /// <c>MigrationUtilities</c>.
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
    /// Aggregates copy-chunk totals: <c>(Total, Inserted, Failed)</c>.
    /// Replaces <c>MigrationUtilities.GetProcessedTotals(TableMigration)</c>.
    /// </summary>
    public (long Total, long Inserted, long Failed) GetProcessedTotals()
    {
        long inserted = CopyChunks?.Sum(c => c.TargetInsertedRowCount) ?? 0;
        long failed = CopyChunks?.Sum(c => c.TargetFailedRowCount) ?? 0;
        return (inserted + failed, inserted, failed);
    }
}
