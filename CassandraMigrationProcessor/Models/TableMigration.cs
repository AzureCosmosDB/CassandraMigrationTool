using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;

namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Rolled-up bulk-copy totals returned by
/// <see cref="TableMigration.GetProcessedTotals"/>. <c>Total</c> is
/// <c>Inserted + Failed</c>; the split is preserved so callers can
/// render both numbers without re-reading the underlying counters.
/// </summary>
public readonly record struct ProcessedTotals(long Total, long Inserted, long Failed);

/// <summary>
/// Lightweight per-table snapshot embedded in <see cref="Job.Tables"/>:
/// identity, status, and rolled-up copy / change-feed counters used by the UI.
/// </summary>
public class TableMigrationSummary
{
    // ── Identity ──

    [JsonIgnore]
    public Job? ParentJob { get; set; }

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

    // Cumulative replay counters, propagated from the live
    // TableMigration by TableMigrationMapper.ToSummary.
    public long ChangeFeedRowsInserted { get; set; }
    public long ChangeFeedInsertEvents { get; set; }

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
        => Coalesce(TargetKeyspaceName, KeyspaceName);

    public string GetEffectiveTargetTableName()
        => Coalesce(TargetTableName, TableName);

    private static string Coalesce(string? overrideName, string fallback)
        => string.IsNullOrWhiteSpace(overrideName) ? fallback : overrideName;

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
/// the summary with bulk-copy phase, per-feed-range <see cref="PartitionSnapshot"/>,
/// change-feed checkpoints, and live counters.
/// </summary>
public class TableMigration : TableMigrationSummary
{
    // ── Bulk Copy State ──

    [JsonProperty]
    public BulkCopyPhase BulkCopyPhase { get; private set; } = BulkCopyPhase.NotStarted;

    /// <summary>
    /// Advances <see cref="BulkCopyPhase"/> to <paramref name="next"/>.
    /// Throws <see cref="InvalidOperationException"/> if the move would
    /// regress (the lifecycle is strictly forward-only:
    /// <c>NotStarted → InitializingDestination → Copying → Completed</c>).
    /// Idempotent: setting to the current phase is a no-op.
    /// </summary>
    public void AdvanceBulkCopyPhase(BulkCopyPhase next)
    {
        if (next == BulkCopyPhase) return;
        if ((int)next < (int)BulkCopyPhase)
            throw new InvalidOperationException(
                $"BulkCopyPhase cannot regress from {BulkCopyPhase} to {next} " +
                $"for {KeyspaceName}.{TableName}.");
        BulkCopyPhase = next;
    }

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
    /// Per-feed-range partition state (bulk + replay checkpoint plus
    /// bulk-completed flag) keyed by feed range JSON. Each runtime
    /// <see cref="DataTransfer.Partition"/> holds a reference to its
    /// checkpoint entry so workers checkpoint through the partition.
    /// </summary>
    public Dictionary<string, PartitionSnapshot> Partitions { get; set; } = new();

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
    //
    // Workers write through Interlocked.Add directly on the internal
    // fields below; readers (UI / mapper) consume the snapshot stored
    // on the base TableMigrationSummary auto-properties via
    // TableMigrationMapper.ToSummary. There is no "new" hide:
    // TableMigration deliberately does NOT re-declare the
    // ChangeFeedRowsInserted / ChangeFeedInsertEvents properties — the
    // single source of truth for any read is "what the mapper last
    // snapshotted", which keeps display values consistent and avoids
    // the two-backing-stores hazard the previous design carried.

    internal long _changeFeedInsertEvents;
    internal long _changeFeedErrors;
    internal long _changeFeedRowsInserted;
    internal long _changeFeedUpdatesInLastBatch;
    // Sticky "last flushed batch size" the dashboard renders.
    // TableMigrationMapper.UpdateParentJob captures the live
    // accumulator, zeroes it, and stamps non-zero values here so the
    // display survives between flushes.
    internal long _changeFeedLastFlushedBatch;

    public long ChangeFeedErrors
    {
        get => Interlocked.Read(ref _changeFeedErrors);
        set => Interlocked.Exchange(
            ref _changeFeedErrors, value);
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
        return Convert.ToHexString(hashBytes)[..16].ToLowerInvariant();
    }

    /// <summary>
    /// Aggregates bulk-copy totals: <c>(Total, Inserted, Failed)</c>.
    /// </summary>
    public ProcessedTotals GetProcessedTotals()
    {
        long inserted = CopyRowsCopied;
        long failed = TargetFailedRowCount;
        return new ProcessedTotals(inserted + failed, inserted, failed);
    }

    // Migrates legacy per-table CopyChunks JSON onto the inline fields.
    // The OnDeserialized callback folds the first chunk's values, then
    // removes the entry so subsequent saves emit only the new shape.

    [JsonExtensionData]
#pragma warning disable CS0649 // Populated via reflection by Newtonsoft.Json [JsonExtensionData].
    private IDictionary<string, JToken>? _legacyFields;
#pragma warning restore CS0649

    [OnDeserialized]
    private void MigrateLegacyCopyChunks(StreamingContext context)
    {
        if (_legacyFields == null) return;
        if (!_legacyFields.TryGetValue("CopyChunks", out var token)) return;
        if (token is JArray arr && arr.Count > 0 && arr[0] is JObject c)
        {
            // Per-field guards: one malformed legacy value must not abort
            // the whole [OnDeserialized] callback. Newtonsoft surfaces a
            // callback throw as a deserialization failure, which the outer
            // SafeExecute in UnitStore.GetFromStorage swallows to null —
            // making the unit look "missing" and triggering wizard-side
            // re-materialisation that silently drops the operator's
            // checkpoint state.
            BulkDownloaded ??= TryReadLegacy<bool?>(c, "IsDownloaded", out _);
            if (CopyRowsCopied == 0)
            {
                long v = TryReadLegacy<long?>(c, "TargetInsertedRowCount", out var insertedFailed) ?? 0;
                if (v > 0) CopyRowsCopied = v;
                // Cross-field guard: if the inserted-count read failed but
                // IsDownloaded came through as true, the bulk pass would be
                // marked Completed with CopyRowsCopied = 0 by the
                // coordinator. Clear BulkDownloaded so the runner re-validates.
                if (insertedFailed && BulkDownloaded == true)
                {
                    BulkDownloaded = null;
                    Console.Error.WriteLine(
                        $"[WARN] Cleared BulkDownloaded on {KeyspaceName}.{TableName} " +
                        $"because legacy TargetInsertedRowCount was unreadable; " +
                        $"runner will re-validate the bulk pass.");
                }
            }
            if (TargetFailedRowCount == 0)
                AssignIfZero(c, "TargetFailedRowCount", v => TargetFailedRowCount = v);
            if (SourceCountDuringCopy == 0)
                AssignIfZero(c, "SourceQueryRowCount", v => SourceCountDuringCopy = v);
        }
        _legacyFields.Remove("CopyChunks");
    }

    /// <summary>
    /// If the legacy JSON object has a positive value at
    /// <paramref name="field"/>, hand it to <paramref name="assign"/>.
    /// Read failures and missing/zero values are intentionally swallowed —
    /// the whole point of the legacy migration path is best-effort
    /// fold-forward, and the caller is expected to only invoke this when
    /// the current field is still at its default.
    /// </summary>
    private void AssignIfZero(JObject source, string field, Action<long> assign)
    {
        long v = TryReadLegacy<long?>(source, field, out _) ?? 0;
        if (v > 0) assign(v);
    }

    private T? TryReadLegacy<T>(JObject source, string field, out bool readFailed)
    {
        readFailed = false;
        var node = source[field];
        if (node == null) return default;
        try
        {
            return node.ToObject<T>();
        }
        catch (JsonException ex)
        {
            readFailed = true;
            Console.Error.WriteLine(
                $"[WARN] Skipping unreadable legacy field '{field}' on " +
                $"{KeyspaceName}.{TableName}: {ex.Message}");
            return default;
        }
    }
}
