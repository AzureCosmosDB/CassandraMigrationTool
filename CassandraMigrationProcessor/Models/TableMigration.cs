using CassandraMigrationProcessor.Infrastructure;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading;

namespace CassandraMigrationProcessor.Models
{
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
    }

    public class TableMigration : TableMigrationSummary
    {
        // ── Bulk Copy State ──

        public DateTime? BulkCopyStartedOn { get; set; }
        public DateTime? BulkCopyEndedOn { get; set; }

        public List<CopyChunk> CopyChunks { get; set; } = new();

        public long EstimatedRowCount { get; set; }
        public long ActualRowCount { get; set; }
        public long SourceCountDuringCopy { get; set; }

        /// <summary>
        /// Per-feed-range copy checkpoint. Key = feed range JSON,
        /// Value = base64-encoded paging state. null value means
        /// the range is fully copied. Persisted periodically so
        /// resume can skip completed ranges and continue from
        /// the last checkpoint of in-progress ranges.
        /// </summary>
        public Dictionary<string, string?> CopyFeedRangeCheckpoints { get; set; } = new();

        /// <summary>
        /// Set of feed ranges whose bulk copy completed fully.
        /// On resume, these ranges are skipped entirely.
        /// </summary>
        public HashSet<string> CompletedCopyFeedRanges { get; set; } = new();

        // ── Change Feed State ──

        public DateTime? ChangeFeedStartedOn { get; set; }
        public string? ChangeFeedContinuationToken { get; set; }

        /// <summary>
        /// Per-feed-range continuation tokens for parallel
        /// change feed. Key = feed range JSON string,
        /// Value = base64-encoded paging state.
        /// Used when feed ranges > 1 for a table.
        /// </summary>
        public Dictionary<string, string>
            FeedRangeContinuationTokens
        { get; set; } = new();

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
            this.Id = MigrationUtilities.GenerateMigrationUnitId(
                keyspaceName, tableName);
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
    }
}
