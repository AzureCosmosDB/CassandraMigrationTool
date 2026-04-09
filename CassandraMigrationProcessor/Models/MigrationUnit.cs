using Newtonsoft.Json;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers;
using System;
using System.Collections.Generic;
using System.Threading;

namespace CassandraMigrationProcessor.Models
{
    public class NameValuePair
    {
        public string Name { get; set; } = "";
        public string? Value { get; set; }
    }

    public class MigrationUnitBasic
    {
        [JsonIgnore]
        public MigrationJob? ParentJob;

        public string Id { get; set; } = string.Empty;
        public string JobId { get; set; } = string.Empty;
        public string KeyspaceName { get; set; } = string.Empty;
        public string TableName { get; set; } = string.Empty;
        public string? TargetKeyspaceName { get; set; }
        public string? TargetTableName { get; set; }

        public long ChangeFeedUpdatesInLastBatch { get; set; }
        public double ChangeFeedAvgReadLatencyInMS { get; set; }
        public double ChangeFeedAvgWriteLatencyInMS { get; set; }
        public DateTime? ChangeFeedLastChecked { get; set; }

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

    public class MigrationUnit : MigrationUnitBasic
    {
        public DateTime? BulkCopyStartedOn { get; set; }
        public DateTime? BulkCopyEndedOn { get; set; }
        public bool TargetCreated { get; set; }

        public DateTime? ChangeFeedStartedOn { get; set; }
        public string? ChangeFeedContinuationToken { get; set; }

        /// <summary>
        /// Per-feed-range continuation tokens for parallel
        /// change feed. Key = feed range JSON string,
        /// Value = base64-encoded paging state.
        /// Used when feed ranges > 1 for a table.
        /// </summary>
        public Dictionary<string, string>?
            FeedRangeContinuationTokens
        { get; set; }

        /// <summary>
        /// Change feed start time captured BEFORE bulk copy
        /// begins. Used as the COSMOS_CHANGEFEED_START_TIME()
        /// anchor so changes during copy are not lost.
        /// </summary>
        public string? ChangeFeedStartToken { get; set; }

        /// <summary>
        /// Per-feed-range copy checkpoint. Key = feed range JSON,
        /// Value = base64-encoded paging state. null value means
        /// the range is fully copied. Persisted periodically so
        /// resume can skip completed ranges and continue from
        /// the last checkpoint of in-progress ranges.
        /// </summary>
        public Dictionary<string, string?>?
            CopyFeedRangeCheckpoints
        { get; set; }

        /// <summary>
        /// Set of feed ranges whose bulk copy completed fully.
        /// On resume, these ranges are skipped entirely.
        /// </summary>
        public HashSet<string>?
            CompletedCopyFeedRanges
        { get; set; }

        public long EstimatedRowCount { get; set; }
        public long ActualRowCount { get; set; }
        public long SourceCountDuringCopy { get; set; }

        // Backing fields for Interlocked access in parallel
        // change feed. Properties delegate to these fields.
        internal long _changeFeedInsertEvents;
        internal long _changeFeedDeleteEvents;
        internal long _changeFeedUpdateEvents;
        internal long _changeFeedErrors;
        internal long _changeFeedRowsInserted;
        internal long _changeFeedRowsDeleted;
        internal long _changeFeedRowsUpdated;
        internal long _changeFeedUpdatesInLastBatch;

        public long ChangeFeedInsertEvents
        {
            get => Interlocked.Read(ref _changeFeedInsertEvents);
            set => Interlocked.Exchange(
                ref _changeFeedInsertEvents, value);
        }
        public long ChangeFeedDeleteEvents
        {
            get => Interlocked.Read(ref _changeFeedDeleteEvents);
            set => Interlocked.Exchange(
                ref _changeFeedDeleteEvents, value);
        }
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
        public long ChangeFeedRowsDeleted
        {
            get => Interlocked.Read(ref _changeFeedRowsDeleted);
            set => Interlocked.Exchange(
                ref _changeFeedRowsDeleted, value);
        }
        public long ChangeFeedRowsUpdated
        {
            get => Interlocked.Read(ref _changeFeedRowsUpdated);
            set => Interlocked.Exchange(
                ref _changeFeedRowsUpdated, value);
        }

        public List<MigrationChunk> MigrationChunks { get; set; } = new();

        public MigrationUnit(
            MigrationJob job,
            string keyspaceName,
            string tableName,
            List<MigrationChunk> migrationChunks)
        {
            this.Id = MigrationHelper.GenerateMigrationUnitId(
                keyspaceName, tableName);
            this.KeyspaceName = keyspaceName;
            this.TableName = tableName;
            this.TargetKeyspaceName = keyspaceName;
            this.TargetTableName = tableName;
            this.MigrationChunks = migrationChunks;
            if (job != null)
            {
                this.JobId = job.Id;
                this.ParentJob = job;
            }
        }

        private static readonly object _updateParentLock = new();

        public bool UpdateParentJob()
        {
            if (this.ParentJob == null) return false;

            try
            {
                lock (_updateParentLock)
                {
                    var index = ParentJob.Tables
                        .FindIndex(mu => mu.Id == this.Id);
                    if (index == -1) return false;

                    ToSummary(ParentJob.Tables[index]);
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public MigrationUnitBasic ToSummary(
            MigrationUnitBasic? mub = null)
        {
            if (mub == null)
                mub = new MigrationUnitBasic();

            mub.Id = MigrationHelper.GenerateMigrationUnitId(
                this.KeyspaceName, this.TableName);
            mub.JobId = this.JobId;
            mub.KeyspaceName = this.KeyspaceName;
            mub.TableName = this.TableName;
            mub.TargetKeyspaceName = this.TargetKeyspaceName;
            mub.TargetTableName = this.TargetTableName;
            mub.ChangeFeedUpdatesInLastBatch =
                Interlocked.Exchange(ref _changeFeedUpdatesInLastBatch, 0);
            mub.ChangeFeedAvgReadLatencyInMS =
                this.ChangeFeedAvgReadLatencyInMS;
            mub.ChangeFeedAvgWriteLatencyInMS =
                this.ChangeFeedAvgWriteLatencyInMS;
            mub.CopyPercent = this.CopyPercent;
            mub.CopyComplete = this.CopyComplete;
            mub.CopyRowsCopied = this.CopyRowsCopied;
            mub.CopyRowsPerSecond = this.CopyRowsPerSecond;
            mub.TotalRowCount = Math.Max(
                this.EstimatedRowCount, this.ActualRowCount);
            mub.SourceStatus = this.SourceStatus;
            mub.SkippedDueToMaxRetries =
                this.SkippedDueToMaxRetries;
            mub.FailedOperation = this.FailedOperation;
            mub.ChangeFeedLastChecked =
                this.ChangeFeedLastChecked;
            return mub;
        }
    }
}
