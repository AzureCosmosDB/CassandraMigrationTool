using Cassandra;
using CassandraMigrationProcessor.Context;
using System;
using System.Collections.Generic;

namespace CassandraMigrationProcessor.Helpers.JobManagement
{
    /// <summary>
    /// Tracks accumulated change feed events (inserts, updates,
    /// deletes) for a single Cassandra table migration unit.
    /// Used to batch-apply changes from Cosmos DB change feed
    /// to OSS Cassandra target.
    /// </summary>
    public class AccumulatedChangesTracker
    {
        private readonly object _lock = new object();

        /// <summary>
        /// Rows to be inserted, keyed by primary key JSON string.
        /// </summary>
        public Dictionary<string, Row> RowsToBeInserted
        { get; private set; } = new();

        /// <summary>
        /// Rows to be updated, keyed by primary key JSON string.
        /// </summary>
        public Dictionary<string, Row> RowsToBeUpdated
        { get; private set; } = new();

        /// <summary>
        /// Rows to be deleted, keyed by primary key JSON string.
        /// </summary>
        public Dictionary<string, Row> RowsToBeDeleted
        { get; private set; } = new();

        public long TotalChangesCount
        {
            get
            {
                return RowsToBeInserted.Count
                    + RowsToBeUpdated.Count
                    + RowsToBeDeleted.Count;
            }
        }

        private long _totalEventCount = 0;
        public long TotalEventCount => _totalEventCount;

        public string TableKey { get; private set; } = string.Empty;
        public string LatestContinuationToken { get; private set; }
            = string.Empty;
        public DateTime LatestTimestamp { get; private set; }
            = DateTime.MinValue;

        public long ReadDurationMs { get; set; } = 0;
        public long WriteDurationMs { get; set; } = 0;

        private readonly string _tableKey;

        public AccumulatedChangesTracker(string tableKey)
        {
            _tableKey = tableKey;
            TableKey = _tableKey;
        }

        public void AddInsert(string primaryKey, Row row)
        {
            lock (_lock)
            {
                _totalEventCount++;
                RowsToBeUpdated.Remove(primaryKey);
                RowsToBeDeleted.Remove(primaryKey);
                RowsToBeInserted[primaryKey] = row;
            }
        }

        public void AddUpdate(string primaryKey, Row row)
        {
            lock (_lock)
            {
                _totalEventCount++;
                RowsToBeDeleted.Remove(primaryKey);
                RowsToBeUpdated[primaryKey] = row;
            }
        }

        public void AddDelete(string primaryKey, Row row)
        {
            lock (_lock)
            {
                _totalEventCount++;
                RowsToBeInserted.Remove(primaryKey);
                RowsToBeUpdated.Remove(primaryKey);
                RowsToBeDeleted[primaryKey] = row;
            }
        }

        public void UpdateContinuationToken(
            string token, DateTime timestamp)
        {
            lock (_lock)
            {
                if (timestamp >= LatestTimestamp)
                {
                    LatestContinuationToken = token;
                    LatestTimestamp = timestamp;
                }
            }
        }

        public bool Reset(bool isFinalFlush = true)
        {
            lock (_lock)
            {
                RowsToBeInserted.Clear();
                RowsToBeUpdated.Clear();
                RowsToBeDeleted.Clear();

                if (isFinalFlush)
                {
                    _totalEventCount = 0;
                    LatestContinuationToken = string.Empty;
                    LatestTimestamp = DateTime.MinValue;
                }
                return true;
            }
        }

        public void ClearMetadata()
        {
            lock (_lock)
            {
                LatestContinuationToken = string.Empty;
                LatestTimestamp = DateTime.MinValue;
            }
        }
    }
}
