using CassandraMigrationProcessor.Models;
using System;
using System.Threading;

namespace CassandraMigrationProcessor.Infrastructure
{
    public static class TableMigrationMapper
    {
        private static readonly object _updateParentLock = new();

        public static bool UpdateParentJob(TableMigration unit)
        {
            if (unit.ParentJob == null) return false;

            try
            {
                lock (_updateParentLock)
                {
                    var index = unit.ParentJob.Tables
                        .FindIndex(mu => mu.Id == unit.Id);
                    if (index == -1) return false;

                    ToSummary(unit, unit.ParentJob.Tables[index]);
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static TableMigrationSummary ToSummary(
            TableMigration unit, TableMigrationSummary? target = null)
        {
            if (target == null)
                target = new TableMigrationSummary();

            target.Id = MigrationUtilities.GenerateMigrationUnitId(
                unit.KeyspaceName, unit.TableName);
            target.JobId = unit.JobId;
            target.KeyspaceName = unit.KeyspaceName;
            target.TableName = unit.TableName;
            target.TargetKeyspaceName = unit.TargetKeyspaceName;
            target.TargetTableName = unit.TargetTableName;
            target.ChangeFeedUpdatesInLastBatch =
                Interlocked.Exchange(
                    ref unit._changeFeedUpdatesInLastBatch, 0);
            target.ChangeFeedAvgReadLatencyInMS =
                unit.ChangeFeedAvgReadLatencyInMS;
            target.ChangeFeedAvgWriteLatencyInMS =
                unit.ChangeFeedAvgWriteLatencyInMS;
            target.CopyPercent = unit.CopyPercent;
            target.CopyComplete = unit.CopyComplete;
            target.CopyRowsCopied = unit.CopyRowsCopied;
            target.CopyRowsPerSecond = unit.CopyRowsPerSecond;
            target.TotalRowCount = Math.Max(
                unit.EstimatedRowCount, unit.ActualRowCount);
            target.SourceStatus = unit.SourceStatus;
            target.SkippedDueToMaxRetries =
                unit.SkippedDueToMaxRetries;
            target.FailedOperation = unit.FailedOperation;
            target.ChangeFeedLastChecked =
                unit.ChangeFeedLastChecked;
            return target;
        }
    }
}
