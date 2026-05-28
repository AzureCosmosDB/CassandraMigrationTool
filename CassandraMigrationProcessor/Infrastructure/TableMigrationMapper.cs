using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Projects a full <see cref="TableMigration"/> down to the lightweight
/// <see cref="TableMigrationSummary"/> stored on the parent <see cref="Job"/>,
/// and keeps the embedded summary in sync when the unit changes.
/// </summary>
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

                var target = unit.ParentJob.Tables[index];
                ToSummary(unit, target);
                // Flush-and-reset the per-batch counter at the explicit
                // sync boundary, not inside ToSummary. Read-only callers
                // (e.g. building an initial summary) should not zero a
                // live counter as a side effect of "looking".
                target.ChangeFeedUpdatesInLastBatch =
                    Interlocked.Exchange(
                        ref unit._changeFeedUpdatesInLastBatch, 0);
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

        target.Id = TableMigration.GenerateId(
            unit.KeyspaceName, unit.TableName);
        target.JobId = unit.JobId;
        target.KeyspaceName = unit.KeyspaceName;
        target.TableName = unit.TableName;
        target.TargetKeyspaceName = unit.TargetKeyspaceName;
        target.TargetTableName = unit.TargetTableName;
        target.ChangeFeedUpdatesInLastBatch =
            Volatile.Read(ref unit._changeFeedUpdatesInLastBatch);
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
