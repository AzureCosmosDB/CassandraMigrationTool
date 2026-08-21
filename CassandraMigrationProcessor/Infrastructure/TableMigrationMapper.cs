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
        if (unit.ParentJob == null)
            throw new InvalidOperationException(
                $"Migration unit '{unit.KeyspaceName}.{unit.TableName}' has no parent job.");

        lock (_updateParentLock)
        {
            var index = unit.ParentJob.Tables
                .FindIndex(mu => mu.Id == unit.Id);
            if (index == -1)
                throw new InvalidOperationException(
                    $"Migration unit '{unit.KeyspaceName}.{unit.TableName}' is missing from its parent job.");

            var target = unit.ParentJob.Tables[index];
            // Flush-and-reset the per-batch accumulator at the
            // explicit sync boundary, then surface the unit via
            // ToSummary. Only overwrite the sticky "last flushed
            // batch" when this flush actually drained fresh
            // activity (flushed > 0); idle ticks preserve the
            // previous sticky value so the dashboard does not zero
            // the column between UI renders while replay is
            // actively applying rows.
            long flushed = Interlocked.Exchange(
                ref unit._changeFeedUpdatesInLastBatch, 0);
            if (flushed > 0)
                Interlocked.Exchange(
                    ref unit._changeFeedLastFlushedBatch, flushed);
            ToSummary(unit, target);
        }
        return true;
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
            Volatile.Read(ref unit._changeFeedLastFlushedBatch);
        target.ChangeFeedAvgReadLatencyInMS =
            unit.ChangeFeedAvgReadLatencyInMS;
        target.ChangeFeedAvgWriteLatencyInMS =
            unit.ChangeFeedAvgWriteLatencyInMS;
        // Cumulative replay counters: snapshot the internal Interlocked
        // fields so the dashboard sees a consistent, monotonically-
        // increasing post-bulk count. Insert-only pipeline today —
        // every replay write lands in _changeFeedRowsInserted
        // regardless of whether the source operation was a true insert
        // or an upsert. Distinguishing requires Full-Fidelity Change
        // Feed (currently reserved on the model).
        target.ChangeFeedRowsInserted =
            Volatile.Read(ref unit._changeFeedRowsInserted);
        target.ChangeFeedInsertEvents =
            Volatile.Read(ref unit._changeFeedInsertEvents);
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
