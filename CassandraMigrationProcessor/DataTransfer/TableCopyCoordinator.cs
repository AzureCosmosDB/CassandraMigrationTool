using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System.Diagnostics;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Per-table drain coordinator. All partitioning, schema lookup and
/// row-count gathering happen up front in
/// <see cref="MigrationJobRunner.DiscoverPartitioningAsync"/> and the
/// resulting <see cref="TablePartitioning"/> objects are handed in here.
/// The coordinator does not touch the partition pool, does not own
/// Cassandra sessions, and does no DDL — it just walks each chunk's
/// <see cref="TableResources.BulkDrainSignal"/>, finalises chunk state,
/// and rolls the table-wide status up.
/// </summary>
internal sealed class TableCopyCoordinator : IDisposable
{
    private readonly MigrationLog _migrationLog;
    private readonly Job _migrationJob;
    private readonly CancellationTokenSource _cts;
    private readonly JobPipeline _jobPipeline;
    private readonly IReadOnlyList<TablePartitioning> _chunks;

    public TableCopyCoordinator(
        MigrationLog log,
        Job job,
        JobPipeline pipeline,
        IReadOnlyList<TablePartitioning> chunks,
        CancellationToken externalToken)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(chunks);

        _migrationLog = log;
        _migrationJob = job;
        _jobPipeline = pipeline;
        _chunks = chunks;
        _cts = externalToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(externalToken)
            : new CancellationTokenSource();
    }

    public void Cancel() => _cts?.Cancel();

    public async Task<TaskResult> MigrateTableAsync(TableMigration tableMigration)
    {
        ArgumentNullException.ThrowIfNull(tableMigration);
        tableMigration.ParentJob = _migrationJob;
        return await MigrateTableCoreAsync(tableMigration);
    }

    private async Task<TaskResult> MigrateTableCoreAsync(TableMigration tableMigration)
    {
        bool isOnline = _migrationJob.IsOnline;
        if (tableMigration.CopyComplete && !isOnline)
        {
            _migrationLog.WriteLine($"Copy for {tableMigration.KeyspaceName}.{tableMigration.TableName} already completed.", LogType.Debug);
            return TaskResult.Success;
        }

        if (_chunks.Count == 0)
        {
            // Source table missing or no work — discovery already logged
            // the reason and updated SourceStatus.
            return TaskResult.Success;
        }

        _migrationLog.WriteLine($"{tableMigration.KeyspaceName}.{tableMigration.TableName} Copy started", LogType.Info);

        foreach (var chunk in _chunks)
        {
            _cts.Token.ThrowIfCancellationRequested();

            var chunkResult = await ProcessChunkAsync(tableMigration, chunk);
            if (chunkResult == TaskResult.Canceled)
                return TaskResult.Canceled;
            if (chunkResult == TaskResult.Abort)
            {
                _migrationLog.WriteLine(
                    $"Copy failed for {tableMigration.KeyspaceName}.{tableMigration.TableName}.",
                    LogType.Error);
                return chunkResult;
            }
        }

        if (_cts.Token.IsCancellationRequested)
            return TaskResult.Canceled;

        long failed = tableMigration.TargetFailedRowCount;
        bool allChunksSucceeded = failed <= 0 && tableMigration.BulkDownloaded == true;
        if (allChunksSucceeded)
        {
            tableMigration.BulkCopyEndedOn = DateTime.UtcNow;
            tableMigration.BulkCopyPhase = BulkCopyPhase.Completed;
            tableMigration.CopyPercent = 100;
            tableMigration.CopyComplete = true;
            TableMigrationMapper.UpdateParentJob(tableMigration);
            MigrationJobContext.Instance.SaveMigrationUnit(tableMigration, true);
            if (!isOnline)
                MigrationJobContext.Instance.MigrationUnitsCache.RemoveMigrationUnit(tableMigration.Id);
        }
        else if (!isOnline)
        {
            _migrationLog.WriteLine(
                $"Copy for {tableMigration.KeyspaceName}.{tableMigration.TableName} had {failed} failed row(s). Job will retry on resume.",
                LogType.Error);
            return TaskResult.Retry;
        }

        return TaskResult.Success;
    }

    private async Task<TaskResult> ProcessChunkAsync(TableMigration tableMigration, TablePartitioning chunk)
    {
        if (chunk.AllRangesAlreadyComplete)
        {
            MarkBulkDownloaded(tableMigration);
            chunk.Resources.Tracker.UpdateMigrationUnit();
            return TaskResult.Success;
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            await chunk.Resources.BulkDrainSignal.Task.WaitAsync(_cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Distinguish fatal-driven cancel (worker tripped FatalErrorFlag,
            // which cascaded into our CTS via JobControlFlags.TriggerFatalShutdown)
            // from user-initiated cancel — the customer needs to see Abort,
            // not "paused", when a worker has failed the job.
            if (Volatile.Read(ref _jobPipeline.Context.Flags.FatalErrorFlag) != 0)
                return TaskResult.Abort;
            return TaskResult.Canceled;
        }
        stopwatch.Stop();

        var tracker = chunk.Resources.Tracker;
        tracker.LogFinal();
        tracker.UpdateMigrationUnit();
        MigrationJobContext.Instance.SaveMigrationUnit(tableMigration, true);

        long sessionWritten = tracker.TotalCopied - tableMigration.CopyRowsCopied;
        _migrationLog.WriteLine(
            $"Bulk drained for {tableMigration.KeyspaceName}.{tableMigration.TableName}: " +
            $"session={sessionWritten:N0} written, {tracker.TotalFailed:N0} failed " +
            $"({stopwatch.Elapsed.TotalSeconds:F1}s)", LogType.Info);

        var result = DetermineOutcome(_jobPipeline.Context.Flags, tracker.TotalFailed);
        if (result == TaskResult.Success)
            MarkBulkDownloaded(tableMigration);
        return result;
    }

    private static TaskResult DetermineOutcome(JobControlFlags flags, long failedCount)
    {
        if (Volatile.Read(ref flags.FatalErrorFlag) != 0)
            return TaskResult.Abort;
        if (flags.WorkerErrors.Any(r => r == TaskResult.Abort))
            return TaskResult.Abort;
        if (flags.WorkerErrors.Any(r => r == TaskResult.Canceled))
            return TaskResult.Canceled;
        return failedCount > 0 ? TaskResult.Retry : TaskResult.Success;
    }

    private void MarkBulkDownloaded(TableMigration tableMigration)
    {
        if (!_cts.Token.IsCancellationRequested)
        {
            tableMigration.BulkDownloaded = true;
        }
        MigrationJobContext.Instance.SaveMigrationUnit(tableMigration, false);
    }

    public void Dispose()
    {
        _cts?.Dispose();
    }
}
