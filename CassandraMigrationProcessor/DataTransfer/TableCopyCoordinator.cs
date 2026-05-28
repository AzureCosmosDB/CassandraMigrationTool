using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.DataTransfer;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Coordinates the copy of a single table on top of the job-shared
/// <see cref="JobPipeline"/>. Does no copying itself: workers in the
/// shared pool drain partitions. Per-table responsibilities are
/// opening per-table sessions, fetching the source column list,
/// slicing the source token range into <see cref="Partition"/>
/// work items (<see cref="Partitioner"/>), seeding them into the
/// shared <see cref="PartitionManager"/>, waiting for the table's
/// own slice to drain, and marking the table CopyComplete.
/// Destination schema provisioning is owned by
/// <see cref="MigrationJobRunner"/> and has completed before
/// <see cref="MigrateTableAsync"/> is invoked.
/// </summary>
internal sealed class TableCopyCoordinator : IDisposable
{
    private readonly MigrationLog _migrationLog;
    private readonly Job _migrationJob;
    private readonly PipelineConfig _pipelineConfig;
    private readonly ISession _source;
    private readonly ISession _target;
    private readonly CancellationTokenSource _cts;
    private readonly JobPipeline _pipeline;

    public volatile bool ProcessRunning;

    private TableCopyCoordinator(MigrationLog log, AppSettings config, Job job,
        ISession source, ISession target,
        JobPipeline pipeline,
        CancellationToken externalToken)
    {
        _migrationLog = log;
        _migrationJob = job;
        _pipelineConfig = PipelineConfig.Resolve(job, config);
        _cts = externalToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(externalToken)
            : new CancellationTokenSource();
        _source = source;
        _target = target;
        _pipeline = pipeline;
    }

    internal static async Task<TableCopyCoordinator> CreateAsync(
        MigrationLog log, AppSettings config, Job job,
        JobPipeline pipeline,
        TokenRefreshManager? tokenRefreshManager = null,
        CancellationToken externalToken = default)
    {
        if (log == null) throw new ArgumentNullException(nameof(log));
        if (job == null) throw new ArgumentNullException(nameof(job));
        if (config == null) throw new ArgumentNullException(nameof(config));

        var source = CassandraClientFactory.CreateSourceSession(log, job, string.Empty, tokenRefreshManager);
        ISession target = job.IsSimulatedRun
            ? new NullSession()
            : await CassandraClientFactory.CreateTargetSessionAsync(log, job, string.Empty);
        return new TableCopyCoordinator(log, config, job, source, target, pipeline, externalToken);
    }

    /// <summary>
    /// Idempotent: signal this coordinator to abort. Cancelling the
    /// CTS unblocks the <c>BulkDrainSignal</c> wait in
    /// <see cref="ProcessChunkAsync"/>, so <see cref="MigrateTableAsync"/>
    /// throws <see cref="OperationCanceledException"/> and runs its
    /// natural exit path (which sets job status + saves).
    /// </summary>
    public void Cancel() => _cts?.Cancel();

    private void FinalizeStatus(TaskResult result)
    {
        switch (result)
        {
            case TaskResult.Canceled:
                _migrationJob.Status = JobStatus.Paused;
                break;
            case TaskResult.Abort:
            case TaskResult.FailedAfterRetries:
                if (_migrationJob.Status == JobStatus.Running)
                    _migrationJob.Status = JobStatus.Pending;
                break;
        }
        MigrationJobContext.Instance.SaveMigrationJob(_migrationJob);
        ProcessRunning = false;
    }

    public async Task<TaskResult> MigrateTableAsync(string migrationUnitId)
    {
        if (string.IsNullOrWhiteSpace(migrationUnitId))
            throw new ArgumentException("Migration unit ID is required", nameof(migrationUnitId));

        var tableMigration = MigrationJobContext.Instance.GetMigrationUnit(migrationUnitId);
        tableMigration.ParentJob = _migrationJob;
        ProcessRunning = true;

        var result = TaskResult.Success;
        try
        {
            result = await MigrateTableCoreAsync(tableMigration);
            return result;
        }
        catch (OperationCanceledException)
        {
            result = TaskResult.Canceled;
            throw;
        }
        finally
        {
            FinalizeStatus(result);
        }
    }

    private async Task<TaskResult> MigrateTableCoreAsync(TableMigration tableMigration)
    {
        var context = CreateTableCopySpec(tableMigration);

        if (!await SchemaManager.TableExistsAsync(_source, context.KeyspaceName, context.TableName))
        {
            _migrationLog.WriteLine($"Source table {context.KeyspaceName}.{context.TableName} not found.", LogType.Error);
            tableMigration.SourceStatus = TableStatus.NotFound;
            MigrationJobContext.Instance.SaveMigrationUnit(tableMigration, true);
            return TaskResult.Abort;
        }

        bool isOnline = MigrationUtilities.IsOnline(_migrationJob);
        if (tableMigration.CopyComplete && !isOnline)
        {
            _migrationLog.WriteLine($"Copy for {context.KeyspaceName}.{context.TableName} already completed.", LogType.Debug);
            return TaskResult.Success;
        }

        _migrationLog.WriteLine($"{context.KeyspaceName}.{context.TableName} Copy started", LogType.Info);

        if (tableMigration.CopyChunks == null || tableMigration.CopyChunks.Count == 0)
            tableMigration.CopyChunks = new List<CopyChunk> { new CopyChunk() };

        for (int chunkIndex = 0; chunkIndex < tableMigration.CopyChunks.Count; chunkIndex++)
        {
            if (MigrationJobContext.Instance.ControlledPauseRequested)
            {
                _migrationLog.WriteLine($"Controlled pause before chunk {chunkIndex}", LogType.Info);
                break;
            }
            _cts.Token.ThrowIfCancellationRequested();

            double initialPercent = ((double)100 / tableMigration.CopyChunks.Count) * chunkIndex;
            double contributionFactor = 1.0 / tableMigration.CopyChunks.Count;

            if (tableMigration.CopyChunks[chunkIndex].IsDownloaded != true || isOnline)
            {
                var result = await ProcessChunkAsync(tableMigration, chunkIndex, context, initialPercent, contributionFactor);
                if (result == TaskResult.Canceled)
                    return TaskResult.Canceled;
                if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries)
                {
                    _migrationLog.WriteLine($"Copy failed for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Error);
                    return result;
                }
            }
        }

        if (MigrationJobContext.Instance.ControlledPauseRequested)
            return TaskResult.Canceled;

        tableMigration.SourceCountDuringCopy = tableMigration.CopyChunks.Sum(c => c.SourceQueryRowCount);
        long failed = tableMigration.CopyChunks.Sum(c => c.TargetFailedRowCount);
        bool allChunksSucceeded = failed <= 0 && tableMigration.CopyChunks.All(c => c.IsDownloaded == true);
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
            _migrationLog.WriteLine($"Copy for {context.KeyspaceName}.{context.TableName} had {failed} failed row(s). Job will retry on resume.", LogType.Error);
            return TaskResult.Retry;
        }

        return TaskResult.Success;
    }

    private async Task<TaskResult> ProcessChunkAsync(TableMigration tableMigration, int chunkIndex,
        TableCopySpec context, double initialPercent, double contributionFactor)
    {
        long rowCount = await CassandraQueries.GetRowCountAsync(context.SourceSession, context.KeyspaceName, context.TableName);
        if (rowCount > 0)
        {
            tableMigration.EstimatedRowCount = rowCount;
            TableMigrationMapper.UpdateParentJob(tableMigration);
        }
        tableMigration.CopyChunks[chunkIndex].SourceQueryRowCount = rowCount;

        if (_migrationJob.IsSimulatedRun)
        {
            _migrationLog.WriteLine($"Simulated: {context.KeyspaceName}.{context.TableName}", LogType.Info);
            return TaskResult.Success;
        }

        // Destination schema (keyspace + UDTs + table) was provisioned by
        // MigrationJobRunner.SetupTargetSchemaAsync before the engine ran;
        // here we only fetch the source column list needed to build writers.
        var columns = await SchemaManager.GetTableColumnsAsync(
            context.SourceSession, context.KeyspaceName, context.TableName);
        if (columns.Count == 0)
        {
            _migrationLog.WriteLine($"No columns for {context.KeyspaceName}.{context.TableName}", LogType.Error);
            return TaskResult.Abort;
        }

        bool isOnline = MigrationUtilities.IsOnline(_migrationJob);

        var tracker = new CopyProgressTracker(_migrationLog,
            tableMigration.CopyRowsCopied, tableMigration,
            new ProgressConfig(chunkIndex, initialPercent, contributionFactor, rowCount));

        var partitioner = new Partitioner(_migrationLog);
        var feedRanges = await CassandraQueries.GetFeedRangesAsync(
            context.SourceSession, context.KeyspaceName, context.TableName);
        _migrationLog.WriteLine(
            $"{context.KeyspaceName}.{context.TableName}: {feedRanges.Count} feed range(s)",
            LogType.Info);

        var resources = new TableResources(context, columns, tracker, feedRanges.Count);
        bool allRangesComplete = await partitioner.SeedAsync(
            resources, tableMigration, feedRanges, _pipeline.Context.Partitions,
            enableReplay: isOnline);

        if (allRangesComplete)
        {
            MarkChunkComplete(tableMigration, chunkIndex);
            tracker.UpdateMigrationUnit();
            return TaskResult.Success;
        }

        _migrationLog.WriteLine($"{context.KeyspaceName}.{context.TableName}: " +
            $"{(rowCount >= 0 ? $"{rowCount:N0} rows" : "count unavailable")}, " +
            $"{resources.TotalFeedRanges} feed range(s) seeded", LogType.Info);

        var stopwatch = Stopwatch.StartNew();

        // Wait for this table's bulk drain.
        try
        {
            await resources.BulkDrainSignal.Task.WaitAsync(_cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Distinguish fatal-driven cancel (worker tripped FatalErrorFlag,
            // which cascaded into our CTS via JobControlFlags.TriggerFatalShutdown)
            // from user-initiated cancel — the customer needs to see Abort,
            // not "paused", when a worker has failed the job.
            if (Volatile.Read(ref _pipeline.Context.Flags.FatalErrorFlag) != 0)
                return TaskResult.Abort;
            return TaskResult.Canceled;
        }
        stopwatch.Stop();

        tracker.LogFinal();
        tracker.UpdateMigrationUnit();
        MigrationJobContext.Instance.SaveMigrationUnit(tableMigration, true);

        long sessionWritten = tracker.TotalCopied - tableMigration.CopyRowsCopied;
        _migrationLog.WriteLine(
            $"Bulk drained for {context.KeyspaceName}.{context.TableName}: " +
            $"session={sessionWritten:N0} written, {tracker.TotalFailed:N0} failed " +
            $"({stopwatch.Elapsed.TotalSeconds:F1}s)", LogType.Info);

        var result = DetermineOutcome(_pipeline.Context.Flags, tracker.TotalFailed);
        if (result == TaskResult.Success)
            MarkChunkComplete(tableMigration, chunkIndex);
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

    private void MarkChunkComplete(TableMigration tableMigration, int chunkIndex)
    {
        if (!_cts.Token.IsCancellationRequested
            && !MigrationJobContext.Instance.ControlledPauseRequested
            && tableMigration.CopyChunks[chunkIndex].Segments.All(seg => seg.IsProcessed == true))
        {
            tableMigration.CopyChunks[chunkIndex].IsDownloaded = true;
        }
        MigrationJobContext.Instance.SaveMigrationUnit(tableMigration, false);
    }

    private TableCopySpec CreateTableCopySpec(TableMigration mu)
    {
        return new TableCopySpec(
            mu.KeyspaceName,
            mu.TableName,
            mu.GetEffectiveTargetKeyspaceName(),
            mu.GetEffectiveTargetTableName(),
            _source);
    }

    public void Dispose()
    {
        _cts?.Dispose();
        MigrationUtilities.SafeDisposeSession(_target, "TableCopyCoordinator target session");
        MigrationUtilities.SafeDisposeSession(_source, "TableCopyCoordinator source session");
    }
}
