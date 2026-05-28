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
/// Prepares a single table for the job-shared <see cref="JobPipeline"/>:
/// counts rows, syncs schema, discovers feed ranges, builds
/// <see cref="TableResources"/>, and seeds partitions into the shared
/// channel. Then awaits the table's drain signal so the caller can mark
/// the table CopyComplete.
/// </summary>
public class TableMigrationEngine : IDisposable
{
    private readonly MigrationLog _migrationLog;
    private readonly Job _migrationJob;
    private readonly PipelineConfig _pipelineConfig;
    private readonly ISession _source;
    private readonly ISession _target;
    private readonly CancellationTokenSource _cts;
    private readonly object _pipelineRef;  // JobPipeline (internal type)

    public volatile bool ProcessRunning;

    private TableMigrationEngine(MigrationLog log, AppSettings config, Job job,
        ISession source, ISession target,
        object pipeline,
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
        _pipelineRef = pipeline;
    }

    internal static async Task<TableMigrationEngine> CreateAsync(
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
        return new TableMigrationEngine(log, config, job, source, target, pipeline, externalToken);
    }

    public void StopProcessing()
    {
        if (_migrationJob.Status == JobStatus.Running)
            _migrationJob.Status = JobStatus.Pending;
        Shutdown();
    }

    public void PauseProcessing()
    {
        _migrationJob.Status = JobStatus.Paused;
        Shutdown();
    }

    private void Shutdown()
    {
        _cts?.Cancel();
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

        var context = CreateTableContext(tableMigration);

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
                {
                    PauseProcessing();
                    return TaskResult.Canceled;
                }
                if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries)
                {
                    _migrationLog.WriteLine($"Copy failed for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Error);
                    StopProcessing();
                    return result;
                }
            }
        }

        if (MigrationJobContext.Instance.ControlledPauseRequested)
        {
            PauseProcessing();
            return TaskResult.Success;
        }

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
        TableContext context, double initialPercent, double contributionFactor)
    {
        long rowCount = await CassandraQueries.GetRowCountAsync(context.SourceSession, context.KeyspaceName, context.TableName);
        if (rowCount > 0)
        {
            tableMigration.EstimatedRowCount = rowCount;
            TableMigrationMapper.UpdateParentJob(tableMigration);
        }
        tableMigration.CopyChunks[chunkIndex].SourceQueryRowCount = rowCount;

        await SchemaManager.EnsureKeyspaceExistsAsync(_target, context.TargetKeyspaceName);
        if (_migrationJob.IsSimulatedRun)
        {
            _migrationLog.WriteLine($"Simulated: {context.KeyspaceName}.{context.TableName}", LogType.Info);
            return TaskResult.Success;
        }

        var migrator = new SchemaMigrator(_migrationLog);
        var schema = await migrator.SyncAsync(context.SourceSession, _target, context);
        if (schema == null) return TaskResult.Abort;

        var pipeline = (JobPipeline)_pipelineRef;
        bool isOnline = MigrationUtilities.IsOnline(_migrationJob);

        var tracker = new CopyProgressTracker(_migrationLog,
            tableMigration.CopyRowsCopied, tableMigration,
            new ProgressConfig(chunkIndex, initialPercent, contributionFactor, rowCount));

        var seeder = new PartitionSeeder(_migrationLog);
        var seed = await seeder.DiscoverAndSeedAsync(
            context.SourceSession, tableMigration, context,
            schema.Columns, tracker, pipeline.Context.Partitions,
            enableReplay: isOnline);

        if (seed.AllRangesComplete)
        {
            MarkChunkComplete(tableMigration, chunkIndex);
            tracker.UpdateMigrationUnit();
            return TaskResult.Success;
        }

        _migrationLog.WriteLine($"{context.KeyspaceName}.{context.TableName}: " +
            $"{(rowCount >= 0 ? $"{rowCount:N0} rows" : "count unavailable")}, " +
            $"{seed.Resources.Ranges.FeedRanges.Count} feed range(s) seeded", LogType.Info);

        var stopwatch = Stopwatch.StartNew();

        // Wait for this table's bulk drain.
        try
        {
            await seed.Resources.BulkDrainSignal.Task.WaitAsync(_cts.Token);
        }
        catch (OperationCanceledException)
        {
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

        var result = DetermineOutcome(pipeline.Context.Counters, tracker.TotalFailed);
        if (result == TaskResult.Success)
            MarkChunkComplete(tableMigration, chunkIndex);
        return result;
    }

    private static TaskResult DetermineOutcome(PipelineCounters counters, long failedCount)
    {
        if (Volatile.Read(ref counters.FatalErrorFlag) != 0)
            return TaskResult.Abort;
        if (counters.WorkerErrors.Any(r => r == TaskResult.Abort))
            return TaskResult.Abort;
        if (counters.WorkerErrors.Any(r => r == TaskResult.Canceled))
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

    private TableContext CreateTableContext(TableMigration mu)
    {
        return new TableContext(
            mu.KeyspaceName,
            mu.TableName,
            mu.GetEffectiveTargetKeyspaceName(),
            mu.GetEffectiveTargetTableName(),
            _source);
    }

    public void Dispose()
    {
        _cts?.Dispose();
        MigrationUtilities.SafeDispose(_target, "TableMigrationEngine target session");
        MigrationUtilities.SafeDispose(_source, "TableMigrationEngine source session");
    }
}
