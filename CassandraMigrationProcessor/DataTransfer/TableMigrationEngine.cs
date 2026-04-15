using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.DataTransfer.BulkCopy;
using CassandraMigrationProcessor.DataTransfer.ChangeFeed;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;
/// <summary>
/// Orchestrates bulk copy for each table: session lifecycle,
/// chunk retry loop, and the per-chunk pipeline
/// (count → discover → seed → schema → execute → finalize).
/// </summary>
public class TableMigrationEngine : IDisposable
{
    private readonly MigrationLog _migrationLog;
    private readonly Job _migrationJob;
    private readonly PipelineConfig _pipelineConfig;
    private readonly ISession _source;
    private readonly ISession _target;
    private readonly CancellationTokenSource _cts;
    private readonly ChangeFeedManager _changeFeedManager;

    public volatile bool ProcessRunning;

    public ChangeFeedManager ChangeFeed => _changeFeedManager;

    public TableMigrationEngine(MigrationLog log, AppSettings config, Job job,
        TokenRefreshManager? tokenRefreshManager = null,
        CancellationToken externalToken = default)
    {
        _migrationLog = log ?? throw new ArgumentNullException(nameof(log));
        _migrationJob = job ?? throw new ArgumentNullException(nameof(job));
        _pipelineConfig = PipelineConfig.Resolve(job, config ?? throw new ArgumentNullException(nameof(config)));
        _cts = externalToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(externalToken)
            : new CancellationTokenSource();
        _source = CassandraClientFactory.CreateSourceSession(log, job, string.Empty, tokenRefreshManager);
        _target = job.IsSimulatedRun
            ? new NullSession()
            : CassandraClientFactory.CreateTargetSession(log, job, string.Empty);
        _changeFeedManager = new ChangeFeedManager(log, job, config, _target, tokenRefreshManager);
    }

    // ── Lifecycle ──

    /// <summary>Stops the migration and resets job status to Pending.</summary>
    public void StopProcessing()
    {
        if (_migrationJob.Status == JobStatus.Running)
            _migrationJob.Status = JobStatus.Pending;
        Shutdown();
    }

    /// <summary>Pauses the migration and persists current state.</summary>
    public void PauseProcessing()
    {
        _migrationJob.Status = JobStatus.Paused;
        Shutdown();
    }

    private void Shutdown()
    {
        _cts?.Cancel();
        _changeFeedManager.Stop();
        MigrationJobContext.Instance.SaveMigrationJob(_migrationJob);
        ProcessRunning = false;
    }

    // ── Change Feed ──

    public void FinalizeOrStartChangeFeed()
    {
        if (!MigrationUtilities.IsOnline(_migrationJob)
            && MigrationUtilities.IsOfflineJobCompleted(_migrationJob))
        {
            if (!MigrationJobContext.Instance.ControlledPauseRequested
                && _migrationJob.Status != JobStatus.Cancelled
                && _migrationJob.Status != JobStatus.Paused)
            {
                _migrationLog.WriteLine($"Job {_migrationJob.Id} Completed", LogType.Info);
                _migrationJob.Status = JobStatus.Completed;
                MigrationJobContext.Instance.SaveMigrationJob(_migrationJob);
            }
            StopProcessing();
        }
        else if (!MigrationJobContext.Instance.ControlledPauseRequested)
        {
            _migrationLog.WriteLine("Invoke RunChangeFeedForAllTables.", LogType.Debug);
            _changeFeedManager.StartAll(_cts.Token);
        }
    }

    // ── Job Orchestration ──

    /// <summary>Runs the bulk-copy pipeline for the specified migration unit.</summary>
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

        if (tableMigration.CopyComplete)
        {
            _migrationLog.WriteLine($"Copy for {context.KeyspaceName}.{context.TableName} already completed.", LogType.Debug);
            return TaskResult.Success;
        }

        _migrationLog.WriteLine($"{context.KeyspaceName}.{context.TableName} Copy started", LogType.Info);

        bool hasWorkRemaining = !tableMigration.CopyComplete && !_cts.Token.IsCancellationRequested;
        if (hasWorkRemaining)
        {
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

                if (tableMigration.CopyChunks[chunkIndex].IsDownloaded != true)
                {
                    TaskResult result = await new RetryHelper().ExecuteWithRetryAsync(
                            () => ProcessChunkAsync(tableMigration, chunkIndex, context, initialPercent, contributionFactor),
                            (ex, _, _) => HandleChunkException(ex),
                            _migrationLog, ct: _cts.Token);

                    if (result == TaskResult.Canceled)
                    {
                        _migrationLog.WriteLine($"Copy paused for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Info);
                        PauseProcessing();
                        return TaskResult.Canceled;
                    }

                    bool isUnrecoverableFailure = result == TaskResult.Abort || result == TaskResult.FailedAfterRetries;
                    if (isUnrecoverableFailure)
                    {
                        _migrationLog.WriteLine($"Copy failed for {context.KeyspaceName}.{context.TableName}[{chunkIndex}] after retries.", LogType.Error);
                        StopProcessing();
                        return result;
                    }
                }
            }

            if (MigrationJobContext.Instance.ControlledPauseRequested)
            {
                _migrationLog.WriteLine("Controlled pause - exiting", LogType.Debug);
                PauseProcessing();
                return TaskResult.Success;
            }

            tableMigration.SourceCountDuringCopy = tableMigration.CopyChunks.Sum(c => c.SourceQueryRowCount);
            long failed = tableMigration.CopyChunks.Sum(c => c.TargetFailedRowCount);

            bool allChunksSucceeded = failed <= 0 && tableMigration.CopyChunks.All(c => c.IsDownloaded == true);
            if (allChunksSucceeded)
            {
                tableMigration.BulkCopyEndedOn = DateTime.UtcNow;
                tableMigration.CopyPercent = 100;
                tableMigration.CopyComplete = true;
                TableMigrationMapper.UpdateParentJob(tableMigration);

                await _changeFeedManager.AddTable(tableMigration, _cts.Token);
                MigrationJobContext.Instance.SaveMigrationUnit(tableMigration, true);

                if (!MigrationUtilities.IsOnline(_migrationJob))
                    MigrationJobContext.Instance.MigrationUnitsCache.RemoveMigrationUnit(tableMigration.Id);
            }
            else
            {
                _migrationLog.WriteLine($"Copy for {context.KeyspaceName}.{context.TableName} had {failed} failed row(s). Job will retry on resume.", LogType.Error);
                return TaskResult.Retry;
            }
        }

        return TaskResult.Success;
    }

    // ── Per-Chunk Pipeline ──

    private async Task<TaskResult> ProcessChunkAsync(TableMigration tableMigration, int chunkIndex,
        TableContext context, double initialPercent, double contributionFactor)
    {
        // Count rows
        long rowCount = await CassandraQueries.GetRowCountAsync(context.SourceSession, context.KeyspaceName,
            context.TableName);

        if (rowCount > 0)
        {
            tableMigration.EstimatedRowCount = rowCount;
            TableMigrationMapper.UpdateParentJob(tableMigration);
        }

        tableMigration.CopyChunks[chunkIndex].SourceQueryRowCount = rowCount;

        // Ensure keyspace
        await SchemaManager.EnsureKeyspaceExistsAsync(_target, context.TargetKeyspaceName);

        if (_migrationJob.IsSimulatedRun)
        {
            _migrationLog.WriteLine($"Simulated: {context.KeyspaceName}.{context.TableName}", LogType.Info);
            return TaskResult.Success;
        }

        // Discover + Seed → Schema → Execute → Finalize
        var seeder = new PartitionSeeder(_migrationLog);
        var (seedResult, allComplete) = await seeder.DiscoverAndSeedAsync(
            context.SourceSession, tableMigration, context);
        if (allComplete)
        {
            MarkChunkComplete(tableMigration, chunkIndex);
            return TaskResult.Success;
        }

        var seed = seedResult ?? throw new InvalidOperationException(
            "Seeder returned null result when ranges are still pending");

        _migrationLog.WriteLine($"{context.KeyspaceName}.{context.TableName}: " +
            $"{(rowCount >= 0 ? $"{rowCount:N0} rows" : "count unavailable")}, " +
            $"{seed.FeedRanges.Count} feed range(s)", LogType.Info);

        var request = new PipelineRequest(tableMigration, chunkIndex, initialPercent,
            contributionFactor, rowCount, context, seed.FeedRanges);

        var migrator = new SchemaMigrator(_migrationLog);
        var schema = await migrator.SyncAsync(context.SourceSession, _target, context);
        if (schema == null) return TaskResult.Abort;

        var executor = new WorkerExecutor(_migrationLog, _migrationJob, _pipelineConfig, _cts.Token);
        var execution = await executor.ExecuteAsync(request, seed, schema, _target);
        var result = executor.Finalize(execution, request);

        // Post-pipeline bookkeeping
        if (result == TaskResult.Success)
        {
            MarkChunkComplete(tableMigration, chunkIndex);
        }
        else if (result == TaskResult.Canceled)
        {
            _migrationLog.WriteLine($"Copy paused for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Info);
        }
        else
        {
            _migrationLog.WriteLine($"Copy failed for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Error);
        }
        return result;
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

    // ── Helpers ──

    private TableContext CreateTableContext(TableMigration mu)
    {
        return new TableContext(
            mu.KeyspaceName,
            mu.TableName,
            mu.GetEffectiveTargetKeyspaceName(),
            mu.GetEffectiveTargetTableName(),
            _source);
    }

    private static Task<TaskResult> HandleChunkException(Exception ex)
    {
        return Task.FromResult(ex is OperationCanceledException ? TaskResult.Abort : TaskResult.Retry);
    }

    public void Dispose()
    {
        _changeFeedManager?.Dispose();
        _cts?.Dispose();
        MigrationUtilities.SafeDispose(_target, "TableMigrationEngine target session");
        MigrationUtilities.SafeDispose(_source, "TableMigrationEngine source session");
    }
}
