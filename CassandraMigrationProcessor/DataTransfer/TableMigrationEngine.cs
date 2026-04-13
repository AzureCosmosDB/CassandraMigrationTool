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
    private readonly ISession? _target;
    private readonly CancellationTokenSource _cts;
    private readonly ChangeFeedManager _changeFeedManager;

    public volatile bool ProcessRunning;

    public ChangeFeedManager ChangeFeed => _changeFeedManager;

    public TableMigrationEngine(MigrationLog log, ISession sourceSession, AppSettings config, Job job,
        TokenRefreshManager? tokenRefreshManager = null)
    {
        _migrationLog = log ?? throw new ArgumentNullException(nameof(log));
        _source = sourceSession ?? throw new ArgumentNullException(nameof(sourceSession));
        _pipelineConfig = PipelineConfig.Resolve(job ?? throw new ArgumentNullException(nameof(job)),
            config ?? throw new ArgumentNullException(nameof(config)));
        _migrationJob = job;
        _cts = new CancellationTokenSource();
        _target = job.IsSimulatedRun
            ? null
            : CassandraClientFactory.CreateTargetSession(log, job, string.Empty);
        var targetForChangeFeed = _target ?? throw new InvalidOperationException(
            "Target session is required for ChangeFeedManager but was not created");
        _changeFeedManager = new ChangeFeedManager(log, job, config, targetForChangeFeed, tokenRefreshManager);
    }

    // ── Lifecycle ──

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
        _changeFeedManager.Stop();
        MigrationJobContext.Instance.SaveMigrationJob(_migrationJob);
        ProcessRunning = false;
    }

    // ── Change Feed ──

    public void StopOfflineOrInvokeChangeFeed()
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

    public async Task<TaskResult> StartProcessAsync(string migrationUnitId)
    {
        if (string.IsNullOrWhiteSpace(migrationUnitId))
            throw new ArgumentException("Migration unit ID is required", nameof(migrationUnitId));

        var TableMigration = MigrationJobContext.Instance.GetMigrationUnit(migrationUnitId);
        TableMigration.ParentJob = _migrationJob;
        ProcessRunning = true;

        var context = CreateTableContext(TableMigration);

        if (TableMigration.CopyComplete)
        {
            _migrationLog.WriteLine($"Copy for {context.KeyspaceName}.{context.TableName} already completed.", LogType.Debug);
            return TaskResult.Success;
        }

        _migrationLog.WriteLine($"{context.KeyspaceName}.{context.TableName} Copy started", LogType.Info);

        if (!TableMigration.CopyComplete && !_cts.Token.IsCancellationRequested)
        {
            if (TableMigration.CopyChunks == null || TableMigration.CopyChunks.Count == 0)
                TableMigration.CopyChunks = new List<CopyChunk> { new CopyChunk() };

            for (int chunkIndex = 0; chunkIndex < TableMigration.CopyChunks.Count; chunkIndex++)
            {
                if (MigrationJobContext.Instance.ControlledPauseRequested)
                {
                    _migrationLog.WriteLine($"Controlled pause before chunk {chunkIndex}", LogType.Info);
                    break;
                }

                _cts.Token.ThrowIfCancellationRequested();

                double initialPercent = ((double)100 / TableMigration.CopyChunks.Count) * chunkIndex;
                double contributionFactor = 1.0 / TableMigration.CopyChunks.Count;

                if (TableMigration.CopyChunks[chunkIndex].IsDownloaded != true)
                {
                    TaskResult result = await new RetryHelper().ExecuteTask(
                            () => ProcessChunkAsync(TableMigration, chunkIndex, context, initialPercent, contributionFactor),
                            (ex, _, _) => HandleChunkException(ex),
                            _migrationLog, ct: _cts.Token);

                    if (result == TaskResult.Canceled)
                    {
                        _migrationLog.WriteLine($"Copy paused for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Info);
                        PauseProcessing();
                        return TaskResult.Canceled;
                    }

                    if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries)
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

            TableMigration.SourceCountDuringCopy = TableMigration.CopyChunks.Sum(c => c.SourceQueryRowCount);
            long failed = TableMigration.CopyChunks.Sum(c => c.TargetFailedRowCount);

            if (failed <= 0 && TableMigration.CopyChunks.All(c => c.IsDownloaded == true))
            {
                TableMigration.BulkCopyEndedOn = DateTime.UtcNow;
                TableMigration.CopyPercent = 100;
                TableMigration.CopyComplete = true;
                TableMigrationMapper.UpdateParentJob(TableMigration);

                await _changeFeedManager.AddTable(TableMigration, _cts.Token);
                MigrationJobContext.Instance.SaveMigrationUnit(TableMigration, true);

                if (!MigrationUtilities.IsOnline(_migrationJob))
                    MigrationJobContext.Instance.MigrationUnitsCache.RemoveMigrationUnit(TableMigration.Id);
            }
            else
            {
                _migrationLog.WriteLine($"Copy for {context.KeyspaceName}.{context.TableName} had failures.", LogType.Error);
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
        if (_target != null)
            await SchemaManager.EnsureKeyspaceExistsAsync(_target, context.TargetKeyspaceName);

        if (_migrationJob.IsSimulatedRun)
        {
            _migrationLog.WriteLine($"Simulated: {context.KeyspaceName}.{context.TableName}", LogType.Info);
            return TaskResult.Success;
        }

        var target = _target ?? throw new InvalidOperationException(
            "Target session not initialized for non-simulated run");

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
        var schema = await migrator.SyncAsync(context.SourceSession, target, context);
        if (schema == null) return TaskResult.Abort;

        var executor = new WorkerExecutor(_migrationLog, _migrationJob, _pipelineConfig, _cts.Token);
        var execution = await executor.ExecuteAsync(request, seed, schema, target);
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
    }
}
