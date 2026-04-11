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

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Orchestrates bulk copy for each table and manages
    /// session lifecycle and cancellation.
    /// </summary>
    public class BulkCopyEngine : IDisposable
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

        public BulkCopyEngine(MigrationLog log, ISession sourceSession, AppSettings config, Job job,
            TokenRefreshManager? tokenRefreshManager = null)
        {
            _migrationLog = log;
            _source = sourceSession;
            _pipelineConfig = PipelineConfig.Resolve(job, config);
            _migrationJob = job;
            _cts = new CancellationTokenSource();
            _target = job.IsSimulatedRun
                ? null
                : CassandraClientFactory.CreateTargetSession(log, job, string.Empty);
            _changeFeedManager = new ChangeFeedManager(log, job, config, _target!, tokenRefreshManager);
        }

        // ── Lifecycle ──

        public void StopProcessing()
        {
            _cts?.Cancel();
            _changeFeedManager.Stop();

            if (_migrationJob.Status == JobStatus.Running)
                _migrationJob.Status = JobStatus.Pending;

            MigrationJobContext.SaveMigrationJob(_migrationJob);
            ProcessRunning = false;
        }

        public void PauseProcessing()
        {
            _cts?.Cancel();
            _changeFeedManager.Stop();

            _migrationJob.Status = JobStatus.Paused;
            MigrationJobContext.SaveMigrationJob(_migrationJob);
            ProcessRunning = false;
        }

        // ── Job completion ──

        public void StopOfflineOrInvokeChangeFeed()
        {
            if (!MigrationUtilities.IsOnline(_migrationJob)
                && MigrationUtilities.IsOfflineJobCompleted(_migrationJob))
            {
                if (!MigrationJobContext.ControlledPauseRequested
                    && _migrationJob.Status != JobStatus.Cancelled
                    && _migrationJob.Status != JobStatus.Paused)
                {
                    _migrationLog.WriteLine($"Job {_migrationJob.Id} Completed", LogType.Info);
                    _migrationJob.Status = JobStatus.Completed;
                    MigrationJobContext.SaveMigrationJob(_migrationJob);
                }
                StopProcessing();
            }
            else if (!MigrationJobContext.ControlledPauseRequested)
            {
                _migrationLog.WriteLine("Invoke RunChangeFeedForAllTables.", LogType.Debug);
                _changeFeedManager.StartAll(_cts.Token);
            }
        }

        // ── Bulk copy orchestration ──

        public async Task<TaskResult> StartProcessAsync(string migrationUnitId)
        {
            var TableMigration = MigrationJobContext.GetMigrationUnit(migrationUnitId);
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
                    if (MigrationJobContext.ControlledPauseRequested)
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

                if (MigrationJobContext.ControlledPauseRequested)
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
                    MigrationJobContext.SaveMigrationUnit(TableMigration, true);

                    if (!MigrationUtilities.IsOnline(_migrationJob))
                        MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(TableMigration.Id);
                }
                else
                {
                    _migrationLog.WriteLine($"Copy for {context.KeyspaceName}.{context.TableName} had failures.", LogType.Error);
                    return TaskResult.Retry;
                }
            }

            return TaskResult.Success;
        }

        private async Task<TaskResult> ProcessChunkAsync(TableMigration TableMigration, int chunkIndex,
            TableContext context, double initialPercent, double contributionFactor)
        {
            long rowCount = await CassandraQueries.GetRowCountAsync(context.SourceSession, context.KeyspaceName,
                context.TableName);

            if (rowCount > 0)
            {
                TableMigration.EstimatedRowCount = rowCount;
                TableMigrationMapper.UpdateParentJob(TableMigration);
            }

            TableMigration.CopyChunks[chunkIndex].SourceQueryRowCount = rowCount;

            if (_target != null)
                await SchemaManager.EnsureKeyspaceExistsAsync(_target, context.TargetKeyspaceName);

            var feedRanges = await CassandraQueries.GetFeedRangesAsync(context.SourceSession, context.KeyspaceName,
                context.TableName, msg => MigrationJobContext.AddVerboseLog(msg));

            _migrationLog.WriteLine($"{context.KeyspaceName}.{context.TableName}: " +
                $"{(rowCount >= 0 ? $"{rowCount:N0} rows" : "count unavailable")}, " +
                $"{feedRanges.Count} feed range(s)", LogType.Info);

            if (_migrationJob.IsSimulatedRun)
            {
                _migrationLog.WriteLine($"Simulated: {context.KeyspaceName}.{context.TableName}", LogType.Info);
                return TaskResult.Success;
            }

            var runner = new BulkCopyRunner(_migrationLog, _migrationJob, _pipelineConfig, _cts.Token, _target!);
            var result = await runner.RunAsync(new PipelineRequest(TableMigration, chunkIndex, initialPercent,
                contributionFactor, rowCount, context, feedRanges));

            if (result == TaskResult.Success)
            {
                if (!_cts.Token.IsCancellationRequested
                    && !MigrationJobContext.ControlledPauseRequested
                    && TableMigration.CopyChunks[chunkIndex].Segments.All(seg => seg.IsProcessed == true))
                {
                    TableMigration.CopyChunks[chunkIndex].IsDownloaded = true;
                    TableMigration.CopyChunks[chunkIndex].IsUploaded = true;
                }
                MigrationJobContext.SaveMigrationUnit(TableMigration, false);
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
            _changeFeedManager.Stop();
            _cts?.Dispose();
            MigrationUtilities.SafeDispose(_target, "BulkCopyEngine target session");
        }
    }
}
