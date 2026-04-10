using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Orchestrates bulk copy for each table and manages
    /// session lifecycle, cancellation, and change feed replay.
    /// </summary>
    public class BulkCopyEngine : IDisposable
    {
        private readonly ISession _sourceSession;
        private ISession? _targetSession;
        private readonly MigrationSettings _config;
        private CancellationTokenSource _cancellation;
        private readonly MigrationLog _log;
        private readonly MigrationJob _job;
        private readonly MigrationWorker _worker;
        private ReplayProcessor? _changeFeedProcessor;
        private readonly object _changeFeedLock = new();

        public volatile bool ProcessRunning;
        public volatile bool IsChangeFeedRunning;

        public BulkCopyEngine(MigrationLog log, ISession sourceSession, MigrationSettings config, MigrationJob job,
            MigrationWorker worker)
        {
            _log = log;
            _sourceSession = sourceSession;
            _config = config;
            _job = job;
            _cancellation = new CancellationTokenSource();
            _worker = worker;
        }

        // ── Session management ──

        private ISession EnsureTargetSession()
        {
            if (_targetSession == null)
                _targetSession = CassandraClientFactory.CreateTargetSession(_log, _job, string.Empty);
            return _targetSession;
        }

        // ── Lifecycle ──

        public void StopProcessing()
        {
            StopInternal(updateRunning: true, isPause: false);
        }

        public void PauseProcessing()
        {
            StopInternal(updateRunning: true, isPause: true);
        }

        private void StopInternal(bool updateRunning, bool isPause)
        {
            _cancellation?.Cancel();
            IsChangeFeedRunning = false;

            if (_changeFeedProcessor != null)
                _changeFeedProcessor.ExecutionCancelled = true;

            if (isPause)
                _job.Status = JobStatus.Paused;
            else if (_job.Status == JobStatus.Running)
                _job.Status = JobStatus.Pending;

            MigrationJobContext.SaveMigrationJob(_job);

            if (updateRunning)
                ProcessRunning = false;
        }

        public void ResetCancellationToken()
        {
            _cancellation?.Dispose();
            _cancellation = new CancellationTokenSource();
        }

        // ── Table context ──

        private TableContext CreateTableContext(MigrationUnit mu)
        {
            return new TableContext
            {
                MigrationUnitId = mu.Id,
                JobId = _job.Id,
                KeyspaceName = mu.KeyspaceName,
                TableName = mu.TableName,
                TargetKeyspaceName = mu.GetEffectiveTargetKeyspaceName(),
                TargetTableName = mu.GetEffectiveTargetTableName(),
                SourceSession = _sourceSession,
            };
        }

        // ── Bulk copy orchestration ──

        public async Task<TaskResult> StartProcessAsync(string migrationUnitId)
        {
            var migrationUnit = MigrationJobContext.GetMigrationUnit(migrationUnitId);
            migrationUnit.ParentJob = _job;
            ProcessRunning = true;

            var context = CreateTableContext(migrationUnit);

            if (migrationUnit.CopyComplete)
            {
                _log.WriteLine($"Copy for {context.KeyspaceName}.{context.TableName} already completed.", LogType.Debug);
                return TaskResult.Success;
            }

            _log.WriteLine($"{context.KeyspaceName}.{context.TableName} Copy started", LogType.Info);

            if (!migrationUnit.CopyComplete && !_cancellation.Token.IsCancellationRequested)
            {
                if (migrationUnit.MigrationChunks == null || migrationUnit.MigrationChunks.Count == 0)
                    migrationUnit.MigrationChunks = new List<MigrationChunk> { new MigrationChunk() };

                for (int chunkIndex = 0; chunkIndex < migrationUnit.MigrationChunks.Count; chunkIndex++)
                {
                    if (MigrationJobContext.ControlledPauseRequested)
                    {
                        _log.WriteLine($"Controlled pause before chunk {chunkIndex}", LogType.Info);
                        break;
                    }

                    _cancellation.Token.ThrowIfCancellationRequested();

                    double initialPercent = ((double)100 / migrationUnit.MigrationChunks.Count) * chunkIndex;
                    double contributionFactor = 1.0 / migrationUnit.MigrationChunks.Count;

                    if (migrationUnit.MigrationChunks[chunkIndex].IsDownloaded != true)
                    {
                        TaskResult result = await new RetryHelper().ExecuteTask(
                                () => ProcessChunkAsync(migrationUnit, chunkIndex, context, initialPercent, contributionFactor),
                                (ex, _, _) => HandleChunkException(ex),
                                _log, ct: _cancellation.Token);

                        if (result == TaskResult.Canceled)
                        {
                            _log.WriteLine($"Copy paused for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Info);
                            PauseProcessing();
                            return TaskResult.Canceled;
                        }

                        if (result == TaskResult.Abort || result == TaskResult.FailedAfterRetries)
                        {
                            _log.WriteLine($"Copy failed for {context.KeyspaceName}.{context.TableName}[{chunkIndex}] after retries.", LogType.Error);
                            StopProcessing();
                            return result;
                        }
                    }
                    else
                    {
                        context.DownloadCount += migrationUnit.MigrationChunks[chunkIndex].SourceQueryRowCount;
                    }
                }

                if (MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine("Controlled pause - exiting", LogType.Debug);
                    PauseProcessing();
                    return TaskResult.Success;
                }

                migrationUnit.SourceCountDuringCopy = migrationUnit.MigrationChunks.Sum(c => c.SourceQueryRowCount);
                long failed = migrationUnit.MigrationChunks.Sum(c => c.TargetFailedRowCount);

                if (failed <= 0 && migrationUnit.MigrationChunks.All(c => c.IsDownloaded == true))
                {
                    migrationUnit.BulkCopyEndedOn = DateTime.UtcNow;
                    migrationUnit.CopyPercent = 100;
                    migrationUnit.CopyComplete = true;
                    migrationUnit.UpdateParentJob();

                    AddTableToChangeFeedQueue(migrationUnit);
                    MigrationJobContext.SaveMigrationUnit(migrationUnit, true);

                    if (!MigrationUtilities.IsOnline(_job))
                        MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(migrationUnit.Id);
                }
                else
                {
                    _log.WriteLine($"Copy for {context.KeyspaceName}.{context.TableName} had failures.", LogType.Error);
                    return TaskResult.Retry;
                }
            }

            return TaskResult.Success;
        }

        private async Task<TaskResult> ProcessChunkAsync(MigrationUnit migrationUnit, int chunkIndex,
            TableContext context, double initialPercent, double contributionFactor)
        {
            long rowCount = await CassandraQueries.GetRowCountAsync(context.SourceSession, context.KeyspaceName,
                context.TableName);

            if (rowCount > 0)
            {
                migrationUnit.EstimatedRowCount = rowCount;
                migrationUnit.UpdateParentJob();
            }

            migrationUnit.MigrationChunks[chunkIndex].SourceQueryRowCount = rowCount;
            context.DownloadCount += rowCount;

            if (_targetSession == null && !_job.IsSimulatedRun)
            {
                var target = EnsureTargetSession();
                await SchemaManager.EnsureKeyspaceExistsAsync(target, context.TargetKeyspaceName);
            }

            var feedRanges = await CassandraQueries.GetFeedRangesAsync(context.SourceSession, context.KeyspaceName,
                context.TableName);

            _log.WriteLine($"{context.KeyspaceName}.{context.TableName}: " +
                $"{(rowCount >= 0 ? $"{rowCount:N0} rows" : "count unavailable")}, " +
                $"{feedRanges.Count} feed range(s)", LogType.Info);

            if (_job.IsSimulatedRun)
            {
                _log.WriteLine($"Simulated: {context.KeyspaceName}.{context.TableName}", LogType.Info);
                return TaskResult.Success;
            }

            var runner = new BulkCopyRunner(_log, _job, _config, _cancellation, EnsureTargetSession);
            var result = await runner.RunAsync(new PipelineRequest(migrationUnit, chunkIndex, initialPercent,
                contributionFactor, rowCount, context, feedRanges));

            if (result == TaskResult.Success)
            {
                if (!_cancellation.Token.IsCancellationRequested
                    && !MigrationJobContext.ControlledPauseRequested
                    && migrationUnit.MigrationChunks[chunkIndex].Segments.All(seg => seg.IsProcessed == true))
                {
                    migrationUnit.MigrationChunks[chunkIndex].IsDownloaded = true;
                    migrationUnit.MigrationChunks[chunkIndex].IsUploaded = true;
                }
                MigrationJobContext.SaveMigrationUnit(migrationUnit, false);
            }
            else if (result == TaskResult.Canceled)
            {
                _log.WriteLine($"Copy paused for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Info);
            }
            else
            {
                _log.WriteLine($"Copy failed for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Error);
            }
            return result;
        }

        private static Task<TaskResult> HandleChunkException(Exception ex)
        {
            return Task.FromResult(ex is OperationCanceledException ? TaskResult.Abort : TaskResult.Retry);
        }

        // ── Change feed replay ──

        public bool AddTableToChangeFeedQueue(MigrationUnit mu)
        {
            if (!MigrationUtilities.IsOnline(_job)) return false;

            lock (_changeFeedLock)
            {
                if (_targetSession == null)
                {
                    var target = EnsureTargetSession();
                    SchemaManager.EnsureKeyspaceExists(target, mu.GetEffectiveTargetKeyspaceName());
                }

                if (_changeFeedProcessor == null)
                {
                    var freshSourceSession = CassandraClientFactory.CreateSourceSession(_log, _job, mu.KeyspaceName);
                    _changeFeedProcessor = new ReplayProcessor(_log, freshSourceSession, _targetSession!,
                        MigrationJobContext.MigrationUnitsCache, _config,
                        _job, true, null);
                }
            }

            _log.WriteLine($"Adding {mu.KeyspaceName}.{mu.TableName} to change feed queue", LogType.Debug);
            _changeFeedProcessor?.AddTableToProcess(mu.Id, _cancellation);

            return true;
        }

        public bool RunChangeFeedForAllTables()
        {
            if (IsChangeFeedRunning) return false;
            if (!MigrationUtilities.IsOnline(_job)) return false;
            if (!MigrationUtilities.IsOfflineJobCompleted(_job)) return false;
            if (!MigrationUtilities.AnyValidTable(_job)) return false;

            IsChangeFeedRunning = true;

            if (_targetSession == null && !_job.IsSimulatedRun)
                EnsureTargetSession();

            if (_changeFeedProcessor == null)
            {
                var freshSourceSession = CassandraClientFactory.CreateSourceSession(_log, _job, string.Empty);
                _changeFeedProcessor = new ReplayProcessor(_log, freshSourceSession, _targetSession!,
                    MigrationJobContext.MigrationUnitsCache,
                    _config, _job, false, _worker);
            }

            _changeFeedProcessor?.RunChangeFeedForAllTables(_cancellation);

            return true;
        }

        public void StopOfflineOrInvokeChangeFeed()
        {
            if (!MigrationUtilities.IsOnline(_job)
                && MigrationUtilities.IsOfflineJobCompleted(_job))
            {
                if (!MigrationJobContext.ControlledPauseRequested
                    && _job.Status != JobStatus.Cancelled
                    && _job.Status != JobStatus.Paused)
                {
                    _log.WriteLine($"Job {_job.Id} Completed", LogType.Info);
                    _job.Status = JobStatus.Completed;
                    MigrationJobContext.SaveMigrationJob(_job);
                }
                StopProcessing();
            }
            else
            {
                if (!MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine("Invoke RunChangeFeedForAllTables.", LogType.Debug);
                    RunChangeFeedForAllTables();
                }
            }
        }

        public void Dispose()
        {
            IsChangeFeedRunning = false;
            _cancellation?.Dispose();
            MigrationUtilities.SafeDispose(_targetSession, "BulkCopyEngine target session");
        }
    }
}
