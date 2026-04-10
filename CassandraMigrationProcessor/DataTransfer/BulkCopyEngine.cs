using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Orchestrates bulk copy for each table: counts rows,
    /// discovers feed ranges, delegates pipeline execution
    /// to <see cref="BulkCopyRunner"/>.
    /// </summary>
    internal class BulkCopyEngine : MigrationProcessor
    {
        public BulkCopyEngine(MigrationLog log, ISession sourceSession, MigrationSettings config, MigrationJob job,
            MigrationWorker migrationWorker)
            : base(log, sourceSession, config, job, migrationWorker)
        {
        }

        public override async Task<TaskResult> StartProcessAsync(string migrationUnitId)
        {
            var migrationUnit = MigrationJobContext.GetMigrationUnit(migrationUnitId);
            migrationUnit.ParentJob = _job;
            ProcessRunning = true;

            var context = SetTableContext(migrationUnit);

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

            if (!HasTargetSession && !_job.IsSimulatedRun)
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
    }
}
