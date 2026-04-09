using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Infrastructure;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Pipeline
{
    internal partial class CopyProcessor
    {
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

            if (!migrationUnit.CopyComplete
                && !_cancellation.Token.IsCancellationRequested)
            {
                // Ensure at least one chunk exists
                if (migrationUnit.MigrationChunks == null
                    || migrationUnit.MigrationChunks.Count == 0)
                {
                    migrationUnit.MigrationChunks =
                        new System.Collections.Generic.List<MigrationChunk>
                        {
                            new MigrationChunk()
                        };
                }

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
                                () => ProcessChunkAsync(migrationUnit, chunkIndex, context, initialPercent,
                                    contributionFactor),
                                (ex, attemptCount, currentBackoff) => CopyProcess_ExceptionHandler(ex, attemptCount,
                                        "Chunk processor",
                                        context.KeyspaceName,
                                        context.TableName,
                                        chunkIndex, currentBackoff),
                                _log,
                                ct: _cancellation.Token);

                        if (result == TaskResult.Canceled)
                        {
                            _log.WriteLine($"Copy paused for {context.KeyspaceName}.{context.TableName}[{chunkIndex}].", LogType.Info);
                            StopProcessing(isPause: true);
                            return TaskResult.Canceled;
                        }

                        if (result == TaskResult.Abort
                            || result == TaskResult.FailedAfterRetries)
                        {
                            _log.WriteLine($"Copy failed for {context.KeyspaceName}.{context.TableName}[{chunkIndex}] after retries.",
                                LogType.Error);
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
                    StopProcessing(isPause: true);
                    return TaskResult.Success;
                }

                migrationUnit.SourceCountDuringCopy = migrationUnit.MigrationChunks.Sum(c => c.SourceQueryRowCount);

                long failed = migrationUnit.MigrationChunks.Sum(c => c.TargetFailedRowCount);

                if (failed <= 0
                    && migrationUnit.MigrationChunks
                        .All(c => c.IsDownloaded == true))
                {
                    migrationUnit.BulkCopyEndedOn = DateTime.UtcNow;
                    migrationUnit.CopyPercent = 100;
                    migrationUnit.CopyComplete = true;
                    migrationUnit.UpdateParentJob();

                    AddTableToChangeFeedQueue(migrationUnit);
                    MigrationJobContext.SaveMigrationUnit(migrationUnit, true);

                    // Only remove from cache if offline — online mode
                    // needs the MU in cache for ChangeFeedProcessor
                    if (!MigrationUtilities.IsOnline(_job))
                    {
                        MigrationJobContext.MigrationUnitsCache.RemoveMigrationUnit(migrationUnit.Id);
                    }
                }
                else
                {
                    _log.WriteLine($"Copy for {context.KeyspaceName}.{context.TableName} had failures.",
                        LogType.Error);
                    return TaskResult.Retry;
                }
            }

            return TaskResult.Success;
        }
    }
}
