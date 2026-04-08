using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Helpers.JobManagement;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Processors
{
    internal partial class CopyProcessor
    {
        public override async Task<TaskResult> StartProcessAsync(
            string migrationUnitId)
        {
            var mu = MigrationJobContext
                .GetMigrationUnit(migrationUnitId);
            mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
            ProcessRunning = true;

            var ctx = SetProcessorContext(mu);

            if (mu.CopyComplete)
            {
                _log.WriteLine(
                    $"Copy for {ctx.KeyspaceName}.{ctx.TableName} " +
                    $"already completed.", LogType.Debug);
                return TaskResult.Success;
            }

            _log.WriteLine(
                $"{ctx.KeyspaceName}.{ctx.TableName} Copy started");

            if (!mu.CopyComplete
                && !_cts.Token.IsCancellationRequested)
            {
                // Ensure at least one chunk exists
                if (mu.MigrationChunks == null
                    || mu.MigrationChunks.Count == 0)
                {
                    mu.MigrationChunks =
                        new System.Collections.Generic.List<MigrationChunk>
                        {
                            new MigrationChunk()
                        };
                }

                for (int i = 0; i < mu.MigrationChunks.Count; i++)
                {
                    if (MigrationJobContext.ControlledPauseRequested)
                    {
                        _log.WriteLine(
                            $"Controlled pause before chunk {i}");
                        break;
                    }

                    _cts.Token.ThrowIfCancellationRequested();

                    double initialPercent =
                        ((double)100 / mu.MigrationChunks.Count) * i;
                    double contributionFactor =
                        1.0 / mu.MigrationChunks.Count;

                    if (mu.MigrationChunks[i].IsDownloaded != true)
                    {
                        TaskResult result =
                            await new RetryHelper().ExecuteTask(
                                () => ProcessChunkAsync(
                                    mu, i, ctx,
                                    initialPercent,
                                    contributionFactor),
                                (ex, attemptCount, currentBackoff) =>
                                    CopyProcess_ExceptionHandler(
                                        ex, attemptCount,
                                        "Chunk processor",
                                        ctx.KeyspaceName,
                                        ctx.TableName,
                                        i, currentBackoff),
                                _log,
                                ct: _cts.Token);

                        if (result == TaskResult.Canceled)
                        {
                            _log.WriteLine(
                                $"Copy paused for " +
                                $"{ctx.KeyspaceName}.{ctx.TableName}" +
                                $"[{i}].");
                            StopProcessing(isPause: true);
                            return TaskResult.Canceled;
                        }

                        if (result == TaskResult.Abort
                            || result == TaskResult.FailedAfterRetries)
                        {
                            _log.WriteLine(
                                $"Copy failed for " +
                                $"{ctx.KeyspaceName}.{ctx.TableName}" +
                                $"[{i}] after retries.",
                                LogType.Error);
                            StopProcessing();
                            return result;
                        }
                    }
                    else
                    {
                        ctx.DownloadCount +=
                            mu.MigrationChunks[i].SourceQueryRowCount;
                    }
                }

                if (MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine(
                        "Controlled pause - exiting",
                        LogType.Debug);
                    StopProcessing(isPause: true);
                    return TaskResult.Success;
                }

                mu.SourceCountDuringCopy = mu.MigrationChunks
                    .Sum(c => c.SourceQueryRowCount);

                long failed = mu.MigrationChunks
                    .Sum(c => c.TargetFailedRowCount);

                if (failed <= 0
                    && mu.MigrationChunks
                        .All(c => c.IsDownloaded == true))
                {
                    mu.BulkCopyEndedOn = DateTime.UtcNow;
                    mu.CopyPercent = 100;
                    mu.CopyComplete = true;
                    mu.UpdateParentJob();

                    AddTableToChangeFeedQueue(mu);
                    MigrationJobContext.SaveMigrationUnit(mu, true);

                    // Only remove from cache if offline — online mode
                    // needs the MU in cache for ChangeFeedProcessor
                    if (!Helper.IsOnline(
                        MigrationJobContext.CurrentlyActiveJob))
                    {
                        MigrationJobContext.MigrationUnitsCache
                            .RemoveMigrationUnit(mu.Id);
                    }
                }
                else
                {
                    _log.WriteLine(
                        $"Copy for {ctx.KeyspaceName}" +
                        $".{ctx.TableName} had failures.",
                        LogType.Error);
                    return TaskResult.Retry;
                }
            }

            return TaskResult.Success;
        }
    }
}
