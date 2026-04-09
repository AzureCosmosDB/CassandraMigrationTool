using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Processors
{
    internal partial class CopyProcessor
    {
        private void SafeCancel()
        {
            try { _cancellation.Cancel(); }
            catch (Exception ex) { Console.Error.WriteLine($"[WARN] CopyProcessor cancel failed: {ex.Message}"); }
        }

        private static void SavePartitionCheckpoint(Partition partition, PipelineContext ctx)
        {
            lock (ctx.Checkpoints)
            {
                var token = partition.GetResumeToken();
                if (token != null)
                    ctx.Checkpoints[partition.FeedRange] = Convert.ToBase64String(token);
                else if (partition.LastPagingState != null)
                    ctx.Checkpoints[partition.FeedRange] = Convert.ToBase64String(partition.LastPagingState);
            }
        }

        private static void MarkRangeCompleted(Partition partition, PipelineContext ctx)
        {
            lock (ctx.Checkpoints)
            {
                ctx.Checkpoints.Remove(partition.FeedRange);
                ctx.Completed.Add(partition.FeedRange);
            }
            ctx.Tracker.RangeCompleted(partition.FeedRange, TaskResult.Success);
            TryCloseChannel(ctx);
        }

        private static void UpdateProgress(PipelineContext ctx)
        {
            long written = Volatile.Read(ref ctx.TotalWritten);
            long failed = Volatile.Read(ref ctx.TotalFailed);
            var chunk = ctx.MigrationUnit.MigrationChunks[ctx.ChunkIndex];
            chunk.SourceResultRowCount = written;
            chunk.TargetInsertedRowCount = written;
            chunk.TargetFailedRowCount = failed;
            ctx.MigrationUnit.CopyRowsCopied = written;
            ctx.MigrationUnit.CopyRowsPerSecond = ctx.Tracker.RecentSpeed;
            if (ctx.TotalRowCount > 0)
            {
                ctx.MigrationUnit.CopyPercent = ctx.InitialPercent +
                    (Math.Min(MigrationDefaults.ProgressCapPercent,
                        (double)written / ctx.TotalRowCount * 100)
                    * ctx.ContributionFactor);
            }
            ctx.MigrationUnit.UpdateParentJob();

            long prevTicks = Volatile.Read(ref ctx.LastCheckpointTicks);
            long nowTicks = DateTime.UtcNow.Ticks;
            if ((nowTicks - prevTicks) / TimeSpan.TicksPerSecond >= MigrationDefaults.CheckpointIntervalSeconds
                && Interlocked.CompareExchange(ref ctx.LastCheckpointTicks, nowTicks, prevTicks) == prevTicks)
            {
                MigrationJobContext.SaveMigrationUnit(ctx.MigrationUnit, true);
            }
        }

        /// <summary>
        /// Unified worker: takes partition → reads page → recycles
        /// partition → writes rows → updates checkpoint.
        /// </summary>
        private async Task RunWorkerAsync(int workerId, PipelineContext ctx)
        {
            ctx.Tracker.WorkerStarted();
            PageReader? reader = null;
            PageWriter? writer = null;
            try
            {
                var job = ctx.Job;
                var sourceSession = CassandraClientFactory.CreateSourceSession(_log, job.SourceConnection, ctx.Context.KeyspaceName);
                var targetSession = CassandraClientFactory.CreateTargetSession(_log, job.TargetConnection, "");
                var (preparedInsert, _) = await CassandraHelper.PrepareInsertAsync(
                    targetSession, ctx.Context.TargetKeyspaceName, ctx.Context.TargetTableName, ctx.Columns);
                reader = new PageReader(_log, _cancellation, sourceSession);
                writer = new PageWriter(_log, _cancellation, targetSession, preparedInsert);

                while (!_cancellation.Token.IsCancellationRequested && Volatile.Read(ref ctx.FatalErrorFlag) == 0)
                {
                    var partition = await TakeNextPartitionAsync(ctx);
                    if (partition == null) break;

                    if (partition.IsExhausted)
                    {
                        MarkRangeCompleted(partition, ctx);
                        continue;
                    }

                    try
                    {
                        var (rows, workChunk, isLastPage) = await reader.ReadAsync(
                            partition, ctx, workerId);

                        if (rows == null)
                        {
                            _log.WriteLine($"[W{workerId}] FATAL: Read failed — failing job", LogType.Error);
                            Interlocked.Exchange(ref ctx.FatalErrorFlag, 1);
                            SafeCancel();
                            break;
                        }

                        // Recycle partition for next page read
                        if (!isLastPage)
                            await ctx.PartitionPool.Writer.WriteAsync(partition, _cancellation.Token);

                        await writer.WriteAsync(rows, workChunk!, ctx, workerId);

                        if (partition.IsExhausted) MarkRangeCompleted(partition, ctx);
                        else SavePartitionCheckpoint(partition, ctx);
                    }
                    catch (OperationCanceledException)
                    {
                        ctx.WorkerErrors.Add(TaskResult.Canceled);
                        SavePartitionCheckpoint(partition, ctx);
                        ctx.PartitionPool.Writer.TryComplete();
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"[W{workerId}] Error: {ex.GetType().Name}: {ex.Message}", LogType.Error);

                        if (IsFatalError(ex))
                        {
                            _log.WriteLine($"[W{workerId}] FATAL — failing job", LogType.Error);
                            Interlocked.Exchange(ref ctx.FatalErrorFlag, 1);
                            SafeCancel();
                            ctx.WorkerErrors.Add(TaskResult.Abort);
                        }
                        else
                        {
                            ctx.WorkerErrors.Add(TaskResult.Retry);
                        }

                        SavePartitionCheckpoint(partition, ctx);
                        ctx.Tracker.RangeCompleted(partition.FeedRange, TaskResult.Retry);
                        ctx.PartitionPool.Writer.TryComplete();
                    }
                    finally
                    {
                        UpdateProgress(ctx);
                    }
                }
            }
            finally
            {
                MigrationHelper.SafeDispose(writer, "worker PageWriter");
                MigrationHelper.SafeDispose(reader, "worker PageReader");
                ctx.Tracker.WorkerExited();
            }
        }

        private static void TryCloseChannel(PipelineContext ctx)
        {
            if (ctx.Completed.Count >= ctx.FeedRanges.Count)
                ctx.PartitionPool.Writer.TryComplete();
        }

        /// <summary>
        /// Takes the next partition from the pool. Returns null
        /// when cancelled or channel completed — no exceptions.
        /// </summary>
        private async Task<Partition?> TakeNextPartitionAsync(PipelineContext ctx)
        {
            try
            {
                if (await ctx.PartitionPool.Reader.WaitToReadAsync(_cancellation.Token))
                    if (ctx.PartitionPool.Reader.TryRead(out var p))
                        return p;
            }
            catch (OperationCanceledException) { /* cancelled — return null */ }
            return null;
        }
    }
}
