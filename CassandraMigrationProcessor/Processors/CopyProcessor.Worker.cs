using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Models;
using System;
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
        private void SafeCancel()
        {
            try { _cancellation.Cancel(); }
            catch (Exception ex) { Console.Error.WriteLine($"[WARN] CopyProcessor cancel failed: {ex.Message}"); }
        }

        private static void SavePartitionCheckpoint(Partition partition, PipelineContext ctx)
        {
            lock (ctx.Ranges.Checkpoints)
            {
                var token = partition.GetResumeToken();
                if (token != null)
                    ctx.Ranges.Checkpoints[partition.FeedRange] = Convert.ToBase64String(token);
                else if (partition.LastPagingState != null)
                    ctx.Ranges.Checkpoints[partition.FeedRange] = Convert.ToBase64String(partition.LastPagingState);
            }
        }

        private static void MarkRangeCompleted(Partition partition, PipelineContext ctx)
        {
            lock (ctx.Ranges.Checkpoints)
            {
                ctx.Ranges.Checkpoints.Remove(partition.FeedRange);
                ctx.Ranges.Completed.Add(partition.FeedRange);
            }
            ctx.Tracker.RangeCompleted(partition.FeedRange, TaskResult.Success);
            TryCloseChannel(ctx);
        }

        /// <summary>
        /// Unified worker: takes partition → reads page → recycles
        /// partition → writes rows → updates checkpoint.
        /// </summary>
        private async Task RunWorkerAsync(int workerId, PipelineContext ctx, int configuredPageSize)
        {
            ctx.Tracker.WorkerStarted();
            PageReader? reader = null;
            PageWriter? writer = null;
            try
            {
                reader = new PageReader(_log, ctx.Worker.SourceConnection, ctx.Worker.Context.KeyspaceName,
                    ctx.Worker.Columns.Select(c => c.Name).ToList(), configuredPageSize, workerId, _cancellation);
                writer = new PageWriter(_log, ctx.Worker.TargetConnection, ctx.Worker.Columns,
                    ctx.Worker.Context.TargetKeyspaceName, ctx.Worker.Context.TargetTableName, configuredPageSize, workerId, _cancellation);

                while (!_cancellation.Token.IsCancellationRequested && Volatile.Read(ref ctx.Counters.FatalErrorFlag) == 0)
                {
                    var partition = await TakeNextPartitionAsync(ctx);
                    if (partition == null) break;

                    try
                    {
                        if (!partition.IsExhausted)
                        {
                            var result = await reader.ReadAsync(partition, ctx);

                            if (result == null)
                            {
                                _log.WriteLine($"[W{workerId}] FATAL: Read failed — failing job", LogType.Error);
                                Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                                SafeCancel();
                                break;
                            }

                            if (!result.IsLastPage)
                                await ctx.PartitionPool.Writer.WriteAsync(partition, _cancellation.Token);

                            await writer.WriteAsync(result.Rows, result.WorkChunk, ctx);
                        }

                        // Always save checkpoint; additionally mark completed if exhausted
                        SavePartitionCheckpoint(partition, ctx);
                        if (partition.IsExhausted) MarkRangeCompleted(partition, ctx);
                    }
                    catch (OperationCanceledException)
                    {
                        ctx.Counters.WorkerErrors.Add(TaskResult.Canceled);
                        SavePartitionCheckpoint(partition, ctx);
                        ctx.PartitionPool.Writer.TryComplete();
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"[W{workerId}] Error: {ex.GetType().Name}: {ex.Message}", LogType.Error);

                        if (IsFatalError(ex))
                        {
                            _log.WriteLine($"[W{workerId}] FATAL — failing job", LogType.Error);
                            Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                            SafeCancel();
                            ctx.Counters.WorkerErrors.Add(TaskResult.Abort);
                        }
                        else
                        {
                            ctx.Counters.WorkerErrors.Add(TaskResult.Retry);
                        }

                        SavePartitionCheckpoint(partition, ctx);
                        ctx.Tracker.RangeCompleted(partition.FeedRange, TaskResult.Retry);
                        ctx.PartitionPool.Writer.TryComplete();
                    }
                    finally
                    {
                        ctx.Tracker.UpdateMigrationUnit();
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
            if (ctx.Ranges.Completed.Count >= ctx.Ranges.FeedRanges.Count)
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


