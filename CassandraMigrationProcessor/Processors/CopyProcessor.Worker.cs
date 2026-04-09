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
        /// <summary>
        /// Unified worker: reads one page from source, creates
        /// a WorkChunk, recycles the partition back into the
        /// pool (so another worker can read the next page),
        /// then writes rows to target and marks the chunk done.
        /// </summary>
        private async Task RunWorkerAsync(int workerId, PipelineContext ctx)
        {
            ctx.Tracker.WorkerStarted();
            ISession? workerTargetSession = null;
            ISession? workerSourceSession = null;
            try
            {
                var job = ctx.Job;
                workerTargetSession = CassandraClientFactory.CreateTargetSession(
                    _log, job.TargetConnection, "");
                workerSourceSession = CassandraClientFactory.CreateSourceSession(
                    _log, job.SourceConnection, ctx.Context.KeyspaceName);

                var reader = new PageReader(_log, _cancellation);
                var writer = new PageWriter(_log, _cancellation);

                var (preparedInsert, _) = await CassandraHelper.PrepareInsertAsync(workerTargetSession,
                        ctx.Context.TargetKeyspaceName,
                        ctx.Context.TargetTableName,
                        ctx.Columns);

                while (!_cancellation.Token.IsCancellationRequested
                    && Volatile.Read(ref ctx.FatalErrorFlag) == 0)
                {
                    Partition partition;
                    try
                    {
                        partition = await ctx.PartitionPool.Reader.ReadAsync(_cancellation.Token);
                    }
                    catch (OperationCanceledException) { break; }
                    catch (ChannelClosedException) { break; }

                    if (_cancellation.Token.IsCancellationRequested
                        || Volatile.Read(ref ctx.FatalErrorFlag) != 0)
                    {
                        // Save checkpoint but do NOT mark the
                        // range as completed — it still has
                        // uncopied data. Closing the channel
                        // lets all workers drain and exit.
                        lock (ctx.Checkpoints)
                        {
                            var token = partition.GetResumeToken();
                            if (token != null)
                                ctx.Checkpoints[
                                    partition.FeedRange] = Convert.ToBase64String(token);
                            else if (partition.LastPagingState != null)
                                ctx.Checkpoints[
                                    partition.FeedRange] = Convert.ToBase64String(partition.LastPagingState);
                        }
                        ctx.PartitionPool.Writer.TryComplete();
                        continue;
                    }

                    if (partition.IsExhausted)
                    {
                        lock (ctx.Checkpoints)
                        {
                            ctx.Completed.Add(partition.FeedRange);
                        }
                        TryCloseChannel(ctx);
                        continue;
                    }

                    bool isLastPage = false;
                    try
                    {
                        var (rows, nextPaging, lastPage, readTimeMs) = await reader.ReadAsync(
                                partition, workerSourceSession!, ctx,
                                workerId);

                        if (rows == null)
                        {
                            // Read failed after all retries —
                            // DO NOT skip this range. Mark as
                            // error so job fails instead of
                            // silently losing data.
                            _log.WriteLine($"[W{workerId}] FATAL: Read failed after retries for range {TruncRange(partition.FeedRange)} — failing job to prevent data loss",
                                LogType.Error);
                            Interlocked.Exchange(ref ctx.FatalErrorFlag, 1);
                            try { _cancellation.Cancel(); }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[WARN] Cancel failed: {ex.Message}");
                            }
                            break;
                        }

                        isLastPage = lastPage;
                        partition.LastPagingState = nextPaging;
                        Interlocked.Add(ref ctx.TotalRead, rows.Count);
                        ctx.Tracker.AddReadTime(readTimeMs);

                        var workChunk = partition.AddChunkAndTrim(nextPaging);

                        if (isLastPage)
                            partition.IsExhausted = true;

                        if (!isLastPage)
                        {
                            try
                            {
                                await ctx.PartitionPool.Writer.WriteAsync(partition, _cancellation.Token);
                            }
                            catch (OperationCanceledException)
                            {
                                isLastPage = true;
                                partition.IsExhausted = true;
                            }
                        }

                        if (rows.Count > 0)
                        {
                            await writer.WriteAsync(rows, preparedInsert, workerTargetSession!, workChunk, ctx, workerId);
                        }
                        else
                        {
                            workChunk.IsCompleted = true;
                        }

                        lock (ctx.Checkpoints)
                        {
                            if (partition.IsExhausted)
                            {
                                ctx.Checkpoints.Remove(partition.FeedRange);
                                ctx.Completed.Add(partition.FeedRange);
                            }
                            else
                            {
                                var token = partition.GetResumeToken();
                                if (token != null)
                                    ctx.Checkpoints[
                                        partition.FeedRange] = Convert.ToBase64String(token);
                            }
                        }

                        if (partition.IsExhausted)
                        {
                            ctx.Tracker.RangeCompleted(partition.FeedRange, TaskResult.Success);
                            TryCloseChannel(ctx);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        ctx.WorkerErrors.Add(TaskResult.Canceled);
                        if (!partition.IsExhausted)
                        {
                            // Save checkpoint but do NOT mark
                            // the range as completed — resume
                            // needs to re-process it.
                            lock (ctx.Checkpoints)
                            {
                                var token = partition.GetResumeToken();
                                if (token != null)
                                    ctx.Checkpoints[
                                        partition.FeedRange] = Convert.ToBase64String(token);
                                else if (partition.LastPagingState
                                    != null)
                                    ctx.Checkpoints[
                                        partition.FeedRange] = Convert.ToBase64String(partition.LastPagingState);
                            }
                            ctx.PartitionPool.Writer.TryComplete();
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"[W{workerId}] Worker error: {ex.GetType().Name}: {ex.Message}",
                            LogType.Error);

                        if (IsFatalError(ex))
                        {
                            _log.WriteLine($"[W{workerId}] FATAL: {ex.GetType().Name} — failing job",
                                LogType.Error);
                            Interlocked.Exchange(ref ctx.FatalErrorFlag, 1);
                            try { _cancellation.Cancel(); }
                            catch (Exception cancelEx)
                            {
                                Console.WriteLine($"[WARN] CopyProcessor cancel failed: {cancelEx.Message}");
                            }
                            ctx.WorkerErrors.Add(TaskResult.Abort);
                        }
                        else
                        {
                            ctx.WorkerErrors.Add(TaskResult.Retry);
                        }

                        if (!ctx.Completed.Contains(partition.FeedRange))
                        {
                            // Save checkpoint for the failed
                            // range so resume can retry from
                            // the last good position. Do NOT
                            // mark as completed — the range
                            // still has uncopied data.
                            lock (ctx.Checkpoints)
                            {
                                var token = partition.GetResumeToken();
                                if (token != null)
                                    ctx.Checkpoints[
                                        partition.FeedRange] = Convert.ToBase64String(token);
                                else if (partition.LastPagingState
                                    != null)
                                    ctx.Checkpoints[
                                        partition.FeedRange] = Convert.ToBase64String(partition.LastPagingState);
                            }
                            ctx.Tracker.RangeCompleted(partition.FeedRange, TaskResult.Retry);
                            // Close channel so workers drain
                            // and the pipeline can return the
                            // error to the retry helper.
                            ctx.PartitionPool.Writer.TryComplete();
                        }
                    }
                    finally
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
                                (Math.Min(MigrationDefaults.ProgressCapPercent, (double)written / ctx.TotalRowCount * 100)
                                * ctx.ContributionFactor);
                        }
                        ctx.MigrationUnit.UpdateParentJob();

                        // Save checkpoint every 10s
                        long prevTicks = Volatile.Read(ref ctx.LastCheckpointTicks);
                        long nowTicks = DateTime.UtcNow.Ticks;
                        if ((nowTicks - prevTicks) / TimeSpan.TicksPerSecond >= MigrationDefaults.CheckpointIntervalSeconds
                            && Interlocked.CompareExchange(ref ctx.LastCheckpointTicks, nowTicks, prevTicks) == prevTicks)
                        {
                            MigrationJobContext.SaveMigrationUnit(ctx.MigrationUnit, true);
                        }
                    }
                }
            }
            finally
            {
                try { workerTargetSession?.Dispose(); }
                catch (Exception ex) { Console.WriteLine($"[WARN] CopyProcessor worker target session dispose failed: {ex.Message}"); }
                try { workerSourceSession?.Dispose(); }
                catch (Exception ex) { Console.WriteLine($"[WARN] CopyProcessor worker source session dispose failed: {ex.Message}"); }
                ctx.Tracker.WorkerExited();
            }
        }

        private static void TryCloseChannel(PipelineContext ctx)
        {
            if (ctx.Completed.Count >= ctx.FeedRanges.Count)
                ctx.PartitionPool.Writer.TryComplete();
        }
    }
}
