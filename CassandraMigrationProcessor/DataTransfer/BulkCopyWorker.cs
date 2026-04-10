using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Runs a single worker: takes partitions from the pool,
    /// reads a page, recycles the partition, writes rows,
    /// and saves checkpoints.
    /// </summary>
    internal class BulkCopyWorker
    {
        private readonly MigrationLog _log;
        private readonly CancellationToken _ct;
        private readonly int _workerId;
        private readonly int _pageSize;

        public BulkCopyWorker(MigrationLog log, CancellationToken cancellationToken, int workerId, int pageSize)
        {
            _log = log;
            _ct = cancellationToken;
            _workerId = workerId;
            _pageSize = pageSize;
        }

        public async Task RunAsync(PipelineContext ctx)
        {
            ctx.Tracker.WorkerStarted();
            PageReader? reader = null;
            PageWriter? writer = null;
            try
            {
                reader = new PageReader(_log, ctx.Worker.SourceConnection, ctx.Worker.Context.KeyspaceName,
                    ctx.Worker.Columns.Select(c => c.Name).ToList(), _pageSize, _workerId, _ct);
                writer = new PageWriter(_log, ctx.Worker.TargetConnection, ctx.Worker.Columns,
                    ctx.Worker.Context.TargetKeyspaceName, ctx.Worker.Context.TargetTableName, _pageSize, _workerId, _ct);

                while (!_ct.IsCancellationRequested && Volatile.Read(ref ctx.Counters.FatalErrorFlag) == 0)
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
                                _log.WriteLine($"[W{_workerId}] FATAL: Read failed — failing job", LogType.Error);
                                Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                                break;
                            }

                            if (!result.IsLastPage)
                                await ctx.PartitionPool.Writer.WriteAsync(partition, _ct);

                            await writer.WriteAsync(result.Rows, result.WorkChunk, ctx);
                        }

                        SaveCheckpoint(partition, ctx);
                        if (partition.IsExhausted) MarkCompleted(partition, ctx);
                    }
                    catch (OperationCanceledException)
                    {
                        ctx.Counters.WorkerErrors.Add(TaskResult.Canceled);
                        SaveCheckpoint(partition, ctx);
                        ctx.PartitionPool.Writer.TryComplete();
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"[W{_workerId}] Error: {ex.GetType().Name}: {ex.Message}", LogType.Error);

                        if (ExceptionClassifier.IsFatal(ex))
                        {
                            _log.WriteLine($"[W{_workerId}] FATAL — failing job", LogType.Error);
                            Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                            ctx.Counters.WorkerErrors.Add(TaskResult.Abort);
                        }
                        else
                        {
                            ctx.Counters.WorkerErrors.Add(TaskResult.Retry);
                        }

                        SaveCheckpoint(partition, ctx);
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
                MigrationUtilities.SafeDispose(writer, "worker PageWriter");
                MigrationUtilities.SafeDispose(reader, "worker PageReader");
                ctx.Tracker.WorkerExited();
            }
        }

        private async Task<Partition?> TakeNextPartitionAsync(PipelineContext ctx)
        {
            try
            {
                if (await ctx.PartitionPool.Reader.WaitToReadAsync(_ct))
                    if (ctx.PartitionPool.Reader.TryRead(out var p))
                        return p;
            }
            catch (OperationCanceledException) { }
            return null;
        }

        private static void SaveCheckpoint(Partition partition, PipelineContext ctx)
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

        private static void MarkCompleted(Partition partition, PipelineContext ctx)
        {
            lock (ctx.Ranges.Checkpoints)
            {
                ctx.Ranges.Checkpoints.Remove(partition.FeedRange);
                ctx.Ranges.Completed.Add(partition.FeedRange);
            }
            ctx.Tracker.RangeCompleted(partition.FeedRange, TaskResult.Success);
            if (ctx.Ranges.Completed.Count >= ctx.Ranges.FeedRanges.Count)
                ctx.PartitionPool.Writer.TryComplete();
        }
    }
}
