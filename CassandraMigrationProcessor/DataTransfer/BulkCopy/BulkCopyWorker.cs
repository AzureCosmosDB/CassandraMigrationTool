using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;
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
        _log = log ?? throw new ArgumentNullException(nameof(log));
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
            try
            {
                reader = new PageReader(_log, ctx.Worker, _pageSize, _workerId, _ct);
                writer = new PageWriter(_log, ctx.Worker, _pageSize, _workerId, _ct);
            }
            catch (OperationCanceledException)
            {
                ctx.Counters.WorkerErrors.Add(TaskResult.Canceled);
                ctx.PartitionPool.Writer.TryComplete();
                return;
            }
            catch (Exception ex)
            {
                // Reader/writer construction failures (e.g., source/target
                // session auth failure) leave the worker with no way to do
                // any work. Treat as fatal so the job is marked Failed
                // instead of silently completing with 0 rows copied.
                _log.WriteLine($"[W{_workerId}] FATAL: worker init failed: {ex.GetType().Name}: {ex.Message}", LogType.Error);
                Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                ctx.Counters.WorkerErrors.Add(TaskResult.Abort);
                ctx.PartitionPool.Writer.TryComplete();
                return;
            }

            while (!_ct.IsCancellationRequested
                && Volatile.Read(ref ctx.Counters.FatalErrorFlag) == 0
                && !MigrationJobContext.Instance.ControlledPauseRequested)
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

                        // Only advance checkpoint if ALL rows in the page succeeded.
                        // If any rows failed, checkpoint stays at previous position
                        // so these rows are retried on resume.
                        if (result.WorkChunk.IsCompleted)
                            SaveCheckpoint(partition, ctx);
                        else
                            _log.WriteLine($"[W{_workerId}] Checkpoint NOT advanced — page had failures", LogType.Warning);
                    }

                    if (partition.IsExhausted) MarkCompleted(partition, ctx);
                }
                catch (OperationCanceledException)
                {
                    ctx.Counters.WorkerErrors.Add(TaskResult.Canceled);
                    // Don't advance checkpoint on cancel — the current page
                    // may have been partially written. Resume will re-read
                    // from the last successfully checkpointed position.
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

                    // Don't advance checkpoint on error — same reasoning
                    // as cancel: partially written page would skip rows.
                    ctx.PartitionPool.Writer.TryComplete();
                }
                finally
                {
                    ctx.Tracker.UpdateMigrationUnit();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Cancellation propagated out of the loop body itself
            // (e.g. while awaiting partition pool). Record graceful exit.
            ctx.Counters.WorkerErrors.Add(TaskResult.Canceled);
            ctx.PartitionPool.Writer.TryComplete();
        }
        catch (Exception ex)
        {
            // Backstop for any exception that escapes the per-partition
            // try/catch above. Without this, a worker task could fault
            // silently and DetermineOutcome would still return Success.
            _log.WriteLine($"[W{_workerId}] FATAL: unhandled worker exception: {ex.GetType().Name}: {ex.Message}", LogType.Error);
            Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
            ctx.Counters.WorkerErrors.Add(TaskResult.Abort);
            ctx.PartitionPool.Writer.TryComplete();
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
        catch (OperationCanceledException) { } // Expected: graceful cancellation
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
        if (ctx.Ranges.Completed.Count >= ctx.Ranges.FeedRanges.Count)
            ctx.PartitionPool.Writer.TryComplete();
    }
}
