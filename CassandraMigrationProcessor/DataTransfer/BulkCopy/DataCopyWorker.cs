using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;
/// <summary>
/// Runs a single worker: takes a partition from the pool, reads one
/// page from the Cosmos change-feed query, writes rows to the target,
/// saves a checkpoint, and re-enqueues the partition (or completes it).
///
/// Handles both lifecycle phases via the partition's
/// <see cref="PartitionPhase"/>:
/// <list type="bullet">
///   <item><b>Bulk</b> — draining the initial snapshot. Each page
///         advances <see cref="Partition.LastPagingState"/>. On the
///         first empty page (rows.Count==0): if replay is enabled,
///         the partition transitions to Replay and is re-enqueued
///         on cooldown; otherwise it is marked completed and removed
///         from the pool.</item>
///   <item><b>Replay</b> — tailing the change feed post-drain. Same
///         CQL, same paging-state handoff. Hot pages re-enqueue
///         immediately, cold pages re-enqueue after a cooldown.
///         Replay partitions never complete; the pool stays alive
///         until cancellation.</item>
/// </list>
/// Replay is gated by <see cref="WorkerConfig.EnableReplay"/>, which is
/// true only when the job's CDCMode is Online.
/// </summary>
internal class DataCopyWorker
{
    private readonly CancellationToken _ct;
    private readonly WorkerLog _workerLog;
    private readonly int _pageSize;
    private readonly int _maxReadRetries;
    private readonly int _maxWriteRetries;

    public DataCopyWorker(MigrationLog log, CancellationToken cancellationToken, int workerId,
        int pageSize, int maxReadRetries, int maxWriteRetries)
    {
        if (log == null) throw new ArgumentNullException(nameof(log));
        _ct = cancellationToken;
        _workerLog = new WorkerLog(log, workerId);
        _pageSize = pageSize;
        _maxReadRetries = maxReadRetries;
        _maxWriteRetries = maxWriteRetries;
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
                reader = await PageReader.CreateAsync(_workerLog, ctx.Worker, _pageSize, _maxReadRetries, _ct);
                writer = await PageWriter.CreateAsync(_workerLog, ctx.Worker, _pageSize, _maxWriteRetries, _ct);
            }
            catch (OperationCanceledException)
            {
                ctx.Counters.WorkerErrors.Add(TaskResult.Canceled);
                ctx.PartitionPool.Writer.TryComplete();
                ctx.Counters.BulkDrainSignal.TrySetCanceled();
                return;
            }
            catch (Exception ex)
            {
                // Reader/writer construction failures (e.g., source/target
                // session auth failure) leave the worker with no way to do
                // any work. Treat as fatal so the job is marked Failed
                // instead of silently completing with 0 rows copied.
                _workerLog.WriteLine($"FATAL: worker init failed: {ex.GetType().Name}: {ex.Message}", LogType.Error);
                Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                ctx.Counters.WorkerErrors.Add(TaskResult.Abort);
                ctx.PartitionPool.Writer.TryComplete();
                ctx.Counters.BulkDrainSignal.TrySetException(ex);
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
                    var result = await reader.ReadAsync(partition, ctx);
                    if (result == null)
                    {
                        _workerLog.WriteLine($"FATAL: Read failed — failing job", LogType.Error);
                        Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                        break;
                    }

                    await writer.WriteAsync(result.Rows, result.WorkChunk, partition, ctx);

                    // Only advance checkpoint if ALL rows in the page succeeded.
                    // If any rows failed, checkpoint stays at previous position
                    // so these rows are retried on resume.
                    if (result.WorkChunk.IsCompleted)
                        SaveCheckpoint(partition, ctx);
                    else
                        _workerLog.WriteLine($"Checkpoint NOT advanced — page had failures", LogType.Warning);

                    DispatchAfterPage(partition, result, ctx);
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
                    _workerLog.WriteLine($"Error: {ex.GetType().Name}: {ex.Message}", LogType.Error);

                    if (ExceptionClassifier.IsFatal(ex))
                    {
                        _workerLog.WriteLine($"FATAL — failing job", LogType.Error);
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
            _workerLog.WriteLine($"FATAL: unhandled worker exception: {ex.GetType().Name}: {ex.Message}", LogType.Error);
            Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
            ctx.Counters.WorkerErrors.Add(TaskResult.Abort);
            ctx.PartitionPool.Writer.TryComplete();
        }
        finally
        {
            MigrationUtilities.SafeDispose(writer, "worker PageWriter");
            MigrationUtilities.SafeDispose(reader, "worker PageReader");
            ctx.Tracker.WorkerExited();
            // If we exited before all partitions could finish Bulk drain
            // (cancel/fatal/pause), unblock the WorkerExecutor that may be
            // awaiting BulkDrainSignal.
            ctx.Counters.BulkDrainSignal.TrySetResult();
        }
    }

    /// <summary>
    /// Decide what to do with a partition after a page completes:
    /// re-enqueue (hot or cold), transition Bulk→Replay, or complete.
    /// </summary>
    private void DispatchAfterPage(Partition partition, PageReader.ReadResult result, PipelineContext ctx)
    {
        // Hot page (rows arrived): same phase, re-enqueue immediately.
        if (!result.IsEmptyPage)
        {
            ctx.PartitionPool.Writer.TryWrite(partition);
            return;
        }

        // Empty page in Bulk phase: the snapshot is drained.
        if (partition.Phase == PartitionPhase.Bulk)
        {
            if (ctx.EnableReplay)
            {
                // Online job: flip to Replay using the same paging-state
                // anchor and re-enqueue on cooldown. Replay polls forward
                // from exactly the drain head — no event between snapshot
                // head and now can be missed (no START_TIME re-anchor).
                partition.TransitionToReplay();
                MarkBulkDrained(partition, ctx);
                ScheduleCooldown(partition, ctx);
            }
            else
            {
                // Offline job: bulk-only mode. Partition is done.
                MarkCompleted(partition, ctx);
            }
            return;
        }

        // Empty page in Replay phase: cold-tail cooldown re-enqueue.
        ScheduleCooldown(partition, ctx);
    }

    /// <summary>
    /// Records that a partition has finished Bulk and entered Replay.
    /// Once all partitions have drained, trips the
    /// <see cref="PipelineCounters.BulkDrainSignal"/> so the caller can
    /// mark the table CopyComplete while workers stay alive replaying.
    /// </summary>
    private static void MarkBulkDrained(Partition partition, PipelineContext ctx)
    {
        // Persist the drain handoff anchor into both the bulk
        // checkpoints dict (so resume picks it up via PartitionSeeder)
        // and the CF continuation tokens dict (so the UI surfaces it).
        // Also record this range in CompletedCopyFeedRanges so resume
        // knows it's past bulk.
        lock (ctx.Ranges.Checkpoints)
        {
            ctx.Ranges.Completed.Add(partition.FeedRange);
        }
        PersistFeedRangeContinuation(ctx, partition);

        int drained = Interlocked.Increment(ref ctx.Counters.BulkPhaseDrainedCount);
        if (drained >= ctx.Ranges.FeedRanges.Count)
            ctx.Counters.BulkDrainSignal.TrySetResult();
    }

    private static void PersistFeedRangeContinuation(PipelineContext ctx, Partition partition)
    {
        var token = partition.LastPagingState;
        if (token == null) return;
        var mu = ctx.Tracker.MigrationUnit;
        lock (mu.FeedRangeContinuationTokens)
        {
            mu.FeedRangeContinuationTokens[partition.FeedRange] = Convert.ToBase64String(token);
        }
    }

    /// <summary>
    /// Fire-and-forget delayed re-enqueue used for cold partitions
    /// (empty replay pages). Drops the partition silently on
    /// cancellation or fatal shutdown — the partition's paging state
    /// is already persisted, so the next run resumes correctly.
    /// </summary>
    private void ScheduleCooldown(Partition partition, PipelineContext ctx)
    {
        int cooldownMs = ctx.Worker.ReplayCooldownMs;
        _ = Task.Run(async () =>
        {
            try { await Task.Delay(cooldownMs, _ct); }
            catch (OperationCanceledException) { return; }

            if (_ct.IsCancellationRequested
                || Volatile.Read(ref ctx.Counters.FatalErrorFlag) != 0
                || MigrationJobContext.Instance.ControlledPauseRequested)
                return;

            ctx.PartitionPool.Writer.TryWrite(partition);
        });
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

        // In Replay phase, also surface the token on the MU's
        // FeedRangeContinuationTokens dict for UI visibility.
        if (partition.Phase == PartitionPhase.Replay)
            PersistFeedRangeContinuation(ctx, partition);
    }

    private static void MarkCompleted(Partition partition, PipelineContext ctx)
    {
        lock (ctx.Ranges.Checkpoints)
        {
            ctx.Ranges.Checkpoints.Remove(partition.FeedRange);
            ctx.Ranges.Completed.Add(partition.FeedRange);
        }
        if (ctx.Ranges.Completed.Count >= ctx.Ranges.FeedRanges.Count)
        {
            ctx.PartitionPool.Writer.TryComplete();
            ctx.Counters.BulkDrainSignal.TrySetResult();
        }
    }
}
