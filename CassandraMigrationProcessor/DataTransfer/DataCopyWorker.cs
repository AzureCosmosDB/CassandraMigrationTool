using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;
/// <summary>
/// Job-shared worker. Takes a partition (from any table) off the shared
/// channel, reads one page, writes rows, saves checkpoint, re-enqueues
/// or completes. All per-table state (tracker, ranges, drain signal,
/// columns, identifiers) is resolved through
/// <see cref="Partition.Resources"/> so a single pool can service many
/// tables concurrently.
///
/// Phase behaviour:
/// <list type="bullet">
///   <item><b>Bulk</b> — drain the snapshot. First empty page either
///         transitions to Replay (online) and re-enqueues on cooldown,
///         or completes the partition (offline).</item>
///   <item><b>Replay</b> — tail the change feed. Hot pages re-enqueue
///         immediately, cold pages re-enqueue after cooldown. Replay
///         partitions never complete; the pool runs until cancellation.</item>
/// </list>
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
                return;
            }
            catch (Exception ex)
            {
                _workerLog.WriteLine($"FATAL: worker init failed: {ex.GetType().Name}: {ex.Message}", LogType.Error);
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
                var resources = partition.Resources;

                try
                {
                    var result = await reader.ReadAsync(partition, ctx);
                    if (result == null)
                    {
                        _workerLog.WriteLine($"FATAL: Read failed for {resources.TableId} — failing job", LogType.Error);
                        Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                        break;
                    }

                    await writer.WriteAsync(result.Rows, result.WorkChunk, partition, ctx);

                    if (result.WorkChunk.IsCompleted)
                        SaveCheckpoint(partition);
                    else
                        _workerLog.WriteLine($"Checkpoint NOT advanced for {resources.TableId} — page had failures", LogType.Warning);

                    DispatchAfterPage(partition, result, ctx);
                }
                catch (OperationCanceledException)
                {
                    ctx.Counters.WorkerErrors.Add(TaskResult.Canceled);
                    ctx.PartitionPool.Writer.TryComplete();
                }
                catch (Exception ex)
                {
                    _workerLog.WriteLine($"Error on {resources.TableId}: {ex.GetType().Name}: {ex.Message}", LogType.Error);

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

                    ctx.PartitionPool.Writer.TryComplete();
                }
                finally
                {
                    resources.Tracker.UpdateMigrationUnit();
                }
            }
        }
        catch (OperationCanceledException)
        {
            ctx.Counters.WorkerErrors.Add(TaskResult.Canceled);
            ctx.PartitionPool.Writer.TryComplete();
        }
        catch (Exception ex)
        {
            _workerLog.WriteLine($"FATAL: unhandled worker exception: {ex.GetType().Name}: {ex.Message}", LogType.Error);
            Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
            ctx.Counters.WorkerErrors.Add(TaskResult.Abort);
            ctx.PartitionPool.Writer.TryComplete();
        }
        finally
        {
            MigrationUtilities.SafeDispose(writer, "worker PageWriter");
            MigrationUtilities.SafeDispose(reader, "worker PageReader");
        }
    }

    private void DispatchAfterPage(Partition partition, PageReader.ReadResult result, PipelineContext ctx)
    {
        if (!result.IsEmptyPage)
        {
            ctx.PartitionPool.Writer.TryWrite(partition);
            return;
        }

        if (partition.Phase == PartitionPhase.Bulk)
        {
            if (ctx.EnableReplay)
            {
                partition.TransitionToReplay();
                MarkBulkDrained(partition);
                ScheduleCooldown(partition, ctx);
            }
            else
            {
                MarkCompleted(partition, ctx);
            }
            return;
        }

        ScheduleCooldown(partition, ctx);
    }

    private static void MarkBulkDrained(Partition partition)
    {
        var resources = partition.Resources;
        lock (resources.Ranges.Checkpoints)
        {
            resources.Ranges.Completed.Add(partition.FeedRange);
        }
        PersistFeedRangeContinuation(partition);

        int drained = Interlocked.Increment(ref resources.BulkDrainedCount);
        if (drained >= resources.Ranges.FeedRanges.Count)
            resources.BulkDrainSignal.TrySetResult();
    }

    private static void PersistFeedRangeContinuation(Partition partition)
    {
        var token = partition.LastPagingState;
        if (token == null) return;
        var mu = partition.Resources.Tracker.MigrationUnit;
        lock (mu.FeedRangeContinuationTokens)
        {
            mu.FeedRangeContinuationTokens[partition.FeedRange] = Convert.ToBase64String(token);
        }
    }

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
        catch (OperationCanceledException) { }
        return null;
    }

    private static void SaveCheckpoint(Partition partition)
    {
        var resources = partition.Resources;
        lock (resources.Ranges.Checkpoints)
        {
            var token = partition.GetResumeToken();
            if (token != null)
                resources.Ranges.Checkpoints[partition.FeedRange] = Convert.ToBase64String(token);
            else if (partition.LastPagingState != null)
                resources.Ranges.Checkpoints[partition.FeedRange] = Convert.ToBase64String(partition.LastPagingState);
        }

        if (partition.Phase == PartitionPhase.Replay)
            PersistFeedRangeContinuation(partition);
    }

    private static void MarkCompleted(Partition partition, PipelineContext ctx)
    {
        var resources = partition.Resources;
        lock (resources.Ranges.Checkpoints)
        {
            resources.Ranges.Checkpoints.Remove(partition.FeedRange);
            resources.Ranges.Completed.Add(partition.FeedRange);
        }
        if (resources.Ranges.Completed.Count >= resources.Ranges.FeedRanges.Count)
        {
            resources.BulkDrainSignal.TrySetResult();
        }
    }
}
