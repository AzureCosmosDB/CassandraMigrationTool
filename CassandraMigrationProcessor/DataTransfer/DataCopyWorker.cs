using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;
/// <summary>
/// Job-shared worker. Takes a partition (from any table) off the shared
/// channel, reads one page, writes rows, saves checkpoint, re-enqueues
/// or completes. All per-table state (tracker, ranges, drain signal,
/// columns, identifiers) is resolved through <see cref="Partition"/>'s
/// own pass-through accessors so a single pool can service many tables
/// concurrently.
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

    public DataCopyWorker(MigrationLog log, CancellationToken cancellationToken, int workerId)
    {
        ArgumentNullException.ThrowIfNull(log);
        _ct = cancellationToken;
        _workerLog = new WorkerLog(log, workerId);
    }

    public async Task RunAsync(PipelineContext ctx)
    {
        PageReader? reader = null;
        PageWriter? writer = null;
        Partition? current = null;
        try
        {
            reader = await PageReader.CreateAsync(_workerLog, ctx.SessionFactory, ctx.ReaderConfig, _ct);
            writer = await PageWriter.CreateAsync(_workerLog, ctx.SessionFactory, ctx.WriterConfig, _ct);

            while (!_ct.IsCancellationRequested
                && Volatile.Read(ref ctx.Flags.FatalErrorFlag) == 0
                && !MigrationJobContext.Instance.ControlledPauseRequested)
            {
                current = await ctx.Partitions.TakeAsync(_ct);
                if (current == null) break;

                var result = await reader.ReadAsync(current, ctx);
                if (result == null)
                {
                    _workerLog.WriteLine($"FATAL: Read failed for {current.FullTableName} — failing job", LogType.Error);
                    ctx.Flags.TripFatal();
                    break;
                }

                await writer.WriteAsync(result.Rows, result.WorkChunk, current, ctx);

                if (result.WorkChunk.IsCompleted)
                    SaveCheckpoint(current);
                else
                    _workerLog.WriteLine($"Checkpoint NOT advanced for {current.FullTableName} — page had failures", LogType.Warning);

                DispatchAfterPage(current, result, ctx);
                current.Tracker.UpdateMigrationUnit();
                current = null;
            }
        }
        catch (OperationCanceledException)
        {
            ctx.Flags.WorkerErrors.Add(TaskResult.Canceled);
        }
        catch (Exception ex)
        {
            string tag = current?.FullTableName ?? "init";
            LogExceptionChain(tag, ex);

            // Any escaped exception here means the inner retry layers
            // (PageReader read retries, PageWriter row-write retries) are
            // already exhausted. The in-flight partition is dropped — it is
            // not recycled, its bulk-completion counter is not advanced,
            // and its checkpoint did not advance — so continuing the job
            // would either hang the table's BulkDrainSignal forever, or
            // worse, silently mark the table complete with a feed range
            // missing.
            // Trip fatal so the operator can investigate and resume from
            // the last persisted checkpoint.
            _workerLog.WriteLine(
                $"FATAL — worker exhausted retries on {tag}; aborting job to preserve data integrity",
                LogType.Error);
            ctx.Flags.TripFatal();
            ctx.Flags.WorkerErrors.Add(TaskResult.Abort);
        }
        finally
        {
            if (current != null) current.Tracker.UpdateMigrationUnit();
            // Do NOT call ctx.Partitions.Complete() here. The shared
            // partition channel is owned by JobPipeline; closing it from
            // one worker's finally would cut off every other worker
            // (potentially serving different tables) the instant ANY
            // worker exits (planned, faulted, or cancelled). The
            // orchestrator drives channel completion explicitly
            // (CompletePartitionChannel in offline mode, _cts.Cancel
            // in stop / fatal). Workers that pulled the last partition
            // exit via TakeAsync returning null when the orchestrator
            // completes the channel; faulted workers leave other workers
            // alive so they can finish in-flight work.
            MigrationUtilities.SafeDispose(writer, "worker PageWriter");
            MigrationUtilities.SafeDispose(reader, "worker PageReader");
        }
    }

    private void DispatchAfterPage(Partition partition, PageReader.ReadResult result, PipelineContext ctx)
    {
        if (!result.IsEmptyPage)
        {
            // Pool is unbounded: Recycle always succeeds unless the
            // pool was completed by a failing worker, in which case
            // Recycle throws and the worker exits via the outer catch.
            ctx.Partitions.Recycle(partition);
            return;
        }

        // Don't act on an empty page if we're shutting down — a cancelled or
        // fatal-tripped run may return early-empty pages from the driver, and
        // persisting Bulk→Replay or Completed here would silently skip data on
        // resume. Defer to the outer loop; it will exit cleanly.
        if (_ct.IsCancellationRequested
            || Volatile.Read(ref ctx.Flags.FatalErrorFlag) != 0)
        {
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
                MarkCompleted(partition);
            }
            return;
        }

        ScheduleCooldown(partition, ctx);
    }

    private void LogExceptionChain(string tag, Exception ex)
    {
        // Surface the actual root cause(s) — not just the
        // outer wrapper. AggregateException.Message is generic
        // ("One or more errors occurred."), and the driver
        // often surfaces a chain of InnerExceptions whose
        // top-level message hides the underlying CQL / network
        // error. Walk both Flatten() and InnerException to give
        // operators every layer of context.
        _workerLog.WriteLine($"Error on {tag}: {ex.GetType().Name}: {ex.Message}", LogType.Error);

        if (ex is AggregateException agg)
        {
            int i = 0;
            foreach (var inner in agg.Flatten().InnerExceptions)
            {
                _workerLog.WriteLine(
                    $"  caused by [{++i}] {inner.GetType().Name}: {inner.Message}",
                    LogType.Error);
            }
            return;
        }

        var cur = ex.InnerException;
        int depth = 0;
        while (cur != null && depth++ < 5)
        {
            _workerLog.WriteLine(
                $"  caused by {cur.GetType().Name}: {cur.Message}",
                LogType.Error);
            cur = cur.InnerException;
        }
    }

    private static void MarkBulkDrained(Partition partition)
    {
        // Online: bulk drained, flip to replay. Partition handles
        // its own state + notifies TableResources so the table-wide
        // counter and BulkDrainSignal advance — worker does not
        // touch table state directly.
        partition.HandoffToReplay();
    }

    private void ScheduleCooldown(Partition partition, PipelineContext ctx)
    {
        // Hand the partition to the pipeline-owned cooldown scheduler.
        // The worker returns to the pool immediately — no Task.Run, no
        // delay held inside this worker. The scheduler is the single
        // owner of in-flight cooldowns and is drained on shutdown.
        ctx.Cooldown.Schedule(partition);
    }

    private static void SaveCheckpoint(Partition partition)
    {
        partition.SaveCheckpoint(
            partition.GetResumeToken() ?? partition.LastPagingState);
    }

    private static void MarkCompleted(Partition partition)
    {
        // Offline final: partition clears its token + notifies
        // TableResources. Worker stays out of table-level state.
        partition.CompleteOffline();
    }
}
