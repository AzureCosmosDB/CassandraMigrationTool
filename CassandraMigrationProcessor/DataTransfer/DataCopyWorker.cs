using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;
/// <summary>
/// Job-shared worker. Takes a partition (from any table) off the
/// shared channel, reads a page, writes rows, saves checkpoint,
/// re-enqueues or completes. Per-table state is reached through
/// <see cref="Partition"/>'s pass-through accessors.
/// <list type="bullet">
///   <item><b>Bulk</b>: drain the snapshot. First empty page either
///         transitions to Replay (online) or completes (offline).</item>
///   <item><b>Replay</b>: tail the change feed. Replay partitions
///         never complete; the pool runs until cancellation.</item>
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
                && !ctx.Control.IsFatal)
            {
                current = await ctx.Partitions.TakeAsync(_ct);
                if (current == null) break;

                var result = await reader.ReadAsync(current);
                if (result == null)
                {
                    // PageReader exhausted its retry budget on a
                    // transient/throttle error. LastPagingState was NOT
                    // advanced, so re-queue via cooldown.
                    // Per-partition cap: a partition that burns through
                    // MaxConsecutiveReadRetryExhaustions cycles in a
                    // row surfaces as a job-wide fatal instead of
                    // silently never progressing.
                    int exhaustions = current.RecordReadRetryExhaustion();
                    if (exhaustions >= Partition.MaxConsecutiveReadRetryExhaustions)
                    {
                        var msg = $"Source read retries exhausted ({exhaustions} consecutive cycles on partition {current.FeedRange}) for table {current.Table.FullTableName}. Aborting migration job (source likely unavailable or rate-limited beyond recovery).";
                        _workerLog.WriteLine($"FATAL: {msg}", LogType.Error);
                        ctx.Control.ReportFault(new MigrationFatalException(msg, reader.LastRetryExhaustionException));
                        break;
                    }
                    _workerLog.WriteLine(
                        $"Read retries exhausted on {current.Table.FullTableName} " +
                        $"(transient; attempt {exhaustions}/{Partition.MaxConsecutiveReadRetryExhaustions}); re-queuing via cooldown",
                        LogType.Warning);
                    // Skip recycle during graceful/fatal shutdown — the
                    // outer loop will exit anyway, and recycling now
                    // races the PartitionManager disposal path.
                    if (_ct.IsCancellationRequested || ctx.Control.IsFatal)
                    {
                        current = null;
                        break;
                    }
                    ctx.Partitions.RecycleAfterCooldown(current);
                    current.Table.Tracker.UpdateMigrationUnit();
                    current = null;
                    continue;
                }

                current.ResetReadRetryExhaustions();

                // Stamp Last-Checked on every replay poll, including
                // the tip-of-stream empty page, so a quiet source
                // doesn't freeze the dashboard timer.
                if (current.Phase == PartitionPhase.Replay)
                    current.Table.Tracker.MarkReplayPolled();

                await writer.WriteAsync(result.Rows, result.WorkChunk, current, ctx);

                if (result.WorkChunk.IsCompleted)
                {
                    SaveCheckpoint(current);
                }
                else if (_ct.IsCancellationRequested
                    || ctx.Control.IsFatal)
                {
                    // Graceful shutdown mid-page; outer loop exits next.
                    break;
                }
                else
                {
                    // Per-row retries exhausted with real failures. Any
                    // page we can't fully write is fatal: continuing
                    // would let a subsequent empty-page read flip
                    // BulkCompleted=true and silently mark the table
                    // done with failed rows missing.
                    var msg = $"Target write retries exhausted for table {current.Table.FullTableName}. Aborting migration job (resume to re-attempt).";
                    _workerLog.WriteLine($"FATAL: {msg}", LogType.Error);
                    ctx.Control.ReportFault(new MigrationFatalException(msg, writer.LastWriteException));
                    break;
                }

                DispatchAfterPage(current, result, ctx);
                current.Table.Tracker.UpdateMigrationUnit();
                current = null;
            }
        }
        catch (OperationCanceledException)
        {
            // Cancellation observed — no fault to report; the cancel is
            // either user-driven (pause/stop/cutover) or already a
            // cascade from a sibling worker's ReportFault.
        }
        catch (Exception ex)
        {
            string tag = current?.Table.FullTableName ?? "init";
            LogExceptionChain(tag, ex);

            // Any escaped exception here means the inner retry layers
            // are exhausted. The in-flight partition is dropped (not
            // recycled, counter not advanced, checkpoint not advanced)
            // so we report fault to abort cleanly rather than silently
            // marking a table complete with a feed range missing.
            string operatorMsg;
            if (IsOutOfMemory(ex))
            {
                // OOM typically means the shared worker pool was sized
                // too large for the host. Translate into sizing
                // guidance.
                operatorMsg = $"Worker out-of-memory while processing {tag}. The shared worker pool is too large for this host's available memory. Reduce the 'Shared workers' value on the Advanced tab (try halving it, or leave blank for auto) and resume from the last checkpoint.";
            }
            else
            {
                operatorMsg = $"Worker exhausted retries for table {tag}. Aborting migration job (resume from last checkpoint).";
            }
            _workerLog.WriteLine($"FATAL: {operatorMsg}", LogType.Error);
            ctx.Control.ReportFault(new MigrationFatalException(operatorMsg, ex));
        }
        finally
        {
            // ForceFlush (not just UpdateMigrationUnit) so the on-disk
            // CopyRowsCopied lands at the very last row this worker wrote
            // — without this, a Pause taking effect mid-checkpoint-cycle
            // would persist the previous (older) cumulative count, and on
            // Resume the homepage "Rows Copied" counter visibly rewinds
            // for ~60-90s. Belt-and-braces: pages that completed inside
            // the loop also call UpdateMigrationUnit, so the cadence-based
            // checkpoint still runs; this final ForceFlush only matters
            // when the worker exited via cancellation between checkpoints.
            if (current != null) current.Table.Tracker.ForceFlush();
            // Do NOT call ctx.Partitions.Complete() here. The shared
            // channel is owned by JobPipeline; closing it from one
            // worker's finally would cut off every other worker the
            // instant any worker exits. Channel completion is driven
            // by the orchestrator (offline) or _cts.Cancel (fatal).
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
            || ctx.Control.IsFatal)
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
        // Surface root causes — the driver wraps CQL/network errors in
        // chains of inner exceptions whose top-level message is
        // generic. Walk Flatten() and InnerException.
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

    /// <summary>
    /// True iff <paramref name="ex"/> is — or wraps anywhere in its
    /// inner-exception chain — an <see cref="OutOfMemoryException"/>.
    /// The Cassandra driver often wraps raw OOMs in driver-level
    /// exceptions (NoHostAvailableException, ReadTimeoutException,
    /// AggregateException), so a plain type-check misses them.
    /// </summary>
    private static bool IsOutOfMemory(Exception? ex)
    {
        int depth = 0;
        while (ex != null && depth++ < 8)
        {
            if (ex is OutOfMemoryException) return true;
            if (ex is AggregateException agg)
            {
                foreach (var inner in agg.Flatten().InnerExceptions)
                {
                    if (IsOutOfMemory(inner)) return true;
                }
                return false;
            }
            ex = ex.InnerException;
        }
        return false;
    }

    private static void MarkBulkDrained(Partition partition)
    {
        // Online: bulk drained, flip to replay. Partition handles its
        // own state and notifies TableResources.
        partition.HandoffToReplay();
    }

    private void ScheduleCooldown(Partition partition, PipelineContext ctx)
    {
        // Hand the partition to the cooldown scheduler. The worker
        // returns to the pool immediately. The scheduler owns
        // in-flight cooldowns and is drained on shutdown.
        ctx.Partitions.RecycleAfterCooldown(partition);
    }

    private static void SaveCheckpoint(Partition partition)
    {
        partition.SaveCheckpoint(
            partition.GetResumeToken() ?? partition.LastPagingState);
    }

    private static void MarkCompleted(Partition partition)
    {
        // Offline final: partition clears its token and notifies
        // TableResources.
        partition.CompleteOffline();
    }
}
