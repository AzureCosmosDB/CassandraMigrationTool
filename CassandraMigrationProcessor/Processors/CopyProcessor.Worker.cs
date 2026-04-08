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
        /// <summary>
        /// Unified worker: reads one page from source, creates
        /// a WorkChunk, recycles the partition back into the
        /// pool (so another worker can read the next page),
        /// then writes rows to target and marks the chunk done.
        /// </summary>
        private async Task RunWorkerAsync(
            int workerId, PipelineContext pctx)
        {
            pctx.Tracker.WorkerStarted();
            try
            {
            await foreach (var partition in pctx.PartitionPool
                .Reader.ReadAllAsync(_cts.Token))
            {
                if (_cts.Token.IsCancellationRequested
                    || Volatile.Read(
                        ref pctx.NonRetriableHitFlag) != 0)
                {
                    lock (pctx.Checkpoints)
                    {
                        pctx.Completed.Add(
                            partition.FeedRange);
                    }
                    TryCloseChannel(pctx);
                    continue;
                }

                if (partition.IsExhausted)
                {
                    lock (pctx.Checkpoints)
                    {
                        pctx.Completed.Add(
                            partition.FeedRange);
                    }
                    TryCloseChannel(pctx);
                    continue;
                }

                bool isLastPage = false;
                try
                {
                    // ── STEP 2: READ one page ───────
                    var readSw = Stopwatch.StartNew();
                    var stmt = new SimpleStatement(
                        BuildSelectCql(
                            pctx.Ctx, partition.FeedRange));
                    stmt.SetPageSize(pctx.ConfiguredPageSize);
                    stmt.SetAutoPage(false);
                    stmt.SetReadTimeoutMillis(60_000);
                    stmt.SetConsistencyLevel(
                        ConsistencyLevel.One);

                    var resumeToken =
                        partition.GetResumeToken();
                    if (resumeToken != null)
                        stmt.SetPagingState(resumeToken);

                    RowSet rs = null;
                    for (int att = 1; att <= 3; att++)
                    {
                        try
                        {
                            rs = await _sourceSession!
                                .ExecuteAsync(stmt)
                                .ConfigureAwait(false);
                            break;
                        }
                        catch (Exception ex) when (
                            att < 3 &&
                            (ex is TimeoutException ||
                             ex.GetType().Name
                                 .Contains("Timeout") ||
                             ex.GetType().Name
                                 .Contains("NoHostAvail")))
                        {
                            _log.WriteLine(
                                $"Read timeout " +
                                $"(att {att}/3)",
                                LogType.Warning);
                            await Task.Delay(
                                att * 5000, _cts.Token);
                        }
                    }

                    if (rs == null)
                    {
                        pctx.WorkerErrors.Add(
                            TaskResult.Retry);
                        isLastPage = true;
                    }
                    else
                    {
                        byte[]? nextPaging = rs.PagingState;

                        // Extract row values
                        var rows = new List<object[]>();
                        int avail = rs
                            .GetAvailableWithoutFetching();
                        int consumed = 0;
                        foreach (var row in rs)
                        {
                            if (consumed >= avail) break;
                            consumed++;
                            var vals =
                                new object[pctx.ColNames.Count];
                            for (int i = 0;
                                i < pctx.ColNames.Count; i++)
                                vals[i] =
                                    row[pctx.ColNames[i]];
                            rows.Add(vals);
                        }

                        Interlocked.Add(
                            ref pctx.TotalRead, rows.Count);
                        readSw.Stop();
                        pctx.Tracker.AddReadTime(
                            readSw.ElapsedMilliseconds);

                        if (rows.Count == 0
                            || nextPaging == null)
                            isLastPage = true;

                        // ── STEP 3+4: Create WorkChunk ──
                        var workChunk =
                            partition.AddChunkAndTrim(
                                nextPaging);

                        if (isLastPage)
                            partition.IsExhausted = true;

                        // ── STEP 5: Recycle partition ────
                        if (!isLastPage)
                        {
                            try
                            {
                                await pctx.PartitionPool
                                    .Writer.WriteAsync(
                                        partition,
                                        _cts.Token);
                            }
                            catch (OperationCanceledException)
                            {
                                isLastPage = true;
                                partition.IsExhausted = true;
                            }
                        }

                        // ── STEP 6: WRITE rows ──────────
                        if (rows.Count > 0)
                        {
                            var writeSw =
                                Stopwatch.StartNew();
                            int writeDone = 0;
                            int writeFail = 0;
                            long semWaitMs = 0;
                            long writeLatencySum = 0;
                            var writeTasks =
                                new List<Task>(rows.Count);

                            foreach (var vals in rows)
                            {
                                if (_cts.Token
                                    .IsCancellationRequested
                                    || Volatile.Read(
                                        ref pctx
                                            .NonRetriableHitFlag)
                                        != 0)
                                    break;
                                long semStart =
                                    Stopwatch.GetTimestamp();
                                await pctx.WriteSem
                                    .WaitAsync(_cts.Token);
                                semWaitMs +=
                                    (Stopwatch.GetTimestamp()
                                        - semStart)
                                    * 1000
                                    / Stopwatch.Frequency;
                                try
                                {
                                    var bound =
                                        pctx.Ps.Bind(vals);
                                    bound
                                        .SetReadTimeoutMillis(
                                            60_000);
                                    bound.SetConsistencyLevel(
                                        ConsistencyLevel
                                            .LocalOne);
                                    var wStart = Stopwatch
                                        .GetTimestamp();
                                    writeTasks.Add(
                                        _targetSession!
                                        .ExecuteAsync(bound)
                                        .ContinueWith(t =>
                                    {
                                        long wElapsed =
                                            (Stopwatch
                                                .GetTimestamp()
                                                - wStart)
                                            * 1000
                                            / Stopwatch
                                                .Frequency;
                                        Interlocked.Add(
                                            ref writeLatencySum,
                                            wElapsed);
                                        pctx.WriteSem
                                            .Release();
                                        if (t.IsFaulted)
                                        {
                                            var ex =
                                                t.Exception!
                                                .InnerException!;
                                            Interlocked
                                                .Increment(
                                                ref pctx
                                                    .TotalFailed);
                                            Interlocked
                                                .Increment(
                                                ref writeFail);
                                            _log.WriteLine(
                                                $"INSERT failed"
                                                + $": {ex.GetType().Name}"
                                                + $": {ex.Message}",
                                                LogType.Error);
                                            if (!IsRetriableWriteError(
                                                ex))
                                                Interlocked
                                                    .Exchange(
                                                    ref pctx
                                                        .NonRetriableHitFlag,
                                                    1);
                                        }
                                        else
                                        {
                                            Interlocked
                                                .Increment(
                                                ref pctx
                                                    .TotalWritten);
                                            Interlocked
                                                .Increment(
                                                ref writeDone);
                                        }
                                    }, TaskContinuationOptions
                                        .ExecuteSynchronously));
                                }
                                catch
                                {
                                    pctx.WriteSem.Release();
                                    throw;
                                }
                            }

                            // Snapshot in-flight before wait
                            pctx.Tracker.SetSemCurrent(
                                pctx.MaxInFlight
                                    - pctx.WriteSem
                                        .CurrentCount);
                            pctx.Tracker.SetPipelineState(
                                pctx.FeedRanges.Count
                                    - pctx.Completed.Count,
                                pctx.ConfiguredPageSize);
                            await Task.WhenAll(writeTasks);

                            // ── STEP 7: Mark completed ──
                            workChunk.IsCompleted = true;

                            writeSw.Stop();
                            pctx.Tracker.AddWriteTime(
                                writeLatencySum,
                                rows.Count);
                            pctx.Tracker.AddSemWaitTime(
                                semWaitMs);
                            pctx.Tracker.AddCopied(
                                writeDone);
                            pctx.Tracker.AddFailed(
                                writeFail);

                            // Estimate data volume
                            long pageBytes = 0;
                            foreach (var r in rows)
                                foreach (var v in r)
                                {
                                    if (v is byte[] b)
                                        pageBytes += b.Length;
                                    else if (v is string s)
                                        pageBytes +=
                                            s.Length * 2;
                                    else if (v != null)
                                        pageBytes += 8;
                                }
                            pctx.Tracker.AddBytes(pageBytes);
                        }
                        else
                        {
                            // Empty page — mark done
                            workChunk.IsCompleted = true;
                        }

                        // Update checkpoint from partition
                        lock (pctx.Checkpoints)
                        {
                            if (partition.IsExhausted)
                            {
                                pctx.Checkpoints.Remove(
                                    partition.FeedRange);
                                pctx.Completed.Add(
                                    partition.FeedRange);
                            }
                            else
                            {
                                var token =
                                    partition.GetResumeToken();
                                if (token != null)
                                    pctx.Checkpoints[
                                        partition.FeedRange] =
                                        Convert.ToBase64String(
                                            token);
                            }
                        }

                        if (partition.IsExhausted)
                        {
                            pctx.Tracker.RangeCompleted(
                                partition.FeedRange,
                                TaskResult.Success);
                            TryCloseChannel(pctx);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    pctx.WorkerErrors.Add(
                        TaskResult.Canceled);
                    if (!partition.IsExhausted)
                    {
                        lock (pctx.Checkpoints)
                        {
                            pctx.Completed.Add(
                                partition.FeedRange);
                        }
                        TryCloseChannel(pctx);
                    }
                }
                catch (Exception ex)
                {
                    _log.WriteLine(
                        $"Worker error: " +
                        $"{ex.GetType().Name}: " +
                        $"{ex.Message}",
                        LogType.Error);
                    pctx.WorkerErrors.Add(TaskResult.Retry);
                    if (!pctx.Completed.Contains(
                        partition.FeedRange))
                    {
                        lock (pctx.Checkpoints)
                        {
                            pctx.Completed.Add(
                                partition.FeedRange);
                        }
                        pctx.Tracker.RangeCompleted(
                            partition.FeedRange,
                            TaskResult.Retry);
                        TryCloseChannel(pctx);
                    }
                }
                finally
                {
                    // Update progress
                    long written = Interlocked.Read(
                        ref pctx.TotalWritten);
                    long failed = Interlocked.Read(
                        ref pctx.TotalFailed);
                    var chunk =
                        pctx.Mu.MigrationChunks[
                            pctx.ChunkIndex];
                    chunk.SourceResultRowCount = written;
                    chunk.TargetInsertedRowCount =
                        written;
                    chunk.TargetFailedRowCount = failed;
                    pctx.Mu.CopyRowsCopied = written;
                    pctx.Mu.CopyRowsPerSecond =
                        pctx.Tracker.RecentSpeed;
                    if (pctx.TotalRowCount > 0)
                    {
                        pctx.Mu.CopyPercent =
                            pctx.InitialPercent +
                            (Math.Min(99.9,
                                (double)written
                                / pctx.TotalRowCount * 100)
                            * pctx.ContributionFactor);
                    }
                    pctx.Mu.UpdateParentJob();

                    // Save checkpoint every 10s
                    long prevTicks = Interlocked.Read(
                        ref pctx.LastCheckpointTicks);
                    var now = DateTime.UtcNow;
                    if ((now.Ticks - prevTicks)
                        / TimeSpan.TicksPerSecond >= 10
                        && Interlocked.CompareExchange(
                            ref pctx.LastCheckpointTicks,
                            now.Ticks, prevTicks)
                            == prevTicks)
                    {
                        MigrationJobContext
                            .SaveMigrationUnit(
                                pctx.Mu, true);
                    }
                }
            }
            }
            finally
            {
                pctx.Tracker.WorkerExited();
            }
        }

        private static string BuildSelectCql(
            ProcessorContext ctx, string range) =>
            $"SELECT * FROM " +
            $"\"{ctx.KeyspaceName}\".\"{ctx.TableName}\"" +
            $" WHERE COSMOS_CHANGEFEED_FROM_START() = true" +
            $" AND COSMOS_FEEDRANGE() = '{range}'";

        private static void TryCloseChannel(
            PipelineContext pctx)
        {
            if (pctx.Completed.Count >= pctx.FeedRanges.Count)
                pctx.PartitionPool.Writer.TryComplete();
        }
    }
}
