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
        private async Task RunWriterAsync(
            int writerId, PipelineContext pctx)
        {
            await foreach (var page in pctx.DataCh.Reader
                .ReadAllAsync(_cts.Token))
            {
                try
                {
                    // ── WRITE rows concurrently ──
                    var writeSw = Stopwatch.StartNew();
                    int writeDone = 0;
                    int writeFail = 0;
                    long semWaitMs = 0;
                    long writeLatencySum = 0;
                    var writeTasks =
                        new List<Task>(page.Rows.Count);

                    foreach (var vals in page.Rows)
                    {
                        if (_cts.Token
                            .IsCancellationRequested
                            || Volatile.Read(
                                ref pctx.NonRetriableHitFlag)
                                != 0)
                            break;
                        long semStart =
                            Stopwatch.GetTimestamp();
                        await pctx.WriteSem.WaitAsync(
                            _cts.Token);
                        semWaitMs +=
                            (Stopwatch.GetTimestamp()
                                - semStart)
                            * 1000
                            / Stopwatch.Frequency;
                        try
                        {
                            var bound = pctx.Ps.Bind(vals);
                            bound
                                .SetReadTimeoutMillis(
                                    60_000);
                            bound.SetConsistencyLevel(
                                ConsistencyLevel.LocalOne);
                            var wStart =
                                Stopwatch.GetTimestamp();
                            writeTasks.Add(
                                _targetSession!
                                .ExecuteAsync(bound)
                                .ContinueWith(t =>
                            {
                                long wElapsed =
                                    (Stopwatch.GetTimestamp()
                                        - wStart)
                                    * 1000
                                    / Stopwatch.Frequency;
                                Interlocked.Add(
                                    ref writeLatencySum,
                                    wElapsed);
                                pctx.WriteSem.Release();
                                if (t.IsFaulted)
                                {
                                    var ex =
                                        t.Exception!
                                        .InnerException!;
                                    Interlocked
                                        .Increment(
                                        ref pctx.TotalFailed);
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
                                        Interlocked.Exchange(
                                            ref pctx
                                                .NonRetriableHitFlag,
                                            1);
                                }
                                else
                                {
                                    Interlocked
                                        .Increment(
                                        ref pctx.TotalWritten);
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
                    // Snapshot in-flight before
                    // waiting for completion
                    pctx.Tracker.SetSemCurrent(
                        pctx.MaxInFlight
                            - pctx.WriteSem.CurrentCount);
                    pctx.Tracker.SetPipelineState(
                        pctx.FeedRanges.Count
                            - pctx.Completed.Count,
                        pctx.ConfiguredPageSize);
                    await Task.WhenAll(writeTasks);
                    writeSw.Stop();
                    pctx.Tracker.AddWriteTime(
                        writeLatencySum,
                        page.Rows.Count);
                    pctx.Tracker.AddSemWaitTime(semWaitMs);
                    pctx.Tracker.AddCopied(writeDone);
                    pctx.Tracker.AddFailed(writeFail);

                    // Estimate data volume
                    long pageBytes = 0;
                    foreach (var r in page.Rows)
                        foreach (var v in r)
                        {
                            if (v is byte[] b)
                                pageBytes += b.Length;
                            else if (v is string s)
                                pageBytes += s.Length * 2;
                            else if (v != null)
                                pageBytes += 8;
                        }
                    pctx.Tracker.AddBytes(pageBytes);

                    // Update checkpoint AFTER writes confirmed
                    // so crash doesn't skip unwritten rows
                    lock (pctx.Checkpoints)
                    {
                        if (page.IsLastPage)
                        {
                            pctx.Checkpoints.Remove(
                                page.FeedRange);
                            pctx.Completed.Add(
                                page.FeedRange);
                        }
                        else if (page.NextPagingState != null)
                        {
                            pctx.Checkpoints[
                                page.FeedRange] =
                                Convert.ToBase64String(
                                    page.NextPagingState);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Writer cancelled
                }
                catch (Exception ex)
                {
                    _log.WriteLine(
                        $"Writer error: " +
                        $"{ex.GetType().Name}: " +
                        $"{ex.Message}",
                        LogType.Error);
                    pctx.WorkerErrors.Add(TaskResult.Retry);
                    // Signal readers to stop if
                    // writer has a fatal error
                    if (!IsRetriableWriteError(ex))
                        Interlocked.Exchange(
                            ref pctx.NonRetriableHitFlag, 1);
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
    }
}
