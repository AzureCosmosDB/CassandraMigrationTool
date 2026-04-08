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
        private async Task RunReaderAsync(
            int readerId, PipelineContext pctx)
        {
            pctx.Tracker.WorkerStarted();
            try
            {
            await foreach (var state in pctx.WorkCh.Reader
                .ReadAllAsync(_cts.Token))
            {
                if (_cts.Token.IsCancellationRequested
                    || Volatile.Read(
                        ref pctx.NonRetriableHitFlag) != 0)
                {
                    lock (pctx.Checkpoints)
                    {
                        pctx.Completed.Add(state.FeedRange);
                    }
                    TryCloseChannel(pctx);
                    continue;
                }

                bool isLastPage = false;
                bool recycledToChannel = false;
                try
                {
                    // ── READ one page ───────────────
                    var readSw = Stopwatch.StartNew();
                    var stmt = new SimpleStatement(
                        BuildSelectCql(
                            pctx.Ctx, state.FeedRange));
                    stmt.SetPageSize(pctx.ConfiguredPageSize);
                    stmt.SetAutoPage(false);
                    stmt.SetReadTimeoutMillis(60_000);
                    stmt.SetConsistencyLevel(
                        ConsistencyLevel.One);
                    if (state.PagingState != null)
                        stmt.SetPagingState(
                            state.PagingState);

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

                        // Recycle feed range for next page
                        // (do NOT update checkpoint here —
                        // writer does it after confirmed write)
                        if (!isLastPage)
                        {
                            try
                            {
                                await pctx.WorkCh.Writer
                                    .WriteAsync(
                                        new FeedRangeState(
                                            state.FeedRange,
                                            nextPaging),
                                        _cts.Token);
                                recycledToChannel = true;
                            }
                            catch (OperationCanceledException)
                            {
                                isLastPage = true;
                            }
                        }

                        // Signal range completion
                        if (isLastPage)
                        {
                            pctx.Tracker.RangeCompleted(
                                state.FeedRange,
                                TaskResult.Success);
                            TryCloseChannel(pctx);
                        }

                        // Push rows to data channel
                        // for writers to consume
                        if (rows.Count > 0 || isLastPage)
                        {
                            await pctx.DataCh.Writer
                                .WriteAsync(
                                    new ReadPage(
                                        rows,
                                        state.FeedRange,
                                        isLastPage,
                                        readSw.ElapsedMilliseconds,
                                        nextPaging),
                                    _cts.Token);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    pctx.WorkerErrors.Add(
                        TaskResult.Canceled);
                    isLastPage = true;
                }
                catch (Exception ex)
                {
                    _log.WriteLine(
                        $"Reader error: " +
                        $"{ex.GetType().Name}: " +
                        $"{ex.Message}",
                        LogType.Error);
                    pctx.WorkerErrors.Add(TaskResult.Retry);
                    isLastPage = true;
                }
                finally
                {
                    // On error/cancel where range wasn't
                    // recycled: mark it complete so channel
                    // can close. (No checkpoint update —
                    // range will restart on resume.)
                    if (!recycledToChannel
                        && !pctx.Completed.Contains(
                            state.FeedRange))
                    {
                        lock (pctx.Checkpoints)
                        {
                            pctx.Completed.Add(
                                state.FeedRange);
                        }
                        pctx.Tracker.RangeCompleted(
                            state.FeedRange,
                            TaskResult.Retry);
                        TryCloseChannel(pctx);
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
                pctx.WorkCh.Writer.TryComplete();
        }
    }
}
