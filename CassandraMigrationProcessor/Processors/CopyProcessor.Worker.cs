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
            int workerId, PipelineContext pipeline)
        {
            pipeline.Tracker.WorkerStarted();
            ISession? workerTargetSession = null;
            ISession? workerSourceSession = null;
            try
            {
            // Create per-worker sessions (1 conn/host each)
            var job = MigrationJobContext.CurrentlyActiveJob;
            workerTargetSession = CassandraClientFactory
                .CreateTargetSession(_log, job, "");
            workerSourceSession = CassandraClientFactory
                .CreateSourceSession(_log, job,
                    pipeline.Context.KeyspaceName);

            // Prepare INSERT for this worker's session
            var (ps, _) = await CassandraHelper.PrepareInsertAsync(
                workerTargetSession,
                pipeline.Context.TargetKeyspaceName,
                pipeline.Context.TargetTableName,
                pipeline.Columns).ConfigureAwait(false);

            while (!_cts.Token.IsCancellationRequested
                && Volatile.Read(ref pipeline.NonRetriableHitFlag) == 0)
            {
                Partition partition;
                try
                {
                    partition = await pipeline.PartitionPool
                        .Reader.ReadAsync(_cts.Token);
                }
                catch (OperationCanceledException) { break; }
                catch (ChannelClosedException) { break; }

                if (_cts.Token.IsCancellationRequested
                    || Volatile.Read(
                        ref pipeline.NonRetriableHitFlag) != 0)
                {
                    lock (pipeline.Checkpoints)
                    {
                        pipeline.Completed.Add(
                            partition.FeedRange);
                    }
                    TryCloseChannel(pipeline);
                    continue;
                }

                if (partition.IsExhausted)
                {
                    lock (pipeline.Checkpoints)
                    {
                        pipeline.Completed.Add(
                            partition.FeedRange);
                    }
                    TryCloseChannel(pipeline);
                    continue;
                }

                bool isLastPage = false;
                try
                {
                    // ── STEP 2: READ one page ───────
                    var readSw = Stopwatch.StartNew();
                    var stmt = new SimpleStatement(
                        BuildSelectCql(
                            pipeline.Context, partition.FeedRange));
                    stmt.SetPageSize(pipeline.ConfiguredPageSize);
                    stmt.SetAutoPage(false);
                    stmt.SetReadTimeoutMillis(60_000);
                    stmt.SetConsistencyLevel(
                        ConsistencyLevel.One);

                    // Use partition's latest paging state
                    // (not GetResumeToken — that's for
                    // checkpoint persistence only)
                    if (partition.LastPagingState != null)
                        stmt.SetPagingState(
                            partition.LastPagingState);

                    RowSet resultSet = null;
                    for (int attempt = 1; attempt <= 3; attempt++)
                    {
                        try
                        {
                            resultSet = await workerSourceSession!
                                .ExecuteAsync(stmt)
                                .ConfigureAwait(false);
                            break;
                        }
                        catch (Exception ex) when (
                            attempt < 3 &&
                            (ex is TimeoutException ||
                             ex.GetType().Name
                                 .Contains("Timeout") ||
                             ex.GetType().Name
                                 .Contains("NoHostAvail")))
                        {
                            _log.WriteLine(
                                $"Read timeout " +
                                $"(attempt {attempt}/3)",
                                LogType.Warning);
                            await Task.Delay(
                                attempt * 5000, _cts.Token);
                        }
                    }

                    if (resultSet == null)
                    {
                        pipeline.WorkerErrors.Add(
                            TaskResult.Retry);
                        isLastPage = true;
                    }
                    else
                    {
                        byte[]? nextPaging = resultSet.PagingState;
                        // Update partition's read cursor
                        partition.LastPagingState = nextPaging;

                        // Extract row values
                        var rows = new List<object[]>();
                        int avail = resultSet
                            .GetAvailableWithoutFetching();
                        int consumed = 0;
                        foreach (var row in resultSet)
                        {
                            if (consumed >= avail) break;
                            consumed++;
                            var rowValues =
                                new object[pipeline.ColumnNames.Count];
                            for (int i = 0;
                                i < pipeline.ColumnNames.Count; i++)
                                rowValues[i] =
                                    row[pipeline.ColumnNames[i]];
                            rows.Add(rowValues);
                        }

                        Interlocked.Add(
                            ref pipeline.TotalRead, rows.Count);
                        readSw.Stop();
                        pipeline.Tracker.AddReadTime(
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
                                await pipeline.PartitionPool
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
                            long writeLatencySum = 0;
                            var writeTasks =
                                new List<Task>(rows.Count);

                            foreach (var rowValues in rows)
                            {
                                if (_cts.Token
                                    .IsCancellationRequested
                                    || Volatile.Read(
                                        ref pipeline
                                            .NonRetriableHitFlag)
                                        != 0)
                                    break;
                                var bound =
                                    ps.Bind(rowValues);
                                bound
                                    .SetReadTimeoutMillis(
                                        60_000);
                                bound.SetConsistencyLevel(
                                    ConsistencyLevel
                                        .LocalOne);
                                var writeStart = Stopwatch
                                    .GetTimestamp();
                                writeTasks.Add(
                                    workerTargetSession!
                                    .ExecuteAsync(bound)
                                    .ContinueWith(writeTask =>
                                {
                                    long wElapsed =
                                        (Stopwatch
                                            .GetTimestamp()
                                            - writeStart)
                                        * 1000
                                        / Stopwatch
                                            .Frequency;
                                    Interlocked.Add(
                                        ref writeLatencySum,
                                        wElapsed);
                                    if (writeTask.IsFaulted)
                                    {
                                        var ex =
                                            writeTask.Exception!
                                            .InnerException!;
                                        Interlocked
                                            .Increment(
                                            ref pipeline
                                                .TotalFailed);
                                        Interlocked
                                            .Increment(
                                            ref writeFail);
                                        _log.WriteLine(
                                            $"INSERT failed"
                                            + $": {ex.GetType().Name}"
                                            + $": {ex.Message}",
                                            LogType.Error);
                                        if (IsFatalError(ex))
                                        {
                                            _log.WriteLine(
                                                $"FATAL: {ex.GetType().Name}" +
                                                $" — failing job",
                                                LogType.Error);
                                            Interlocked.Exchange(
                                                ref pipeline
                                                    .NonRetriableHitFlag,
                                                1);
                                            try { _cts.Cancel(); }
                                            catch { }
                                        }
                                        else if (!IsRetriableWriteError(
                                            ex))
                                            Interlocked
                                                .Exchange(
                                                ref pipeline
                                                    .NonRetriableHitFlag,
                                                1);
                                    }
                                    else
                                    {
                                        Interlocked
                                            .Increment(
                                            ref pipeline
                                                .TotalWritten);
                                        Interlocked
                                            .Increment(
                                            ref writeDone);
                                    }
                                }, TaskContinuationOptions
                                    .ExecuteSynchronously));
                            }

                            pipeline.Tracker.SetPipelineState(
                                pipeline.FeedRanges.Count
                                    - pipeline.Completed.Count,
                                pipeline.ConfiguredPageSize);
                            await Task.WhenAll(writeTasks);

                            // ── STEP 7: Mark completed ──
                            workChunk.IsCompleted = true;

                            writeSw.Stop();
                            pipeline.Tracker.AddWriteTime(
                                writeLatencySum,
                                rows.Count);
                            pipeline.Tracker.AddCopied(
                                writeDone);
                            pipeline.Tracker.AddFailed(
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
                            pipeline.Tracker.AddBytes(pageBytes);
                        }
                        else
                        {
                            // Empty page — mark done
                            workChunk.IsCompleted = true;
                        }

                        // Update checkpoint from partition
                        lock (pipeline.Checkpoints)
                        {
                            if (partition.IsExhausted)
                            {
                                pipeline.Checkpoints.Remove(
                                    partition.FeedRange);
                                pipeline.Completed.Add(
                                    partition.FeedRange);
                            }
                            else
                            {
                                var token =
                                    partition.GetResumeToken();
                                if (token != null)
                                    pipeline.Checkpoints[
                                        partition.FeedRange] =
                                        Convert.ToBase64String(
                                            token);
                            }
                        }

                        if (partition.IsExhausted)
                        {
                            pipeline.Tracker.RangeCompleted(
                                partition.FeedRange,
                                TaskResult.Success);
                            TryCloseChannel(pipeline);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    pipeline.WorkerErrors.Add(
                        TaskResult.Canceled);
                    if (!partition.IsExhausted)
                    {
                        lock (pipeline.Checkpoints)
                        {
                            pipeline.Completed.Add(
                                partition.FeedRange);
                        }
                        TryCloseChannel(pipeline);
                    }
                }
                catch (Exception ex)
                {
                    _log.WriteLine(
                        $"Worker error: " +
                        $"{ex.GetType().Name}: " +
                        $"{ex.Message}",
                        LogType.Error);

                    if (IsFatalError(ex))
                    {
                        _log.WriteLine(
                            $"FATAL: {ex.GetType().Name}" +
                            $" — failing job",
                            LogType.Error);
                        Interlocked.Exchange(
                            ref pipeline.NonRetriableHitFlag, 1);
                        try { _cts.Cancel(); } catch { }
                        pipeline.WorkerErrors.Add(
                            TaskResult.Abort);
                    }
                    else
                    {
                        pipeline.WorkerErrors.Add(
                            TaskResult.Retry);
                    }

                    if (!pipeline.Completed.Contains(
                        partition.FeedRange))
                    {
                        lock (pipeline.Checkpoints)
                        {
                            pipeline.Completed.Add(
                                partition.FeedRange);
                        }
                        pipeline.Tracker.RangeCompleted(
                            partition.FeedRange,
                            TaskResult.Retry);
                        TryCloseChannel(pipeline);
                    }
                }
                finally
                {
                    // Update progress
                    long written = Interlocked.Read(
                        ref pipeline.TotalWritten);
                    long failed = Interlocked.Read(
                        ref pipeline.TotalFailed);
                    var chunk =
                        pipeline.MigrationUnit.MigrationChunks[
                            pipeline.ChunkIndex];
                    chunk.SourceResultRowCount = written;
                    chunk.TargetInsertedRowCount =
                        written;
                    chunk.TargetFailedRowCount = failed;
                    pipeline.MigrationUnit.CopyRowsCopied = written;
                    pipeline.MigrationUnit.CopyRowsPerSecond =
                        pipeline.Tracker.RecentSpeed;
                    if (pipeline.TotalRowCount > 0)
                    {
                        pipeline.MigrationUnit.CopyPercent =
                            pipeline.InitialPercent +
                            (Math.Min(99.9,
                                (double)written
                                / pipeline.TotalRowCount * 100)
                            * pipeline.ContributionFactor);
                    }
                    pipeline.MigrationUnit.UpdateParentJob();

                    // Save checkpoint every 10s
                    long prevTicks = Interlocked.Read(
                        ref pipeline.LastCheckpointTicks);
                    var now = DateTime.UtcNow;
                    if ((now.Ticks - prevTicks)
                        / TimeSpan.TicksPerSecond >= 10
                        && Interlocked.CompareExchange(
                            ref pipeline.LastCheckpointTicks,
                            now.Ticks, prevTicks)
                            == prevTicks)
                    {
                        MigrationJobContext
                            .SaveMigrationUnit(
                                pipeline.MigrationUnit, true);
                    }
                }
            }
            }
            finally
            {
                try { workerTargetSession?.Dispose(); } catch { }
                try { workerSourceSession?.Dispose(); } catch { }
                pipeline.Tracker.WorkerExited();
            }
        }

        private static string BuildSelectCql(
            ProcessorContext ctx, string range) =>
            $"SELECT * FROM " +
            $"\"{ctx.KeyspaceName}\".\"{ctx.TableName}\"" +
            $" WHERE COSMOS_CHANGEFEED_FROM_START() = true" +
            $" AND COSMOS_FEEDRANGE() = '{range}'";

        private static void TryCloseChannel(
            PipelineContext pipeline)
        {
            if (pipeline.Completed.Count >= pipeline.FeedRanges.Count)
                pipeline.PartitionPool.Writer.TryComplete();
        }
    }
}
