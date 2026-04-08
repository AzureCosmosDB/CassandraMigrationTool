using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            int workerId, PipelineContext ctx)
        {
            ctx.Tracker.WorkerStarted();
            ISession? workerTargetSession = null;
            ISession? workerSourceSession = null;
            try
            {
                var job = ctx.Job;
                workerTargetSession = CassandraClientFactory
                    .CreateTargetSession(_log, job, "");
                workerSourceSession = CassandraClientFactory
                    .CreateSourceSession(_log, job,
                        ctx.Context.KeyspaceName);

                var (preparedInsert, _) = await CassandraHelper
                    .PrepareInsertAsync(
                        workerTargetSession,
                        ctx.Context.TargetKeyspaceName,
                        ctx.Context.TargetTableName,
                        ctx.Columns).ConfigureAwait(false);

                while (!_cancellation.Token.IsCancellationRequested
                    && Volatile.Read(ref ctx.FatalErrorFlag) == 0)
                {
                    Partition partition;
                    try
                    {
                        partition = await ctx.PartitionPool
                            .Reader.ReadAsync(_cancellation.Token);
                    }
                    catch (OperationCanceledException) { break; }
                    catch (ChannelClosedException) { break; }

                    if (_cancellation.Token.IsCancellationRequested
                        || Volatile.Read(
                            ref ctx.FatalErrorFlag) != 0)
                    {
                        // Save checkpoint but do NOT mark the
                        // range as completed — it still has
                        // uncopied data. Closing the channel
                        // lets all workers drain and exit.
                        lock (ctx.Checkpoints)
                        {
                            var token =
                                partition.GetResumeToken();
                            if (token != null)
                                ctx.Checkpoints[
                                    partition.FeedRange] =
                                    Convert.ToBase64String(
                                        token);
                            else if (
                                partition.LastPagingState != null)
                                ctx.Checkpoints[
                                    partition.FeedRange] =
                                    Convert.ToBase64String(
                                        partition.LastPagingState);
                        }
                        ctx.PartitionPool.Writer.TryComplete();
                        continue;
                    }

                    if (partition.IsExhausted)
                    {
                        lock (ctx.Checkpoints)
                        {
                            ctx.Completed.Add(
                                partition.FeedRange);
                        }
                        TryCloseChannel(ctx);
                        continue;
                    }

                    bool isLastPage = false;
                    try
                    {
                        var (rows, nextPaging, lastPage, readTimeMs) =
                            await ReadPageAsync(
                                partition, workerSourceSession!, ctx,
                                workerId);

                        if (rows == null)
                        {
                            // Read failed after all retries —
                            // DO NOT skip this range. Mark as
                            // error so job fails instead of
                            // silently losing data.
                            _log.WriteLine(
                                $"[W{workerId}] FATAL: Read " +
                                $"failed after {MaxReadRetries} " +
                                $"retries for range " +
                                $"{TruncRange(partition.FeedRange)}" +
                                $" — failing job to prevent " +
                                $"data loss",
                                LogType.Error);
                            Interlocked.Exchange(
                                ref ctx.FatalErrorFlag, 1);
                            try { _cancellation.Cancel(); }
                            catch { }
                            break;
                        }

                        isLastPage = lastPage;
                        partition.LastPagingState = nextPaging;
                        Interlocked.Add(
                            ref ctx.TotalRead, rows.Count);
                        ctx.Tracker.AddReadTime(readTimeMs);

                        var workChunk =
                            partition.AddChunkAndTrim(nextPaging);

                        if (isLastPage)
                            partition.IsExhausted = true;

                        if (!isLastPage)
                        {
                            try
                            {
                                await ctx.PartitionPool
                                    .Writer.WriteAsync(
                                        partition, _cancellation.Token);
                            }
                            catch (OperationCanceledException)
                            {
                                isLastPage = true;
                                partition.IsExhausted = true;
                            }
                        }

                        if (rows.Count > 0)
                        {
                            await WriteRowsAsync(
                                rows, preparedInsert,
                                workerTargetSession!, workChunk,
                                ctx, workerId);
                        }
                        else
                        {
                            workChunk.IsCompleted = true;
                        }

                        lock (ctx.Checkpoints)
                        {
                            if (partition.IsExhausted)
                            {
                                ctx.Checkpoints.Remove(
                                    partition.FeedRange);
                                ctx.Completed.Add(
                                    partition.FeedRange);
                            }
                            else
                            {
                                var token =
                                    partition.GetResumeToken();
                                if (token != null)
                                    ctx.Checkpoints[
                                        partition.FeedRange] =
                                        Convert.ToBase64String(
                                            token);
                            }
                        }

                        if (partition.IsExhausted)
                        {
                            ctx.Tracker.RangeCompleted(
                                partition.FeedRange,
                                TaskResult.Success);
                            TryCloseChannel(ctx);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        ctx.WorkerErrors.Add(
                            TaskResult.Canceled);
                        if (!partition.IsExhausted)
                        {
                            // Save checkpoint but do NOT mark
                            // the range as completed — resume
                            // needs to re-process it.
                            lock (ctx.Checkpoints)
                            {
                                var token =
                                    partition.GetResumeToken();
                                if (token != null)
                                    ctx.Checkpoints[
                                        partition.FeedRange] =
                                        Convert.ToBase64String(
                                            token);
                                else if (
                                    partition.LastPagingState
                                    != null)
                                    ctx.Checkpoints[
                                        partition.FeedRange] =
                                        Convert.ToBase64String(
                                            partition
                                                .LastPagingState);
                            }
                            ctx.PartitionPool.Writer
                                .TryComplete();
                        }
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine(
                            $"[W{workerId}] Worker error: " +
                            $"{ex.GetType().Name}: " +
                            $"{ex.Message}",
                            LogType.Error);

                        if (IsFatalError(ex))
                        {
                            _log.WriteLine(
                                $"[W{workerId}] FATAL: {ex.GetType().Name}" +
                                $" — failing job",
                                LogType.Error);
                            Interlocked.Exchange(
                                ref ctx.FatalErrorFlag, 1);
                            try { _cancellation.Cancel(); }
                            catch (Exception cancelEx)
                            {
                                Console.WriteLine($"[WARN] CopyProcessor cancel failed: {cancelEx.Message}");
                            }
                            ctx.WorkerErrors.Add(
                                TaskResult.Abort);
                        }
                        else
                        {
                            ctx.WorkerErrors.Add(
                                TaskResult.Retry);
                        }

                        if (!ctx.Completed.Contains(
                            partition.FeedRange))
                        {
                            // Save checkpoint for the failed
                            // range so resume can retry from
                            // the last good position. Do NOT
                            // mark as completed — the range
                            // still has uncopied data.
                            lock (ctx.Checkpoints)
                            {
                                var token =
                                    partition.GetResumeToken();
                                if (token != null)
                                    ctx.Checkpoints[
                                        partition.FeedRange] =
                                        Convert.ToBase64String(
                                            token);
                                else if (
                                    partition.LastPagingState
                                    != null)
                                    ctx.Checkpoints[
                                        partition.FeedRange] =
                                        Convert.ToBase64String(
                                            partition
                                                .LastPagingState);
                            }
                            ctx.Tracker.RangeCompleted(
                                partition.FeedRange,
                                TaskResult.Retry);
                            // Close channel so workers drain
                            // and the pipeline can return the
                            // error to the retry helper.
                            ctx.PartitionPool.Writer
                                .TryComplete();
                        }
                    }
                    finally
                    {
                        long written = Interlocked.Read(
                            ref ctx.TotalWritten);
                        long failed = Interlocked.Read(
                            ref ctx.TotalFailed);
                        var chunk =
                            ctx.MigrationUnit.MigrationChunks[
                                ctx.ChunkIndex];
                        chunk.SourceResultRowCount = written;
                        chunk.TargetInsertedRowCount = written;
                        chunk.TargetFailedRowCount = failed;
                        ctx.MigrationUnit.CopyRowsCopied = written;
                        ctx.MigrationUnit.CopyRowsPerSecond =
                            ctx.Tracker.RecentSpeed;
                        if (ctx.TotalRowCount > 0)
                        {
                            ctx.MigrationUnit.CopyPercent =
                                ctx.InitialPercent +
                                (Math.Min(99.9,
                                    (double)written
                                    / ctx.TotalRowCount * 100)
                                * ctx.ContributionFactor);
                        }
                        ctx.MigrationUnit.UpdateParentJob();

                        long prevTicks = Interlocked.Read(
                            ref ctx.LastCheckpointTicks);
                        var now = DateTime.UtcNow;
                        if ((now.Ticks - prevTicks)
                            / TimeSpan.TicksPerSecond >= 10
                            && Interlocked.CompareExchange(
                                ref ctx.LastCheckpointTicks,
                                now.Ticks, prevTicks)
                                == prevTicks)
                        {
                            MigrationJobContext
                                .SaveMigrationUnit(
                                    ctx.MigrationUnit, true);
                        }
                    }
                }
            }
            finally
            {
                try { workerTargetSession?.Dispose(); }
                catch (Exception ex) { Console.WriteLine($"[WARN] CopyProcessor worker target session dispose failed: {ex.Message}"); }
                try { workerSourceSession?.Dispose(); }
                catch (Exception ex) { Console.WriteLine($"[WARN] CopyProcessor worker source session dispose failed: {ex.Message}"); }
                ctx.Tracker.WorkerExited();
            }
        }

        /// <summary>
        /// Reads a single page of rows from the source
        /// partition, retrying on transient timeouts.
        /// Returns null rows when all retries are exhausted.
        /// </summary>
        private async Task<(List<object[]>? rows,
            byte[]? nextPaging, bool isLastPage,
            long readTimeMs)> ReadPageAsync(
            Partition partition,
            ISession sourceSession,
            PipelineContext ctx,
            int workerId)
        {
            var stopwatch = Stopwatch.StartNew();
            var stmt = new SimpleStatement(
                BuildSelectCql(
                    ctx.Context, partition.FeedRange));
            stmt.SetPageSize(ctx.ConfiguredPageSize);
            stmt.SetAutoPage(false);
            stmt.SetReadTimeoutMillis(ReadTimeoutMs);
            stmt.SetConsistencyLevel(ConsistencyLevel.One);

            if (partition.LastPagingState != null)
                stmt.SetPagingState(
                    partition.LastPagingState);

            RowSet resultSet = null;
            for (int attempt = 1;
                attempt <= MaxReadRetries; attempt++)
            {
                try
                {
                    resultSet = await sourceSession
                        .ExecuteAsync(stmt)
                        .ConfigureAwait(false);
                    break;
                }
                catch (Exception ex) when (
                    attempt < MaxReadRetries &&
                    (ex is TimeoutException ||
                     ex.GetType().Name
                         .Contains("Timeout") ||
                     ex.GetType().Name
                         .Contains("NoHostAvail")))
                {
                    _log.WriteLine(
                        $"[W{workerId}] Read timeout " +
                        $"(attempt {attempt}/{MaxReadRetries})",
                        LogType.Warning);
                    await Task.Delay(
                        attempt * RetryDelayMs, _cancellation.Token);
                }
            }

            if (resultSet == null)
            {
                ctx.WorkerErrors.Add(TaskResult.Retry);
                stopwatch.Stop();
                return (null, null, true,
                    stopwatch.ElapsedMilliseconds);
            }

            byte[]? nextPaging = resultSet.PagingState;

            var rows = new List<object[]>();
            int available =
                resultSet.GetAvailableWithoutFetching();
            int consumed = 0;
            foreach (var row in resultSet)
            {
                if (consumed >= available) break;
                consumed++;
                var rowValues =
                    new object[ctx.ColumnNames.Count];
                for (int i = 0;
                    i < ctx.ColumnNames.Count; i++)
                    rowValues[i] =
                        row[ctx.ColumnNames[i]];
                rows.Add(rowValues);
            }

            stopwatch.Stop();
            bool isLastPage =
                rows.Count == 0 || nextPaging == null;
            return (rows, nextPaging, isLastPage,
                stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Writes extracted rows to the target cluster in
        /// parallel, tracking progress and handling errors.
        /// </summary>
        private async Task WriteRowsAsync(
            List<object[]> rows,
            PreparedStatement preparedInsert,
            ISession targetSession,
            WorkChunk workChunk,
            PipelineContext ctx,
            int workerId)
        {
            var stopwatch = Stopwatch.StartNew();
            int writeDone = 0;
            int writeFail = 0;
            long writeLatencySum = 0;
            var writeTasks = new List<Task>(rows.Count);

            foreach (var rowValues in rows)
            {
                if (_cancellation.Token.IsCancellationRequested
                    || Volatile.Read(
                        ref ctx.FatalErrorFlag) != 0)
                    break;

                var bound = preparedInsert.Bind(rowValues);
                bound.SetReadTimeoutMillis(ReadTimeoutMs);
                bound.SetConsistencyLevel(
                    ConsistencyLevel.LocalOne);

                var writeStart = Stopwatch.GetTimestamp();
                writeTasks.Add(
                    targetSession
                    .ExecuteAsync(bound)
                    .ContinueWith(task =>
                {
                    long elapsed =
                        (Stopwatch.GetTimestamp()
                            - writeStart)
                        * 1000
                        / Stopwatch.Frequency;
                    Interlocked.Add(
                        ref writeLatencySum, elapsed);

                    if (task.IsFaulted)
                    {
                        var ex =
                            task.Exception!
                            .InnerException!;
                        Interlocked.Increment(
                            ref ctx.TotalFailed);
                        Interlocked.Increment(
                            ref writeFail);
                        _log.WriteLine(
                            $"[W{workerId}] INSERT failed"
                            + $": {ex.GetType().Name}"
                            + $": {ex.Message}",
                            LogType.Error);

                        if (IsFatalError(ex))
                        {
                            _log.WriteLine(
                                $"[W{workerId}] FATAL: {ex.GetType().Name}" +
                                $" — failing job",
                                LogType.Error);
                            Interlocked.Exchange(
                                ref ctx.FatalErrorFlag, 1);
                            try { _cancellation.Cancel(); }
                            catch (Exception cancelEx)
                            {
                                Console.WriteLine($"[WARN] CopyProcessor batch cancel failed: {cancelEx.Message}");
                            }
                        }
                        else if (!IsRetriableWriteError(ex))
                        {
                            Interlocked.Exchange(
                                ref ctx.FatalErrorFlag, 1);
                        }
                    }
                    else
                    {
                        Interlocked.Increment(
                            ref ctx.TotalWritten);
                        Interlocked.Increment(
                            ref writeDone);
                    }
                }, TaskContinuationOptions
                    .ExecuteSynchronously));
            }

            ctx.Tracker.SetPipelineState(
                ctx.FeedRanges.Count
                    - ctx.Completed.Count,
                ctx.ConfiguredPageSize);
            await Task.WhenAll(writeTasks);

            // Only mark chunk completed if ALL rows succeeded.
            // Failed rows mean this page must be retried on resume.
            if (writeFail == 0)
            {
                workChunk.IsCompleted = true;
            }
            else
            {
                _log.WriteLine(
                    $"[W{workerId}] {writeFail}/{rows.Count}" +
                    $" writes failed — checkpoint NOT advanced" +
                    $" (will retry on resume)",
                    LogType.Warning);
            }

            stopwatch.Stop();
            ctx.Tracker.AddWriteTime(
                writeLatencySum, rows.Count);
            ctx.Tracker.AddCopied(writeDone);
            ctx.Tracker.AddFailed(writeFail);

            long pageBytes = 0;
            foreach (var r in rows)
                foreach (var v in r)
                {
                    if (v is byte[] b)
                        pageBytes += b.Length;
                    else if (v is string s)
                        pageBytes += s.Length * 2;
                    else if (v != null)
                        pageBytes += 8;
                }
            ctx.Tracker.AddBytes(pageBytes);
        }

        private static string BuildSelectCql(
            ProcessorContext context, string range) =>
            $"SELECT * FROM " +
            $"\"{context.KeyspaceName}\".\"{context.TableName}\"" +
            $" WHERE COSMOS_CHANGEFEED_FROM_START() = true" +
            $" AND COSMOS_FEEDRANGE() = '{range}'";

        private static void TryCloseChannel(
            PipelineContext ctx)
        {
            if (ctx.Completed.Count >= ctx.FeedRanges.Count)
                ctx.PartitionPool.Writer.TryComplete();
        }
    }
}