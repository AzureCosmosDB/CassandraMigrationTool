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
    /// <summary>
    /// Copies rows from source Cassandra (Cosmos DB) to
    /// target Cassandra (OSS) using the DataStax driver.
    /// </summary>
    internal class CopyProcessor : MigrationProcessor
    {
        public CopyProcessor(
            Log log,
            ISession sourceSession,
            MigrationSettings config,
            MigrationWorker? migrationWorker = null)
            : base(log, sourceSession, config, migrationWorker)
        {
            MigrationJobContext.AddVerboseLog(
                "CopyProcessor: Constructor called");
        }

        private Task<TaskResult> CopyProcess_ExceptionHandler(
            Exception ex,
            int attemptCount,
            string processName,
            string keyspace,
            string table,
            int chunkIndex,
            int currentBackoff)
        {
            Console.WriteLine(
                $"  CHUNK ERROR: {keyspace}.{table}[{chunkIndex}] " +
                $"attempt={attemptCount}: {ex.Message}");

            if (ex is OperationCanceledException)
            {
                return Task.FromResult(TaskResult.Abort);
            }
            else
            {
                _log.WriteLine(
                    $"{processName} attempt {attemptCount} for " +
                    $"{keyspace}.{table}[{chunkIndex}] failed. " +
                    $"Details:{ex}. Retrying in {currentBackoff}s...",
                    LogType.Error);
                return Task.FromResult(TaskResult.Retry);
            }
        }

        private async Task<TaskResult> ProcessChunkAsync(
            MigrationUnit mu,
            int chunkIndex,
            ProcessorContext ctx,
            double initialPercent,
            double contributionFactor)
        {
            MigrationJobContext.AddVerboseLog(
                $"CopyProcessor.ProcessChunkAsync: " +
                $"{mu.KeyspaceName}.{mu.TableName}[{chunkIndex}]");

            Console.WriteLine(
                $"  GetRowCount: {ctx.KeyspaceName}.{ctx.TableName}...");
            _log.WriteLine(
                $"Counting source documents for " +
                $"{ctx.KeyspaceName}.{ctx.TableName} " +
                $"(SELECT COUNT(*) with 120s timeout)...");
            long rowCount = await CassandraHelper.GetRowCountAsync(
                ctx.SourceSession,
                ctx.KeyspaceName,
                ctx.TableName)
                .ConfigureAwait(false);
            Console.WriteLine(
                $"  RowCount={rowCount}");
            _log.WriteLine(
                rowCount >= 0
                    ? $"Source document count: {rowCount:N0} " +
                      $"for {ctx.KeyspaceName}.{ctx.TableName}"
                    : $"Could not determine document count " +
                      $"for {ctx.KeyspaceName}.{ctx.TableName} " +
                      $"(COUNT timed out)");

            // Persist row count on migration unit
            if (rowCount > 0)
            {
                mu.EstimatedRowCount = rowCount;
                mu.UpdateParentJob();
            }

            mu.MigrationChunks[chunkIndex].SourceQueryRowCount =
                rowCount;
            ctx.DownloadCount += rowCount;

            _log.WriteLine(
                $"Count for {ctx.KeyspaceName}.{ctx.TableName}" +
                $"[{chunkIndex}] is {rowCount}");

            if (_targetSession == null
                && !MigrationJobContext
                    .CurrentlyActiveJob.IsSimulatedRun)
            {
                var job = MigrationJobContext.CurrentlyActiveJob;
                Console.WriteLine($"CopyProcessor: Creating target session for {ctx.TargetKeyspaceName}");
                _targetSession = CassandraClientFactory
                    .CreateTargetSession(
                        _log, job,
                        string.Empty);
                await CassandraHelper.EnsureKeyspaceExistsAsync(
                    _targetSession,
                    ctx.TargetKeyspaceName)
                    .ConfigureAwait(false);
                Console.WriteLine($"CopyProcessor: Target session ready for {ctx.TargetKeyspaceName}");
            }

            Console.WriteLine(
                $"  Starting CopyRowsAsync: {rowCount} rows...");

            // Discover feed ranges for parallel copy
            _log.WriteLine(
                $"Discovering feed ranges for " +
                $"{ctx.KeyspaceName}.{ctx.TableName}...");
            var feedRanges = await CassandraHelper.GetFeedRangesAsync(
                _sourceSession!,
                ctx.KeyspaceName,
                ctx.TableName)
                .ConfigureAwait(false);
            _log.WriteLine(
                $"Found {feedRanges.Count} feed ranges " +
                $"for {ctx.KeyspaceName}.{ctx.TableName}");

            TaskResult result;
            if (feedRanges.Count > 1)
            {
                _log.WriteLine(
                    $"Parallel copy: {feedRanges.Count} " +
                    $"feed ranges for " +
                    $"{ctx.KeyspaceName}.{ctx.TableName}");
                result = await CopyWithFeedRangesAsync(
                    mu, chunkIndex,
                    initialPercent, contributionFactor,
                    rowCount, ctx, feedRanges);
            }
            else
            {
                var copier = new DocumentCopyWorker();
                copier.Initialize(
                    _log,
                    _sourceSession!,
                    _targetSession!,
                    ctx.KeyspaceName,
                    ctx.TableName,
                    ctx.TargetKeyspaceName,
                    ctx.TargetTableName,
                    _config.CqlCopyPageSize);
                result = await copier.CopyRowsAsync(
                    mu, chunkIndex,
                    initialPercent, contributionFactor,
                    rowCount, _cts.Token,
                    MigrationJobContext
                        .CurrentlyActiveJob.IsSimulatedRun);
            }
            Console.WriteLine(
                $"  CopyRowsAsync result: {result}");

            if (result == TaskResult.Success)
            {
                if (!_cts.Token.IsCancellationRequested
                    && mu.MigrationChunks[chunkIndex].Segments
                        .All(seg => seg.IsProcessed == true))
                {
                    mu.MigrationChunks[chunkIndex].IsDownloaded = true;
                    mu.MigrationChunks[chunkIndex].IsUploaded = true;
                }
                MigrationJobContext.SaveMigrationUnit(mu, false);
                return TaskResult.Success;
            }
            else if (result == TaskResult.Canceled)
            {
                _log.WriteLine(
                    $"Copy paused for {ctx.KeyspaceName}" +
                    $".{ctx.TableName}[{chunkIndex}].");
                return TaskResult.Canceled;
            }
            else
            {
                _log.WriteLine(
                    $"Copy failed for {ctx.KeyspaceName}" +
                    $".{ctx.TableName}[{chunkIndex}].",
                    LogType.Error);
                return TaskResult.Retry;
            }
        }

        /// <summary>
        /// Unified worker pipeline with feed range state objects:
        ///
        ///  workChannel ──► Worker (read 1 page, write rows)
        ///       ▲               │
        ///       └── recycle ◄───┘  (if more pages)
        ///
        /// Each worker pulls a FeedRangeState, reads ONE page
        /// from source, writes the rows to target, then either
        /// recycles the range back (with updated paging state)
        /// or marks it complete. Single worker pool = simpler,
        /// natural backpressure (slow writes slow reads).
        /// Continuation state persisted periodically for resume.
        /// </summary>
        private async Task<TaskResult> CopyWithFeedRangesAsync(
            MigrationUnit mu,
            int chunkIndex,
            double initialPercent,
            double contributionFactor,
            long totalRowCount,
            ProcessorContext ctx,
            List<string> feedRanges)
        {
            bool isSimulated = MigrationJobContext
                .CurrentlyActiveJob.IsSimulatedRun;
            int rawValue = MigrationJobContext
                .CurrentlyActiveJob.MaxFeedRangeParallelism;
            int workerCount = Math.Max(1, rawValue);

            // ── Resume: filter out completed ranges ─────────
            var completed = mu.CompletedCopyFeedRanges
                ?? new HashSet<string>();
            var checkpoints = mu.CopyFeedRangeCheckpoints
                ?? new Dictionary<string, string?>();
            mu.CompletedCopyFeedRanges = completed;
            mu.CopyFeedRangeCheckpoints = checkpoints;

            var pendingRanges = feedRanges
                .Where(r => !completed.Contains(r))
                .ToList();

            if (pendingRanges.Count == 0)
            {
                _log.WriteLine(
                    $"All {feedRanges.Count} ranges already " +
                    $"completed for {ctx.KeyspaceName}" +
                    $".{ctx.TableName}");
                return TaskResult.Success;
            }

            // No cap — with recycle-after-read,
            // workers > ranges is fine (extra workers
            // just wait on the channel for work)

            _log.WriteLine(
                $"Pipeline copy: {pendingRanges.Count} ranges " +
                $"({completed.Count} already done), " +
                $"{workerCount} workers " +
                $"for {ctx.KeyspaceName}.{ctx.TableName}");
            Console.WriteLine(
                $"  Pipeline: {pendingRanges.Count} ranges, " +
                $"workers={workerCount}");

            // ── Schema setup (once) ─────────────────────────
            _log.WriteLine(
                $"Detecting target table " +
                $"{ctx.TargetKeyspaceName}" +
                $".{ctx.TargetTableName}...");
            if (!await CassandraHelper.TableExistsAsync(
                _targetSession!, ctx.TargetKeyspaceName,
                ctx.TargetTableName)
                .ConfigureAwait(false))
            {
                _log.WriteLine(
                    $"Target table not found — creating " +
                    $"{ctx.TargetKeyspaceName}" +
                    $".{ctx.TargetTableName} from source schema");
                await CassandraHelper.EnsureKeyspaceExistsAsync(
                    _targetSession!, ctx.TargetKeyspaceName)
                    .ConfigureAwait(false);
                await CassandraHelper.CreateTableFromSourceAsync(
                    _sourceSession!, _targetSession!,
                    ctx.KeyspaceName, ctx.TableName,
                    ctx.TargetKeyspaceName, ctx.TargetTableName)
                    .ConfigureAwait(false);
                _log.WriteLine(
                    $"Created target table " +
                    $"{ctx.TargetKeyspaceName}" +
                    $".{ctx.TargetTableName}");
            }
            else
            {
                _log.WriteLine(
                    $"Target table exists — syncing schema " +
                    $"for {ctx.TargetKeyspaceName}" +
                    $".{ctx.TargetTableName}");
                await CassandraHelper.CreateTableFromSourceAsync(
                    _sourceSession!, _targetSession!,
                    ctx.KeyspaceName, ctx.TableName,
                    ctx.TargetKeyspaceName, ctx.TargetTableName)
                    .ConfigureAwait(false);
            }

            _log.WriteLine(
                $"Discovering source columns for " +
                $"{ctx.KeyspaceName}.{ctx.TableName}...");
            var columns = await CassandraHelper.GetTableColumnsAsync(
                _sourceSession!, ctx.KeyspaceName, ctx.TableName)
                .ConfigureAwait(false);
            if (columns.Count == 0)
            {
                _log.WriteLine(
                    $"No columns for {ctx.KeyspaceName}" +
                    $".{ctx.TableName}", LogType.Error);
                return TaskResult.Abort;
            }
            _log.WriteLine(
                $"Source schema: {columns.Count} columns " +
                $"[{string.Join(", ", columns.Select(c => c.Name))}]");

            _log.WriteLine(
                $"Preparing INSERT statement for " +
                $"{ctx.TargetKeyspaceName}" +
                $".{ctx.TargetTableName}...");
            var (ps, colNames) = await CassandraHelper.PrepareInsertAsync(
                _targetSession!, ctx.TargetKeyspaceName,
                ctx.TargetTableName, columns)
                .ConfigureAwait(false);
            _log.WriteLine(
                $"INSERT prepared with {colNames.Count} columns");

            // ── Work channel ────────────────────────────────
            // Bounded: workers that finish writing fast will
            // block recycling if all slots are full, providing
            // natural backpressure.
            var workCh = Channel.CreateBounded<FeedRangeState>(
                new BoundedChannelOptions(
                    pendingRanges.Count + workerCount)
                {
                    FullMode = BoundedChannelFullMode.Wait
                });

            // Write throttle: cap total in-flight INSERTs.
            // Use job-level MaxWriteConcurrency if set,
            // otherwise auto-calculate from worker count.
            int jobWriteConcurrency = MigrationJobContext
                .CurrentlyActiveJob.MaxWriteConcurrency;
            int maxInFlight = jobWriteConcurrency > 0
                ? jobWriteConcurrency
                : Math.Min(workerCount * 64, 8000);
            var writeSem = new SemaphoreSlim(maxInFlight);
            _log.WriteLine(
                $"Write: concurrent INSERTs, " +
                $"max {maxInFlight} in-flight" +
                $"{(jobWriteConcurrency > 0 ? " (configured)" : " (auto)")}");

            // Seed channel with pending ranges (resume-aware)
            _log.WriteLine(
                $"Seeding work channel with " +
                $"{pendingRanges.Count} feed ranges...");
            int resumedCount = 0;
            foreach (var range in pendingRanges)
            {
                byte[]? pagingState = null;
                if (checkpoints.TryGetValue(range, out var b64)
                    && b64 != null)
                {
                    pagingState = Convert.FromBase64String(b64);
                    resumedCount++;
                    _log.WriteLine(
                        $"Resuming range from checkpoint: " +
                        $"{TruncRange(range)}");
                }
                await workCh.Writer.WriteAsync(
                    new FeedRangeState(range, pagingState));
            }
            if (resumedCount > 0)
                _log.WriteLine(
                    $"{resumedCount} ranges resumed from " +
                    $"checkpoint, {pendingRanges.Count - resumedCount} " +
                    $"starting fresh");
            else
                _log.WriteLine(
                    $"All {pendingRanges.Count} ranges " +
                    $"starting fresh");

            var tracker = new CopyProgressTracker(
                _log, ctx.KeyspaceName, ctx.TableName,
                workerCount, pendingRanges.Count);

            long totalRead = 0;
            long totalWritten = 0;
            long totalFailed = 0;
            int nonRetriableHitFlag = 0;
            var workerErrors = new ConcurrentBag<TaskResult>();
            var sw = Stopwatch.StartNew();
            long lastCheckpointTicks =
                DateTime.UtcNow.Ticks;

            // Adaptive page size: starts at configured value,
            // adjusts based on observed row size to target
            // ~5MB per page read.
            int jobPageSize = MigrationJobContext
                .CurrentlyActiveJob?.PageSize ?? 0;
            int configuredPageSize = jobPageSize > 0
                ? jobPageSize
                : _config.CqlCopyPageSize > 0
                    ? _config.CqlCopyPageSize
                    : 500;
            int adaptivePageSize = configuredPageSize;
            const long TargetPageBytes = 5 * 1024 * 1024; // 5MB

            string BuildSelectCql(string range) =>
                $"SELECT * FROM " +
                $"\"{ctx.KeyspaceName}\".\"{ctx.TableName}\"" +
                $" WHERE COSMOS_CHANGEFEED_FROM_START() = true" +
                $" AND COSMOS_FEEDRANGE() = '{range}'";

            // Close channel when ALL feed ranges are done
            void TryCloseChannel()
            {
                if (completed.Count >= feedRanges.Count)
                    workCh.Writer.TryComplete();
            }

            // ── UNIFIED WORKERS ─────────────────────────────
            _log.WriteLine(
                $"Launching {workerCount} workers " +
                $"for {ctx.KeyspaceName}.{ctx.TableName} " +
                $"({pendingRanges.Count} feed ranges, " +
                $"page size={configuredPageSize}" +
                $"{(configuredPageSize != adaptivePageSize ? $"→{adaptivePageSize} adaptive" : "")})...");
            var workers = Enumerable.Range(0, workerCount)
                .Select(wid => Task.Run(async () =>
                {
                    tracker.WorkerStarted();
                    try
                    {
                    await foreach (var state in workCh.Reader
                        .ReadAllAsync(_cts.Token))
                    {
                        if (_cts.Token.IsCancellationRequested
                            || Volatile.Read(ref nonRetriableHitFlag) != 0)
                        {
                            // On cancel/abort, mark range done
                            // so we can close the channel
                            lock (checkpoints)
                            {
                                completed.Add(state.FeedRange);
                            }
                            TryCloseChannel();
                            continue;
                        }

                        bool isLastPage = false;
                        bool recycledToChannel = false;
                        byte[]? nextPaging = null;
                        try
                        {
                            // ── READ one page ───────────────
                            var readSw = Stopwatch.StartNew();
                            var stmt = new SimpleStatement(
                                BuildSelectCql(state.FeedRange));
                            // Adaptive page size: use shared
                            // estimate updated after each page
                            int effectivePageSize = Volatile.Read(
                                ref adaptivePageSize);
                            stmt.SetPageSize(effectivePageSize);
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
                                workerErrors.Add(
                                    TaskResult.Retry);
                                isLastPage = true;
                                // fall through to finally
                            }
                            else
                            {
                                nextPaging = rs.PagingState;

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
                                        new object[colNames.Count];
                                    for (int i = 0;
                                        i < colNames.Count; i++)
                                        vals[i] =
                                            row[colNames[i]];
                                    rows.Add(vals);
                                }

                                Interlocked.Add(
                                    ref totalRead, rows.Count);
                                readSw.Stop();
                                tracker.AddReadTime(
                                    readSw.ElapsedMilliseconds);

                                // Adapt page size based on row size
                                if (rows.Count > 0)
                                {
                                    long sampleSize = 0;
                                    var sample = rows[0];
                                    foreach (var v in sample)
                                    {
                                        if (v is byte[] b)
                                            sampleSize += b.Length;
                                        else if (v is string s)
                                            sampleSize += s.Length * 2;
                                        else if (v != null)
                                            sampleSize += 8;
                                    }
                                    if (sampleSize > 0)
                                    {
                                        int ideal = (int)Math.Clamp(
                                            TargetPageBytes / sampleSize,
                                            10, configuredPageSize);
                                        Volatile.Write(
                                            ref adaptivePageSize, ideal);
                                    }
                                }

                                if (rows.Count == 0
                                    || nextPaging == null)
                                    isLastPage = true;

                                // ── RECYCLE immediately after
                                // read so next page can be read
                                // by another worker in parallel
                                // while this one writes ──────
                                bool recycled = false;
                                if (!isLastPage)
                                {
                                    try
                                    {
                                        await workCh.Writer
                                            .WriteAsync(
                                                new FeedRangeState(
                                                    state.FeedRange,
                                                    nextPaging),
                                                _cts.Token);
                                        recycledToChannel = true;
                                    }
                                    catch (OperationCanceledException)
                                    {
                                        // Cancelled during
                                        // recycle — treat as
                                        // last page so range
                                        // doesn't get lost
                                        isLastPage = true;
                                    }
                                }

                                // Update checkpoint AFTER
                                // successful recycle/completion
                                lock (checkpoints)
                                {
                                    if (isLastPage)
                                    {
                                        checkpoints.Remove(
                                            state.FeedRange);
                                        completed.Add(
                                            state.FeedRange);
                                    }
                                    else if (recycledToChannel
                                        && nextPaging != null)
                                    {
                                        checkpoints[
                                            state.FeedRange] =
                                            Convert.ToBase64String(
                                                nextPaging);
                                    }
                                }

                                // ── WRITE rows concurrently ──
                                var writeSw = Stopwatch.StartNew();
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
                                        || Volatile.Read(ref nonRetriableHitFlag) != 0)
                                        break;
                                    long semStart = Stopwatch.GetTimestamp();
                                    await writeSem.WaitAsync(
                                        _cts.Token);
                                    semWaitMs += (Stopwatch.GetTimestamp() - semStart)
                                        * 1000 / Stopwatch.Frequency;
                                    try
                                    {
                                        var bound = ps.Bind(vals);
                                        bound
                                            .SetReadTimeoutMillis(
                                                60_000);
                                        // Use LocalOne for bulk writes
                                        // (faster, replicated async)
                                        bound.SetConsistencyLevel(
                                            ConsistencyLevel.LocalOne);
                                        var wStart = Stopwatch.GetTimestamp();
                                        writeTasks.Add(
                                            _targetSession!
                                            .ExecuteAsync(bound)
                                            .ContinueWith(t =>
                                        {
                                            long wElapsed = (Stopwatch.GetTimestamp() - wStart)
                                                * 1000 / Stopwatch.Frequency;
                                            Interlocked.Add(ref writeLatencySum, wElapsed);
                                            writeSem.Release();
                                            if (t.IsFaulted)
                                            {
                                                var ex =
                                                    t.Exception!
                                                    .InnerException!;
                                                Interlocked
                                                    .Increment(
                                                    ref totalFailed);
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
                                                        ref nonRetriableHitFlag, 1);
                                            }
                                            else
                                            {
                                                Interlocked
                                                    .Increment(
                                                    ref totalWritten);
                                                Interlocked
                                                    .Increment(
                                                    ref writeDone);
                                            }
                                        }, TaskContinuationOptions
                                            .ExecuteSynchronously));
                                    }
                                    catch
                                    {
                                        writeSem.Release();
                                        throw;
                                    }
                                }
                                // Snapshot in-flight before
                                // waiting for completion
                                tracker.SetSemCurrent(
                                    maxInFlight - writeSem.CurrentCount);
                                tracker.SetPipelineState(
                                    feedRanges.Count - completed.Count,
                                    Volatile.Read(ref adaptivePageSize));
                                await Task.WhenAll(writeTasks);
                                writeSw.Stop();
                                tracker.AddWriteTime(
                                    writeLatencySum,
                                    rows.Count);
                                tracker.AddSemWaitTime(semWaitMs);
                                tracker.AddCopied(writeDone);
                                tracker.AddFailed(writeFail);

                                // Estimate data volume
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
                                tracker.AddBytes(pageBytes);

                                // ── LAST PAGE: signal
                                // completion AFTER writes ─────
                                if (isLastPage)
                                {
                                    _log.WriteLine(
                                        $"Range complete: " +
                                        $"{TruncRange(state.FeedRange)} " +
                                        $"[{completed.Count}" +
                                        $"/{feedRanges.Count}]");
                                    tracker.RangeCompleted(
                                        state.FeedRange,
                                        TaskResult.Success);
                                    TryCloseChannel();
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            workerErrors.Add(
                                TaskResult.Canceled);
                            isLastPage = true;
                        }
                        catch (Exception ex)
                        {
                            _log.WriteLine(
                                $"Worker error: " +
                                $"{ex.GetType().Name}: " +
                                $"{ex.Message}",
                                LogType.Error);
                            workerErrors.Add(TaskResult.Retry);
                            isLastPage = true;
                        }
                        finally
                        {
                            // Handle error/cancel cases where
                            // range wasn't recycled or completed
                            // in the normal path above.
                            // If recycledToChannel is true, the
                            // range is back in the channel — do
                            // NOT mark it completed (another
                            // worker owns it).
                            if (!recycledToChannel
                                && !completed.Contains(
                                    state.FeedRange))
                            {
                                lock (checkpoints)
                                {
                                    checkpoints.Remove(
                                        state.FeedRange);
                                    completed.Add(
                                        state.FeedRange);
                                }
                                _log.WriteLine(
                                    $"Range failed: " +
                                    $"{TruncRange(state.FeedRange)} " +
                                    $"[{completed.Count}" +
                                    $"/{feedRanges.Count}]");
                                tracker.RangeCompleted(
                                    state.FeedRange,
                                    TaskResult.Retry);
                                TryCloseChannel();
                            }

                            // Update progress
                            long written = Interlocked.Read(
                                ref totalWritten);
                            long failed = Interlocked.Read(
                                ref totalFailed);
                            var chunk =
                                mu.MigrationChunks[chunkIndex];
                            chunk.SourceResultRowCount = written;
                            chunk.TargetInsertedRowCount =
                                written;
                            chunk.TargetFailedRowCount = failed;
                            mu.CopyRowsCopied = written;
                            mu.CopyRowsPerSecond =
                                tracker.RecentSpeed;
                            if (totalRowCount > 0)
                            {
                                mu.CopyPercent = initialPercent +
                                    (Math.Min(99.9,
                                        (double)written
                                        / totalRowCount * 100)
                                    * contributionFactor);
                            }
                            mu.UpdateParentJob();

                            // Save checkpoint every 10s
                            // (atomic CAS for thread safety)
                            long prevTicks = Interlocked.Read(
                                ref lastCheckpointTicks);
                            var now = DateTime.UtcNow;
                            if ((now.Ticks - prevTicks)
                                / TimeSpan.TicksPerSecond >= 10
                                && Interlocked.CompareExchange(
                                    ref lastCheckpointTicks,
                                    now.Ticks, prevTicks)
                                    == prevTicks)
                            {
                                MigrationJobContext
                                    .SaveMigrationUnit(
                                        mu, true);
                            }
                        }
                    }
                    }
                    finally
                    {
                        tracker.WorkerExited();
                    }
                })).ToArray();

            try
            {
                await Task.WhenAll(workers);
            }
            catch (OperationCanceledException)
            {
                // Workers exited due to cancellation
            }

            // Ensure channel is closed
            workCh.Writer.TryComplete();

            // ── Final stats ─────────────────────────────────
            tracker.LogFinal();
            long finalWritten = Interlocked.Read(ref totalWritten);
            long finalFailed = Interlocked.Read(ref totalFailed);
            long finalRead = Interlocked.Read(ref totalRead);

            var elapsed = sw.Elapsed;
            double avgSpeed = elapsed.TotalSeconds > 0
                ? finalWritten / elapsed.TotalSeconds : 0;
            _log.WriteLine(
                $"Pipeline complete for {ctx.KeyspaceName}" +
                $".{ctx.TableName}:");
            _log.WriteLine(
                $"  Total read:    {finalRead:N0} rows");
            _log.WriteLine(
                $"  Total written: {finalWritten:N0} rows");
            _log.WriteLine(
                $"  Total failed:  {finalFailed:N0} rows");
            _log.WriteLine(
                $"  Ranges:        " +
                $"{completed.Count}/{feedRanges.Count} completed");
            _log.WriteLine(
                $"  Duration:      {elapsed.TotalSeconds:F1}s");
            _log.WriteLine(
                $"  Avg speed:     {avgSpeed:F0} rows/sec");
            _log.WriteLine(
                $"  Workers used:  {workerCount}");

            // Final chunk update
            var fc = mu.MigrationChunks[chunkIndex];
            fc.SourceResultRowCount = finalWritten;
            fc.TargetInsertedRowCount = finalWritten;
            fc.TargetFailedRowCount = finalFailed;
            mu.CopyRowsCopied = finalWritten;
            mu.ActualRowCount = Math.Max(
                mu.ActualRowCount, finalRead);
            if (fc.Segments.Count == 0)
            {
                fc.Segments.Add(new Segment
                {
                    Id = "0",
                    IsProcessed = true,
                    ResultDocCount = finalWritten
                });
            }
            else
            {
                foreach (var seg in fc.Segments)
                    seg.IsProcessed = true;
            }
            MigrationJobContext.SaveMigrationUnit(mu, true);

            if (Volatile.Read(ref nonRetriableHitFlag) != 0)
                return TaskResult.Abort;
            if (workerErrors.Any(r => r == TaskResult.Abort))
                return TaskResult.Abort;
            if (workerErrors.Any(r => r == TaskResult.Canceled))
                return TaskResult.Canceled;
            if (finalFailed > 0)
                return TaskResult.Retry;
            return TaskResult.Success;
        }

        /// <summary>
        /// State of a feed range — its token and paging position.
        /// </summary>
        private record FeedRangeState(
            string FeedRange,
            byte[]? PagingState);

        private static string TruncRange(string r) =>
            r.Length > 30 ? r[..15] + "..." : r;

        private static bool IsRetriableWriteError(Exception ex)
        {
            var msg = ex.Message ?? string.Empty;
            if (msg.Contains("429")
                || msg.Contains("TooManyRequests",
                    StringComparison.OrdinalIgnoreCase)
                || msg.Contains("rate",
                    StringComparison.OrdinalIgnoreCase))
                return true;
            if (ex is TimeoutException
                || ex is System.IO.IOException
                || msg.Contains("timeout",
                    StringComparison.OrdinalIgnoreCase))
                return true;
            if (ex is System.Net.Sockets.SocketException
                || msg.Contains("connection",
                    StringComparison.OrdinalIgnoreCase))
                return true;
            // Non-retriable: auth, schema, syntax errors
            return false;
        }

        public override async Task<TaskResult> StartProcessAsync(
            string migrationUnitId)
        {
            Console.WriteLine(
                $"CopyProcessor.StartProcessAsync: mu={migrationUnitId}");

            var mu = MigrationJobContext
                .GetMigrationUnit(migrationUnitId);
            mu.ParentJob = MigrationJobContext.CurrentlyActiveJob;
            ProcessRunning = true;

            var ctx = SetProcessorContext(mu);

            Console.WriteLine(
                $"  CopyComplete={mu.CopyComplete}, " +
                $"Chunks={mu.MigrationChunks?.Count ?? 0}, " +
                $"ks={ctx.KeyspaceName}, tbl={ctx.TableName}");

            if (mu.CopyComplete)
            {
                Console.WriteLine(
                    $"  SKIPPING - already complete");
                _log.WriteLine(
                    $"Copy for {ctx.KeyspaceName}.{ctx.TableName} " +
                    $"already completed.", LogType.Debug);
                return TaskResult.Success;
            }

            Console.WriteLine(
                $"  Copy starting for {ctx.KeyspaceName}.{ctx.TableName}");
            _log.WriteLine(
                $"{ctx.KeyspaceName}.{ctx.TableName} Copy started");

            if (!mu.CopyComplete
                && !_cts.Token.IsCancellationRequested)
            {
                // Ensure at least one chunk exists
                if (mu.MigrationChunks == null
                    || mu.MigrationChunks.Count == 0)
                {
                    mu.MigrationChunks =
                        new System.Collections.Generic.List<MigrationChunk>
                        {
                            new MigrationChunk()
                        };
                }

                for (int i = 0; i < mu.MigrationChunks.Count; i++)
                {
                    Console.WriteLine(
                        $"  Processing chunk {i}/{mu.MigrationChunks.Count}, " +
                        $"IsDownloaded={mu.MigrationChunks[i].IsDownloaded}");

                    if (MigrationJobContext.ControlledPauseRequested)
                    {
                        _log.WriteLine(
                            $"Controlled pause before chunk {i}");
                        break;
                    }

                    _cts.Token.ThrowIfCancellationRequested();

                    double initialPercent =
                        ((double)100 / mu.MigrationChunks.Count) * i;
                    double contributionFactor =
                        1.0 / mu.MigrationChunks.Count;

                    if (mu.MigrationChunks[i].IsDownloaded != true)
                    {
                        TaskResult result =
                            await new RetryHelper().ExecuteTask(
                                () => ProcessChunkAsync(
                                    mu, i, ctx,
                                    initialPercent,
                                    contributionFactor),
                                (ex, attemptCount, currentBackoff) =>
                                    CopyProcess_ExceptionHandler(
                                        ex, attemptCount,
                                        "Chunk processor",
                                        ctx.KeyspaceName,
                                        ctx.TableName,
                                        i, currentBackoff),
                                _log,
                                ct: _cts.Token);

                        if (result == TaskResult.Canceled)
                        {
                            _log.WriteLine(
                                $"Copy paused for " +
                                $"{ctx.KeyspaceName}.{ctx.TableName}" +
                                $"[{i}].");
                            StopProcessing(isPause: true);
                            return TaskResult.Canceled;
                        }

                        if (result == TaskResult.Abort
                            || result == TaskResult.FailedAfterRetries)
                        {
                            _log.WriteLine(
                                $"Copy failed for " +
                                $"{ctx.KeyspaceName}.{ctx.TableName}" +
                                $"[{i}] after retries.",
                                LogType.Error);
                            StopProcessing();
                            return result;
                        }
                    }
                    else
                    {
                        ctx.DownloadCount +=
                            mu.MigrationChunks[i].SourceQueryRowCount;
                    }
                }

                if (MigrationJobContext.ControlledPauseRequested)
                {
                    _log.WriteLine(
                        "Controlled pause - exiting",
                        LogType.Debug);
                    StopProcessing(isPause: true);
                    return TaskResult.Success;
                }

                mu.SourceCountDuringCopy = mu.MigrationChunks
                    .Sum(c => c.SourceQueryRowCount);

                long failed = mu.MigrationChunks
                    .Sum(c => c.TargetFailedRowCount);

                if (failed <= 0
                    && mu.MigrationChunks
                        .All(c => c.IsDownloaded == true))
                {
                    mu.BulkCopyEndedOn = DateTime.UtcNow;
                    mu.CopyPercent = 100;
                    mu.CopyComplete = true;
                    mu.UpdateParentJob();

                    AddTableToChangeFeedQueue(mu);
                    MigrationJobContext.SaveMigrationUnit(mu, true);

                    // Only remove from cache if offline — online mode
                    // needs the MU in cache for ChangeFeedProcessor
                    if (!Helper.IsOnline(
                        MigrationJobContext.CurrentlyActiveJob))
                    {
                        MigrationJobContext.MigrationUnitsCache
                            .RemoveMigrationUnit(mu.Id);
                    }
                }
                else
                {
                    _log.WriteLine(
                        $"Copy for {ctx.KeyspaceName}" +
                        $".{ctx.TableName} had failures.",
                        LogType.Error);
                    return TaskResult.Retry;
                }
            }

            StopOfflineOrInvokeChangeFeed();
            return TaskResult.Success;
        }
    }
}
