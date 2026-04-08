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
        /// Split reader/writer pipeline with feed range state objects:
        ///
        ///  workCh ──► Reader (read 1 page) ──► dataCh ──► Writer (write rows)
        ///    ▲             │
        ///    └── recycle ◄─┘ (if more pages)
        ///
        /// Readers pull FeedRangeState, read ONE page from source,
        /// extract rows, recycle the range, and push ReadPage to
        /// dataCh. Writers consume ReadPage objects and fire
        /// concurrent INSERTs. Separating read from write lets
        /// readers prefetch the next page while writes are in
        /// flight. Continuation state persisted periodically
        /// for resume.
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
            int readerCount = workerCount;
            int writerCount = Math.Max(4, workerCount / 2);

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
                $"{readerCount} readers + {writerCount} writers " +
                $"for {ctx.KeyspaceName}.{ctx.TableName}");
            Console.WriteLine(
                $"  Pipeline: {pendingRanges.Count} ranges, " +
                $"readers={readerCount}, writers={writerCount}");

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

            // Seed counters from prior run for resume
            long priorCopied = mu.CopyRowsCopied;

            var tracker = new CopyProgressTracker(
                _log, ctx.KeyspaceName, ctx.TableName,
                readerCount, pendingRanges.Count,
                priorCopied);

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

            // ── DATA CHANNEL ────────────────────────────────
            var dataCh = Channel.CreateBounded<ReadPage>(
                new BoundedChannelOptions(workerCount * 2)
                {
                    FullMode = BoundedChannelFullMode.Wait
                });

            var sw = Stopwatch.StartNew();

            // ── Build shared context ────────────────────────
            var pctx = new PipelineContext
            {
                WorkCh = workCh,
                DataCh = dataCh,
                WriteSem = writeSem,
                Ps = ps,
                ColNames = colNames,
                Completed = completed,
                Checkpoints = checkpoints,
                FeedRanges = feedRanges,
                Tracker = tracker,
                TotalRead = 0,
                TotalWritten = priorCopied,
                TotalFailed = 0,
                NonRetriableHitFlag = 0,
                WorkerErrors = new ConcurrentBag<TaskResult>(),
                ConfiguredPageSize = configuredPageSize,
                MaxInFlight = maxInFlight,
                Ctx = ctx,
                Mu = mu,
                ChunkIndex = chunkIndex,
                InitialPercent = initialPercent,
                ContributionFactor = contributionFactor,
                TotalRowCount = totalRowCount,
                LastCheckpointTicks = DateTime.UtcNow.Ticks,
            };

            // ── READER + WRITER POOLS ───────────────────────
            _log.WriteLine(
                $"Launching {readerCount} readers + " +
                $"{writerCount} writers " +
                $"for {ctx.KeyspaceName}.{ctx.TableName} " +
                $"({pendingRanges.Count} feed ranges, " +
                $"page size={configuredPageSize})...");
            var readers = Enumerable.Range(0, readerCount)
                .Select(rid => Task.Run(
                    () => RunReaderAsync(rid, pctx)))
                .ToArray();

            // ── WRITER WORKERS ──────────────────────────────
            var writers = Enumerable.Range(0, writerCount)
                .Select(wid => Task.Run(
                    () => RunWriterAsync(wid, pctx)))
                .ToArray();

            // Wait for readers, then close data channel
            try
            {
                await Task.WhenAll(readers);
            }
            catch (OperationCanceledException)
            {
                // Readers exited due to cancellation
            }
            pctx.DataCh.Writer.TryComplete();

            // Wait for writers to drain remaining pages
            try
            {
                await Task.WhenAll(writers);
            }
            catch (OperationCanceledException)
            {
                // Writers exited due to cancellation
            }

            // Ensure channels are closed
            pctx.WorkCh.Writer.TryComplete();

            // ── Final stats ─────────────────────────────────
            pctx.Tracker.LogFinal();
            long finalWritten = Interlocked.Read(
                ref pctx.TotalWritten);
            long finalFailed = Interlocked.Read(
                ref pctx.TotalFailed);
            long finalRead = Interlocked.Read(
                ref pctx.TotalRead);

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
                $"{pctx.Completed.Count}/{feedRanges.Count} completed");
            _log.WriteLine(
                $"  Duration:      {elapsed.TotalSeconds:F1}s");
            _log.WriteLine(
                $"  Avg speed:     {avgSpeed:F0} rows/sec");
            _log.WriteLine(
                $"  Workers used:  {readerCount} readers + {writerCount} writers");

            // Final chunk update
            var fc = mu.MigrationChunks[chunkIndex];
            fc.SourceResultRowCount = finalWritten;
            fc.TargetInsertedRowCount = finalWritten;
            fc.TargetFailedRowCount = finalFailed;
            mu.CopyRowsCopied = finalWritten;
            mu.ActualRowCount = Math.Max(
                mu.ActualRowCount, finalRead);
            // Only mark segments as processed if ALL feed
            // ranges actually completed (not cancelled/paused)
            bool allRangesComplete =
                pctx.Completed.Count >= feedRanges.Count;
            if (fc.Segments.Count == 0)
            {
                fc.Segments.Add(new Segment
                {
                    Id = "0",
                    IsProcessed = allRangesComplete,
                    ResultDocCount = finalWritten
                });
            }
            else if (allRangesComplete)
            {
                foreach (var seg in fc.Segments)
                    seg.IsProcessed = true;
            }
            MigrationJobContext.SaveMigrationUnit(mu, true);

            if (Volatile.Read(
                ref pctx.NonRetriableHitFlag) != 0)
                return TaskResult.Abort;
            if (pctx.WorkerErrors.Any(
                r => r == TaskResult.Abort))
                return TaskResult.Abort;
            if (pctx.WorkerErrors.Any(
                r => r == TaskResult.Canceled))
                return TaskResult.Canceled;
            if (finalFailed > 0)
                return TaskResult.Retry;
            return TaskResult.Success;
        }
    }
}
