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
        /// Unified worker pipeline with partition pool:
        ///
        ///  Partition Pool (Channel) ──► Worker (read + write)
        ///         ▲                         │
        ///         └──── recycle ◄────────────┘ (if more pages)
        ///
        /// Each worker takes a partition, reads one page,
        /// creates a WorkChunk, recycles the partition back
        /// to the pool (so another worker can read the next
        /// page), then writes rows and marks the chunk done.
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

            _log.WriteLine(
                $"Pipeline copy: {pendingRanges.Count} ranges " +
                $"({completed.Count} already done), " +
                $"{workerCount} workers " +
                $"for {ctx.KeyspaceName}.{ctx.TableName}");

            // ── Schema setup (once) ─────────────────────────
            _log.WriteLine(
                $"Detecting target table " +
                $"{ctx.TargetKeyspaceName}" +
                $".{ctx.TargetTableName}...",
                LogType.Debug);
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
                    $".{ctx.TargetTableName}",
                    LogType.Debug);
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
                $"[{string.Join(", ", columns.Select(c => c.Name))}]",
                LogType.Debug);

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

            // ── Partition pool channel ───────────────────────
            var partitionPool = Channel.CreateBounded<Partition>(
                new BoundedChannelOptions(
                    pendingRanges.Count + workerCount)
                {
                    FullMode = BoundedChannelFullMode.Wait
                });

            // Write throttle: cap total in-flight INSERTs.
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
                $"Seeding partition pool with " +
                $"{pendingRanges.Count} feed ranges...",
                LogType.Debug);
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
                        $"{TruncRange(range)}",
                        LogType.Debug);
                }
                await partitionPool.Writer.WriteAsync(
                    new Partition(range, pagingState));
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

            // Page size
            int jobPageSize = MigrationJobContext
                .CurrentlyActiveJob?.PageSize ?? 0;
            int configuredPageSize = jobPageSize > 0
                ? jobPageSize
                : _config.CqlCopyPageSize > 0
                    ? _config.CqlCopyPageSize
                    : 500;

            var tracker = new CopyProgressTracker(
                _log, ctx.KeyspaceName, ctx.TableName,
                workerCount, pendingRanges.Count,
                priorCopied);

            var sw = Stopwatch.StartNew();

            // ── Build shared context ────────────────────────
            var pctx = new PipelineContext
            {
                PartitionPool = partitionPool,
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

            // ── LAUNCH UNIFIED WORKERS ──────────────────────
            _log.WriteLine(
                $"Launching {workerCount} workers " +
                $"for {ctx.KeyspaceName}.{ctx.TableName} " +
                $"({pendingRanges.Count} feed ranges, " +
                $"page size={configuredPageSize})...");
            var workers = Enumerable.Range(0, workerCount)
                .Select(wid => Task.Run(
                    () => RunWorkerAsync(wid, pctx)))
                .ToArray();

            // Wait for all workers
            try
            {
                await Task.WhenAll(workers);
            }
            catch (OperationCanceledException)
            {
                // Workers exited due to cancellation
            }

            // Ensure channel is closed
            pctx.PartitionPool.Writer.TryComplete();

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
                $"  Workers used:  {workerCount}");

            // Final chunk update
            var fc = mu.MigrationChunks[chunkIndex];
            fc.SourceResultRowCount = finalWritten;
            fc.TargetInsertedRowCount = finalWritten;
            fc.TargetFailedRowCount = finalFailed;
            mu.CopyRowsCopied = finalWritten;
            mu.ActualRowCount = Math.Max(
                mu.ActualRowCount, finalRead);
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
