using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers;
using CassandraMigrationProcessor.Helpers.Cassandra;
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
        ///  Stage 1: SeedPartitions (detect ranges, restore checkpoints)
        ///  Stage 2: Schema sync
        ///  Stage 3: Worker execution
        ///
        ///  Partition Pool (Channel) ──► Worker (read + write)
        ///         ▲                         │
        ///         └──── recycle ◄────────────┘ (if more pages)
        /// </summary>
        private record PartitionStageResult(
            Channel<Partition> Pool,
            HashSet<string> Completed,
            Dictionary<string, string?> Checkpoints,
            int PendingCount);

        private async Task<PartitionStageResult?> SeedPartitionsAsync(
            MigrationUnit migrationUnit, List<string> feedRanges,
            int workerCount, string keyspace, string table)
        {
            var completed = migrationUnit.CompletedCopyFeedRanges
                ?? new HashSet<string>();
            var checkpoints = migrationUnit.CopyFeedRangeCheckpoints
                ?? new Dictionary<string, string?>();
            migrationUnit.CompletedCopyFeedRanges = completed;
            migrationUnit.CopyFeedRangeCheckpoints = checkpoints;

            List<string> pendingRanges;
            lock (checkpoints)
            {
                pendingRanges = feedRanges
                    .Where(r => !completed.Contains(r))
                    .ToList();
            }

            if (pendingRanges.Count == 0)
            {
                _log.WriteLine($"All {feedRanges.Count} ranges already completed for {keyspace}.{table}", LogType.Info);
                return null;
            }

            _log.WriteLine($"Pipeline copy: {pendingRanges.Count} ranges ({completed.Count} already done), {workerCount} workers for {keyspace}.{table}", LogType.Info);

            var pool = Channel.CreateBounded<Partition>(new BoundedChannelOptions(
                    pendingRanges.Count + workerCount)
                { FullMode = BoundedChannelFullMode.Wait });

            int resumedCount = 0;
            foreach (var range in pendingRanges)
            {
                byte[]? pagingState = null;
                if (checkpoints.TryGetValue(range, out var base64Token)
                    && base64Token != null)
                {
                    pagingState = Convert.FromBase64String(base64Token);
                    resumedCount++;
                }
                await pool.Writer.WriteAsync(new Partition(range, pagingState));
            }
            if (resumedCount > 0)
                _log.WriteLine($"Resuming {resumedCount}/{pendingRanges.Count} ranges from checkpoint", LogType.Info);

            return new PartitionStageResult(pool, completed, checkpoints, pendingRanges.Count);
        }

        private async Task<TaskResult> CopyWithFeedRangesAsync(PipelineRequest request)
        {
            var migrationUnit = request.MigrationUnit;
            var chunkIndex = request.ChunkIndex;
            var initialPercent = request.InitialPercent;
            var contributionFactor = request.ContributionFactor;
            var totalRowCount = request.TotalRowCount;
            var processorContext = request.Context;
            var feedRanges = request.FeedRanges;

            int totalBudget = Environment.ProcessorCount * MigrationDefaults.WorkerMultiplier;
            int parallelTables = Math.Max(1, _job.ParallelThreads);
            int autoWorkers = Math.Max(MigrationDefaults.MinWorkers, totalBudget / parallelTables);
            int workerCount = _job.MaxFeedRangeParallelism > 0
                ? _job.MaxFeedRangeParallelism
                : autoWorkers;

            // ── Stage 1: Partition seeding ──
            var partitions = await SeedPartitionsAsync(
                migrationUnit, feedRanges, workerCount,
                processorContext.KeyspaceName, processorContext.TableName);
            if (partitions == null)
                return TaskResult.Success;

            // ── Stage 2: Schema sync (uses metadata sessions, not per-worker sessions) ──
            var targetSession = EnsureTargetSession();
            var columns = await SchemaManager.SyncSchemaAsync(
                processorContext.SourceSession, targetSession,
                processorContext.KeyspaceName, processorContext.TableName,
                processorContext.TargetKeyspaceName, processorContext.TargetTableName);
            if (columns.Count == 0)
            {
                _log.WriteLine($"No columns for {processorContext.KeyspaceName}.{processorContext.TableName}", LogType.Error);
                return TaskResult.Abort;
            }

            long priorCopied = migrationUnit.CopyRowsCopied;

            int jobPageSize = _job?.PageSize ?? 0;
            int configuredPageSize = jobPageSize > 0
                ? jobPageSize
                : _config.CqlCopyPageSize > 0
                    ? _config.CqlCopyPageSize
                    : MigrationDefaults.DefaultPageSize;

            var tracker = new CopyProgressTracker(_log, processorContext.KeyspaceName, processorContext.TableName,
                workerCount, partitions.PendingCount,
                priorCopied,
                migrationUnit, chunkIndex,
                initialPercent, contributionFactor, totalRowCount);

            var stopwatch = Stopwatch.StartNew();

            // ── Stage 3: Worker execution ──
            var ctx = new PipelineContext(
                partitions.Pool,
                new WorkerConfig(_job.SourceConnection, _job.TargetConnection, columns, processorContext),
                new RangeState(partitions.Completed, partitions.Checkpoints, feedRanges),
                new PipelineCounters(),
                tracker);

            _log.WriteLine($"Launching {workerCount} workers for {processorContext.KeyspaceName}.{processorContext.TableName} ({partitions.PendingCount} feed ranges, page size={configuredPageSize})...", LogType.Info);
            using var pool = new WorkerPool(_log, workerCount, _cancellation);
            pool.Start(workerId => RunWorkerAsync(workerId, ctx, configuredPageSize));
            await pool.WaitForCompletionAsync();
            ctx.PartitionPool.Writer.TryComplete();

            ctx.Tracker.LogFinal();
            long finalWritten = tracker.TotalCopied;
            long finalFailed = tracker.TotalFailed;
            long finalRead = tracker.TotalRead;
            long sessionWritten = finalWritten - priorCopied;

            var elapsed = stopwatch.Elapsed;
            double avgSpeed = elapsed.TotalSeconds > 0
                ? sessionWritten / elapsed.TotalSeconds : 0;
            int completedCount;
            lock (ctx.Ranges.Checkpoints) { completedCount = ctx.Ranges.Completed.Count; }
            _log.WriteLine($"Pipeline complete for {processorContext.KeyspaceName}.{processorContext.TableName}: " +
                $"session={sessionWritten:N0} written, {finalFailed:N0} failed | " +
                $"cumulative={finalWritten:N0} | {completedCount}/{feedRanges.Count} ranges | " +
                $"{elapsed.TotalSeconds:F1}s ({avgSpeed:F0} rows/sec)", LogType.Info);

            var chunk = migrationUnit.MigrationChunks[chunkIndex];
            chunk.SourceResultRowCount = finalWritten;
            chunk.TargetInsertedRowCount = finalWritten;
            chunk.TargetFailedRowCount = finalFailed;
            migrationUnit.CopyRowsCopied = finalWritten;
            migrationUnit.ActualRowCount = Math.Max(migrationUnit.ActualRowCount, finalRead);
            bool allRangesComplete;
            lock (ctx.Ranges.Checkpoints)
            {
                allRangesComplete = ctx.Ranges.Completed.Count >= feedRanges.Count;
            }
            if (chunk.Segments.Count == 0)
            {
                chunk.Segments.Add(new Segment
                {
                    Id = "0",
                    IsProcessed = allRangesComplete,
                    ResultDocCount = finalWritten
                });
            }
            else if (allRangesComplete)
            {
                foreach (var seg in chunk.Segments)
                    seg.IsProcessed = true;
            }
            MigrationJobContext.SaveMigrationUnit(migrationUnit, true);

            if (Volatile.Read(ref ctx.Counters.FatalErrorFlag) != 0)
                return TaskResult.Abort;
            if (ctx.Counters.WorkerErrors.Any(r => r == TaskResult.Abort))
                return TaskResult.Abort;
            if (ctx.Counters.WorkerErrors.Any(r => r == TaskResult.Canceled))
                return TaskResult.Canceled;
            if (finalFailed > 0)
                return TaskResult.Retry;
            return TaskResult.Success;
        }
    }
}
