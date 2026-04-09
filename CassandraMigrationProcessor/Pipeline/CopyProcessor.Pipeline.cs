using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Pipeline;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Pipeline
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
            string keyspace, string table)
        {
            var completed = migrationUnit.CompletedCopyFeedRanges;
            var checkpoints = migrationUnit.CopyFeedRangeCheckpoints;

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

            _log.WriteLine($"Pipeline copy: {pendingRanges.Count} ranges ({completed.Count} already done) for {keyspace}.{table}", LogType.Info);

            var pool = Channel.CreateBounded<Partition>(new BoundedChannelOptions(pendingRanges.Count)
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

        private int ResolveWorkerCount()
        {
            if (_job.MaxFeedRangeParallelism > 0)
                return _job.MaxFeedRangeParallelism;
            int totalBudget = Environment.ProcessorCount * MigrationDefaults.WorkerMultiplier;
            int parallelTables = Math.Max(1, _job.ParallelThreads);
            return Math.Max(MigrationDefaults.MinWorkers, totalBudget / parallelTables);
        }

        private int ResolvePageSize()
        {
            int jobPageSize = _job?.PageSize ?? 0;
            if (jobPageSize > 0) return jobPageSize;
            if (_config.CqlCopyPageSize > 0) return _config.CqlCopyPageSize;
            return MigrationDefaults.DefaultPageSize;
        }

        private async Task<TaskResult> CopyWithFeedRangesAsync(PipelineRequest request)
        {
            var mu = request.MigrationUnit;
            var ctx0 = request.Context;
            int workerCount = ResolveWorkerCount();
            int pageSize = ResolvePageSize();

            // ── Stage 1: Partition seeding ──
            var partitions = await SeedPartitionsAsync(
                mu, request.FeedRanges, ctx0.KeyspaceName, ctx0.TableName);
            if (partitions == null)
                return TaskResult.Success;

            // ── Stage 2: Schema sync ──
            var columns = await SchemaManager.SyncSchemaAsync(
                ctx0.SourceSession, EnsureTargetSession(),
                ctx0.KeyspaceName, ctx0.TableName,
                ctx0.TargetKeyspaceName, ctx0.TargetTableName);
            if (columns.Count == 0)
            {
                _log.WriteLine($"No columns for {ctx0.KeyspaceName}.{ctx0.TableName}", LogType.Error);
                return TaskResult.Abort;
            }

            long priorCopied = mu.CopyRowsCopied;
            var tracker = new CopyProgressTracker(_log, ctx0.KeyspaceName, ctx0.TableName,
                workerCount, partitions.PendingCount, priorCopied,
                mu, request.ChunkIndex,
                request.InitialPercent, request.ContributionFactor, request.TotalRowCount);

            var stopwatch = Stopwatch.StartNew();

            // ── Stage 3: Worker execution ──
            var ctx = new PipelineContext(
                partitions.Pool,
                new WorkerConfig(_job.SourceConnection, _job.TargetConnection, columns, ctx0),
                new RangeState(partitions.Completed, partitions.Checkpoints, request.FeedRanges),
                new PipelineCounters(),
                tracker);

            _log.WriteLine($"Launching {workerCount} workers for {ctx0.KeyspaceName}.{ctx0.TableName} ({partitions.PendingCount} feed ranges, page size={pageSize})...", LogType.Info);
            using var pool = new WorkerPool(_log, workerCount, _cancellation);
            pool.Start(workerId => RunWorkerAsync(workerId, ctx, pageSize));
            await pool.WaitForCompletionAsync();
            ctx.PartitionPool.Writer.TryComplete();

            // ── Finalization ──
            return FinalizeResults(ctx, mu, request, priorCopied, stopwatch.Elapsed);
        }

        private TaskResult FinalizeResults(PipelineContext ctx, MigrationUnit mu,
            PipelineRequest request, long priorCopied, TimeSpan elapsed)
        {
            var tracker = ctx.Tracker;
            tracker.LogFinal();

            long written = tracker.TotalCopied;
            long failed = tracker.TotalFailed;
            long read = tracker.TotalRead;
            long sessionWritten = written - priorCopied;
            double speed = elapsed.TotalSeconds > 0 ? sessionWritten / elapsed.TotalSeconds : 0;

            int completedCount;
            lock (ctx.Ranges.Checkpoints) { completedCount = ctx.Ranges.Completed.Count; }
            _log.WriteLine($"Pipeline complete for {request.Context.KeyspaceName}.{request.Context.TableName}: " +
                $"session={sessionWritten:N0} written, {failed:N0} failed | " +
                $"cumulative={written:N0} | {completedCount}/{request.FeedRanges.Count} ranges | " +
                $"{elapsed.TotalSeconds:F1}s ({speed:F0} rows/sec)", LogType.Info);

            var chunk = mu.MigrationChunks[request.ChunkIndex];
            chunk.SourceResultRowCount = written;
            chunk.TargetInsertedRowCount = written;
            chunk.TargetFailedRowCount = failed;
            mu.CopyRowsCopied = written;
            mu.ActualRowCount = Math.Max(mu.ActualRowCount, read);

            bool allComplete;
            lock (ctx.Ranges.Checkpoints)
            {
                allComplete = ctx.Ranges.Completed.Count >= request.FeedRanges.Count;
            }
            if (chunk.Segments.Count == 0)
            {
                chunk.Segments.Add(new Segment
                {
                    Id = "0",
                    IsProcessed = allComplete,
                    ResultDocCount = written
                });
            }
            else if (allComplete)
            {
                foreach (var seg in chunk.Segments)
                    seg.IsProcessed = true;
            }
            MigrationJobContext.SaveMigrationUnit(mu, true);

            if (Volatile.Read(ref ctx.Counters.FatalErrorFlag) != 0)
                return TaskResult.Abort;
            if (ctx.Counters.WorkerErrors.Any(r => r == TaskResult.Abort))
                return TaskResult.Abort;
            if (ctx.Counters.WorkerErrors.Any(r => r == TaskResult.Canceled))
                return TaskResult.Canceled;
            return failed > 0 ? TaskResult.Retry : TaskResult.Success;
        }
    }
}
