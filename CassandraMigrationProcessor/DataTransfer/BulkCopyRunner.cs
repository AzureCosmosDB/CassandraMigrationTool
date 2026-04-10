using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Executes the bulk copy pipeline for a single table
    /// using a 4-stage pipeline pattern:
    ///
    ///   Seed → Schema → Execute → Finalize
    ///
    /// Each stage produces a typed result consumed by the next.
    /// </summary>
    internal class BulkCopyRunner
    {
        private readonly MigrationLog _log;
        private readonly MigrationJob _job;
        private readonly PipelineConfig _pipelineConfig;
        private readonly CancellationToken _ct;
        private readonly ISession _targetSession;

        public BulkCopyRunner(MigrationLog log, MigrationJob job, PipelineConfig pipelineConfig,
            CancellationToken cancellationToken, ISession targetSession)
        {
            _log = log;
            _job = job;
            _pipelineConfig = pipelineConfig;
            _ct = cancellationToken;
            _targetSession = targetSession;
        }

        // ── Stage results ──

        private record SeedResult(
            Channel<Partition> Pool,
            HashSet<string> Completed,
            Dictionary<string, string?> Checkpoints,
            int PendingCount);

        private record SchemaResult(
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> Columns);

        private record ExecutionResult(
            CopyProgressTracker Tracker,
            PipelineContext Context,
            TimeSpan Elapsed);

        // ── Pipeline orchestrator ──

        public async Task<TaskResult> RunAsync(PipelineRequest request)
        {
            var ctx0 = request.Context;

            var seed = await SeedAsync(request);
            if (seed == null) return TaskResult.Success;

            var schema = await SyncSchemaAsync(ctx0);
            if (schema == null) return TaskResult.Abort;

            var execution = await ExecuteAsync(request, seed, schema);

            return Finalize(execution, request);
        }

        // ── Stage 1: Seed partitions ──

        private async Task<SeedResult?> SeedAsync(PipelineRequest request)
        {
            var mu = request.MigrationUnit;
            var ctx0 = request.Context;
            var completed = mu.CompletedCopyFeedRanges;
            var checkpoints = mu.CopyFeedRangeCheckpoints;

            List<string> pendingRanges;
            lock (checkpoints)
            {
                pendingRanges = request.FeedRanges.Where(r => !completed.Contains(r)).ToList();
            }

            if (pendingRanges.Count == 0)
            {
                _log.WriteLine($"All {request.FeedRanges.Count} ranges already completed for {ctx0.KeyspaceName}.{ctx0.TableName}", LogType.Info);
                return null;
            }

            _log.WriteLine($"Pipeline copy: {pendingRanges.Count} ranges ({completed.Count} already done) for {ctx0.KeyspaceName}.{ctx0.TableName}", LogType.Info);

            var pool = Channel.CreateBounded<Partition>(new BoundedChannelOptions(pendingRanges.Count)
                { FullMode = BoundedChannelFullMode.Wait });

            int resumedCount = 0;
            foreach (var range in pendingRanges)
            {
                byte[]? pagingState = null;
                if (checkpoints.TryGetValue(range, out var base64Token) && base64Token != null)
                {
                    pagingState = Convert.FromBase64String(base64Token);
                    resumedCount++;
                }
                await pool.Writer.WriteAsync(new Partition(range, pagingState));
            }
            if (resumedCount > 0)
                _log.WriteLine($"Resuming {resumedCount}/{pendingRanges.Count} ranges from checkpoint", LogType.Info);

            return new SeedResult(pool, completed, checkpoints, pendingRanges.Count);
        }

        // ── Stage 2: Schema sync ──

        private async Task<SchemaResult?> SyncSchemaAsync(TableContext ctx0)
        {
            var columns = await SchemaManager.SyncSchemaAsync(
                ctx0.SourceSession, _targetSession,
                ctx0.KeyspaceName, ctx0.TableName,
                ctx0.TargetKeyspaceName, ctx0.TargetTableName);

            if (columns.Count == 0)
            {
                _log.WriteLine($"No columns for {ctx0.KeyspaceName}.{ctx0.TableName}", LogType.Error);
                return null;
            }

            return new SchemaResult(columns);
        }

        // ── Stage 3: Execute workers ──

        private async Task<ExecutionResult> ExecuteAsync(PipelineRequest request, SeedResult seed, SchemaResult schema)
        {
            var ctx0 = request.Context;
            int workerCount = _pipelineConfig.WorkerCount;
            int pageSize = _pipelineConfig.PageSize;
            long priorCopied = request.MigrationUnit.CopyRowsCopied;

            var tracker = new CopyProgressTracker(_log, ctx0.KeyspaceName, ctx0.TableName,
                workerCount, seed.PendingCount, priorCopied,
                request.MigrationUnit, request.ChunkIndex,
                request.InitialPercent, request.ContributionFactor, request.TotalRowCount);

            var stopwatch = Stopwatch.StartNew();

            var ctx = new PipelineContext(
                seed.Pool,
                new WorkerConfig(_job.SourceConnection, _job.TargetConnection, schema.Columns, ctx0),
                new RangeState(seed.Completed, seed.Checkpoints, request.FeedRanges),
                new PipelineCounters(),
                tracker);

            _log.WriteLine($"Launching {workerCount} workers for {ctx0.KeyspaceName}.{ctx0.TableName} ({seed.PendingCount} feed ranges, page size={pageSize})...", LogType.Info);
            using var pool = new WorkerPool(_log, workerCount, _ct);
            pool.Start(workerId => new BulkCopyWorker(_log, _ct, workerId, pageSize).RunAsync(ctx));
            await pool.WaitForCompletionAsync();
            ctx.PartitionPool.Writer.TryComplete();

            return new ExecutionResult(tracker, ctx, stopwatch.Elapsed);
        }

        // ── Stage 4: Finalize ──

        private TaskResult Finalize(ExecutionResult execution, PipelineRequest request)
        {
            execution.Tracker.LogFinal();
            UpdateMigrationStats(execution, request);
            LogPipelineSummary(execution, request);
            return DetermineOutcome(execution.Context, execution.Tracker.TotalFailed);
        }

        private void UpdateMigrationStats(ExecutionResult execution, PipelineRequest request)
        {
            var tracker = execution.Tracker;
            var mu = request.MigrationUnit;
            var chunk = mu.MigrationChunks[request.ChunkIndex];

            chunk.SourceResultRowCount = tracker.TotalCopied;
            chunk.TargetInsertedRowCount = tracker.TotalCopied;
            chunk.TargetFailedRowCount = tracker.TotalFailed;
            mu.CopyRowsCopied = tracker.TotalCopied;
            mu.ActualRowCount = Math.Max(mu.ActualRowCount, tracker.TotalRead);

            bool allComplete;
            lock (execution.Context.Ranges.Checkpoints)
            {
                allComplete = execution.Context.Ranges.Completed.Count >= request.FeedRanges.Count;
            }

            if (chunk.Segments.Count == 0)
                chunk.Segments.Add(new Segment { Id = "0", IsProcessed = allComplete, ResultDocCount = tracker.TotalCopied });
            else if (allComplete)
                chunk.Segments.ForEach(s => s.IsProcessed = true);

            MigrationJobContext.SaveMigrationUnit(mu, true);
        }

        private void LogPipelineSummary(ExecutionResult execution, PipelineRequest request)
        {
            long sessionWritten = execution.Tracker.TotalCopied - request.MigrationUnit.CopyRowsCopied;
            double speed = execution.Elapsed.TotalSeconds > 0 ? sessionWritten / execution.Elapsed.TotalSeconds : 0;
            int completedCount;
            lock (execution.Context.Ranges.Checkpoints) { completedCount = execution.Context.Ranges.Completed.Count; }

            _log.WriteLine($"Pipeline complete for {request.Context.KeyspaceName}.{request.Context.TableName}: " +
                $"session={sessionWritten:N0} written, {execution.Tracker.TotalFailed:N0} failed | " +
                $"cumulative={execution.Tracker.TotalCopied:N0} | {completedCount}/{request.FeedRanges.Count} ranges | " +
                $"{execution.Elapsed.TotalSeconds:F1}s ({speed:F0} rows/sec)", LogType.Info);
        }

        private static TaskResult DetermineOutcome(PipelineContext ctx, long failedCount)
        {
            if (Volatile.Read(ref ctx.Counters.FatalErrorFlag) != 0)
                return TaskResult.Abort;
            if (ctx.Counters.WorkerErrors.Any(r => r == TaskResult.Abort))
                return TaskResult.Abort;
            if (ctx.Counters.WorkerErrors.Any(r => r == TaskResult.Canceled))
                return TaskResult.Canceled;
            return failedCount > 0 ? TaskResult.Retry : TaskResult.Success;
        }

    }
}
