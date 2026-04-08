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
            MigrationUnit migrationUnit,
            int chunkIndex,
            double initialPercent,
            double contributionFactor,
            long totalRowCount,
            ProcessorContext processorContext,
            List<string> feedRanges)
        {
            // Calculate workers: configured or auto
            // Target ~100 total workers on 8 vCPU, scaled by cores
            int totalBudget = Environment.ProcessorCount * 13;
            int parallelTables = Math.Max(1, _job.ParallelThreads);
            int autoWorkers = Math.Max(4, totalBudget / parallelTables);
            int workerCount = _job.MaxFeedRangeParallelism > 0
                ? _job.MaxFeedRangeParallelism
                : autoWorkers;

            // ── Resume: filter out completed ranges ─────────
            var completed = migrationUnit.CompletedCopyFeedRanges
                ?? new HashSet<string>();
            var checkpoints = migrationUnit.CopyFeedRangeCheckpoints
                ?? new Dictionary<string, string?>();
            migrationUnit.CompletedCopyFeedRanges = completed;
            migrationUnit.CopyFeedRangeCheckpoints = checkpoints;

            var pendingRanges = feedRanges
                .Where(r => !completed.Contains(r))
                .ToList();

            if (pendingRanges.Count == 0)
            {
                _log.WriteLine(
                    $"All {feedRanges.Count} ranges already " +
                    $"completed for {processorContext.KeyspaceName}" +
                    $".{processorContext.TableName}");
                return TaskResult.Success;
            }

            _log.WriteLine(
                $"Pipeline copy: {pendingRanges.Count} ranges " +
                $"({completed.Count} already done), " +
                $"{workerCount} workers " +
                $"for {processorContext.KeyspaceName}.{processorContext.TableName}");

            // Schema setup
            if (!await CassandraHelper.TableExistsAsync(
                _targetSession!, processorContext.TargetKeyspaceName,
                processorContext.TargetTableName)
                .ConfigureAwait(false))
            {
                await CassandraHelper.EnsureKeyspaceExistsAsync(
                    _targetSession!, processorContext.TargetKeyspaceName)
                    .ConfigureAwait(false);
                await CassandraHelper.CreateTableFromSourceAsync(
                    _sourceSession!, _targetSession!,
                    processorContext.KeyspaceName, processorContext.TableName,
                    processorContext.TargetKeyspaceName, processorContext.TargetTableName)
                    .ConfigureAwait(false);
                _log.WriteLine(
                    $"Created target table " +
                    $"{processorContext.TargetKeyspaceName}" +
                    $".{processorContext.TargetTableName}");
            }
            else
            {
                await CassandraHelper.CreateTableFromSourceAsync(
                    _sourceSession!, _targetSession!,
                    processorContext.KeyspaceName, processorContext.TableName,
                    processorContext.TargetKeyspaceName, processorContext.TargetTableName)
                    .ConfigureAwait(false);
            }

            var columns = await CassandraHelper.GetTableColumnsAsync(
                _sourceSession!, processorContext.KeyspaceName, processorContext.TableName)
                .ConfigureAwait(false);
            if (columns.Count == 0)
            {
                _log.WriteLine(
                    $"No columns for {processorContext.KeyspaceName}" +
                    $".{processorContext.TableName}", LogType.Error);
                return TaskResult.Abort;
            }

            var columnNames = columns.Select(c => c.Name).ToList();

            // ── Partition pool channel ───────────────────────
            var partitionPool = Channel.CreateBounded<Partition>(
                new BoundedChannelOptions(
                    pendingRanges.Count + workerCount)
                {
                    FullMode = BoundedChannelFullMode.Wait
                });

            // Seed channel with pending ranges (resume-aware)
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
                await partitionPool.Writer.WriteAsync(
                    new Partition(range, pagingState));
            }
            if (resumedCount > 0)
                _log.WriteLine(
                    $"Resuming {resumedCount}/{pendingRanges.Count}" +
                    $" ranges from checkpoint");

            // Seed counters from prior run for resume
            long priorCopied = migrationUnit.CopyRowsCopied;

            // Page size
            int jobPageSize = _job?.PageSize ?? 0;
            int configuredPageSize = jobPageSize > 0
                ? jobPageSize
                : _config.CqlCopyPageSize > 0
                    ? _config.CqlCopyPageSize
                    : 500;

            var tracker = new CopyProgressTracker(
                _log, processorContext.KeyspaceName, processorContext.TableName,
                workerCount, pendingRanges.Count,
                priorCopied);

            var stopwatch = Stopwatch.StartNew();

            // ── Build shared context ────────────────────────
            var pipeline = new PipelineContext
            {
                PartitionPool = partitionPool,
                ColumnNames = columnNames,
                Columns = columns,
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
                Context = processorContext,
                MigrationUnit = migrationUnit,
                Job = _job,
                ChunkIndex = chunkIndex,
                InitialPercent = initialPercent,
                ContributionFactor = contributionFactor,
                TotalRowCount = totalRowCount,
                LastCheckpointTicks = DateTime.UtcNow.Ticks,
            };

            // ── LAUNCH UNIFIED WORKERS ──────────────────────
            _log.WriteLine(
                $"Launching {workerCount} workers " +
                $"for {processorContext.KeyspaceName}.{processorContext.TableName} " +
                $"({pendingRanges.Count} feed ranges, " +
                $"page size={configuredPageSize})...");
            var workers = Enumerable.Range(0, workerCount)
                .Select(workerId => Task.Run(
                    () => RunWorkerAsync(workerId, pipeline)))
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
            pipeline.PartitionPool.Writer.TryComplete();

            // ── Final stats ─────────────────────────────────
            pipeline.Tracker.LogFinal();
            long finalWritten = Interlocked.Read(
                ref pipeline.TotalWritten);
            long finalFailed = Interlocked.Read(
                ref pipeline.TotalFailed);
            long finalRead = Interlocked.Read(
                ref pipeline.TotalRead);

            var elapsed = stopwatch.Elapsed;
            double avgSpeed = elapsed.TotalSeconds > 0
                ? finalWritten / elapsed.TotalSeconds : 0;
            _log.WriteLine(
                $"Pipeline complete for {processorContext.KeyspaceName}" +
                $".{processorContext.TableName}:");
            _log.WriteLine(
                $"  Total read:    {finalRead:N0} rows");
            _log.WriteLine(
                $"  Total written: {finalWritten:N0} rows");
            _log.WriteLine(
                $"  Total failed:  {finalFailed:N0} rows");
            _log.WriteLine(
                $"  Ranges:        " +
                $"{pipeline.Completed.Count}/{feedRanges.Count} completed");
            _log.WriteLine(
                $"  Duration:      {elapsed.TotalSeconds:F1}s");
            _log.WriteLine(
                $"  Avg speed:     {avgSpeed:F0} rows/sec");
            _log.WriteLine(
                $"  Workers used:  {workerCount}");

            // Final chunk update
            var chunk = migrationUnit.MigrationChunks[chunkIndex];
            chunk.SourceResultRowCount = finalWritten;
            chunk.TargetInsertedRowCount = finalWritten;
            chunk.TargetFailedRowCount = finalFailed;
            migrationUnit.CopyRowsCopied = finalWritten;
            migrationUnit.ActualRowCount = Math.Max(
                migrationUnit.ActualRowCount, finalRead);
            bool allRangesComplete =
                pipeline.Completed.Count >= feedRanges.Count;
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

            if (Volatile.Read(
                ref pipeline.NonRetriableHitFlag) != 0)
                return TaskResult.Abort;
            if (pipeline.WorkerErrors.Any(
                r => r == TaskResult.Abort))
                return TaskResult.Abort;
            if (pipeline.WorkerErrors.Any(
                r => r == TaskResult.Canceled))
                return TaskResult.Canceled;
            if (finalFailed > 0)
                return TaskResult.Retry;
            return TaskResult.Success;
        }
    }
}
