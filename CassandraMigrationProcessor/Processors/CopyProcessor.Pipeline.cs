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
        ///  Partition Pool (Channel) ──► Worker (read + write)
        ///         ▲                         │
        ///         └──── recycle ◄────────────┘ (if more pages)
        ///
        /// Each worker takes a partition, reads one page,
        /// creates a WorkChunk, recycles the partition back
        /// to the pool (so another worker can read the next
        /// page), then writes rows and marks the chunk done.
        /// </summary>
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
                _log.WriteLine($"All {feedRanges.Count} ranges already completed for {processorContext.KeyspaceName}.{processorContext.TableName}");
                return TaskResult.Success;
            }

            _log.WriteLine($"Pipeline copy: {pendingRanges.Count} ranges ({completed.Count} already done), {workerCount} workers for {processorContext.KeyspaceName}.{processorContext.TableName}");

            if (!await CassandraHelper.TableExistsAsync(_targetSession!, processorContext.TargetKeyspaceName,
                processorContext.TargetTableName))
            {
                await CassandraHelper.EnsureKeyspaceExistsAsync(_targetSession!, processorContext.TargetKeyspaceName);
                await CassandraHelper.CreateTableFromSourceAsync(_sourceSession!, _targetSession!,
                    processorContext.KeyspaceName, processorContext.TableName,
                    processorContext.TargetKeyspaceName, processorContext.TargetTableName);
                _log.WriteLine($"Created target table {processorContext.TargetKeyspaceName}.{processorContext.TargetTableName}");
            }
            else
            {
                await CassandraHelper.CreateTableFromSourceAsync(_sourceSession!, _targetSession!,
                    processorContext.KeyspaceName, processorContext.TableName,
                    processorContext.TargetKeyspaceName, processorContext.TargetTableName);
            }

            var columns = await CassandraHelper.GetTableColumnsAsync(
                _sourceSession!, processorContext.KeyspaceName, processorContext.TableName);
            if (columns.Count == 0)
            {
                _log.WriteLine($"No columns for {processorContext.KeyspaceName}.{processorContext.TableName}", LogType.Error);
                return TaskResult.Abort;
            }

            var columnNames = columns.Select(c => c.Name).ToList();

            var partitionPool = Channel.CreateBounded<Partition>(new BoundedChannelOptions(
                    pendingRanges.Count + workerCount)
                {
                    FullMode = BoundedChannelFullMode.Wait
                });

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
                await partitionPool.Writer.WriteAsync(new Partition(range, pagingState));
            }
            if (resumedCount > 0)
                _log.WriteLine($"Resuming {resumedCount}/{pendingRanges.Count} ranges from checkpoint");

            long priorCopied = migrationUnit.CopyRowsCopied;

            int jobPageSize = _job?.PageSize ?? 0;
            int configuredPageSize = jobPageSize > 0
                ? jobPageSize
                : _config.CqlCopyPageSize > 0
                    ? _config.CqlCopyPageSize
                    : MigrationDefaults.DefaultPageSize;

            var tracker = new CopyProgressTracker(_log, processorContext.KeyspaceName, processorContext.TableName,
                workerCount, pendingRanges.Count,
                priorCopied);

            var stopwatch = Stopwatch.StartNew();

            var ctx = new PipelineContext
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
                FatalErrorFlag = 0,
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

            _log.WriteLine($"Launching {workerCount} workers for {processorContext.KeyspaceName}.{processorContext.TableName} ({pendingRanges.Count} feed ranges, page size={configuredPageSize})...");
            using var pool = new WorkerPool(_log, workerCount, _cancellation);
            pool.Start(workerId => RunWorkerAsync(workerId, ctx));
            await pool.WaitForCompletionAsync();
            ctx.PartitionPool.Writer.TryComplete();

            ctx.Tracker.LogFinal();
            long finalWritten = Volatile.Read(ref ctx.TotalWritten);
            long finalFailed = Volatile.Read(ref ctx.TotalFailed);
            long finalRead = Volatile.Read(ref ctx.TotalRead);
            long sessionWritten = finalWritten - priorCopied;

            var elapsed = stopwatch.Elapsed;
            double avgSpeed = elapsed.TotalSeconds > 0
                ? sessionWritten / elapsed.TotalSeconds : 0;
            int completedCount;
            lock (ctx.Checkpoints) { completedCount = ctx.Completed.Count; }
            _log.WriteLine($"Pipeline complete for {processorContext.KeyspaceName}.{processorContext.TableName}: " +
                $"session={sessionWritten:N0} written, {finalFailed:N0} failed | " +
                $"cumulative={finalWritten:N0} | {completedCount}/{feedRanges.Count} ranges | " +
                $"{elapsed.TotalSeconds:F1}s ({avgSpeed:F0} rows/sec)");

            var chunk = migrationUnit.MigrationChunks[chunkIndex];
            chunk.SourceResultRowCount = finalWritten;
            chunk.TargetInsertedRowCount = finalWritten;
            chunk.TargetFailedRowCount = finalFailed;
            migrationUnit.CopyRowsCopied = finalWritten;
            migrationUnit.ActualRowCount = Math.Max(migrationUnit.ActualRowCount, finalRead);
            bool allRangesComplete;
            lock (ctx.Checkpoints)
            {
                allRangesComplete = ctx.Completed.Count >= feedRanges.Count;
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

            if (Volatile.Read(ref ctx.FatalErrorFlag) != 0)
                return TaskResult.Abort;
            if (ctx.WorkerErrors.Any(r => r == TaskResult.Abort))
                return TaskResult.Abort;
            if (ctx.WorkerErrors.Any(r => r == TaskResult.Canceled))
                return TaskResult.Canceled;
            if (finalFailed > 0)
                return TaskResult.Retry;
            return TaskResult.Success;
        }
    }
}
