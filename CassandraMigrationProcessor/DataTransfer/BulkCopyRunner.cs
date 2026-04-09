using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Executes the bulk copy pipeline for a single table:
    /// seed partitions → sync schema → run workers → finalize.
    ///
    ///  Partition Pool (Channel) ──► Worker (read + write)
    ///         ▲                         │
    ///         └──── recycle ◄────────────┘ (if more pages)
    /// </summary>
    internal class BulkCopyRunner
    {
        private readonly MigrationLog _log;
        private readonly MigrationJob _job;
        private readonly MigrationSettings _config;
        private readonly CancellationTokenSource _cancellation;
        private readonly Func<ISession> _ensureTargetSession;

        public BulkCopyRunner(MigrationLog log, MigrationJob job, MigrationSettings config,
            CancellationTokenSource cancellation, Func<ISession> ensureTargetSession)
        {
            _log = log;
            _job = job;
            _config = config;
            _cancellation = cancellation;
            _ensureTargetSession = ensureTargetSession;
        }

        // ── Public entry point ──

        public async Task<TaskResult> RunAsync(PipelineRequest request)
        {
            var mu = request.MigrationUnit;
            var ctx0 = request.Context;
            int workerCount = ResolveWorkerCount();
            int pageSize = ResolvePageSize();

            // Stage 1: Partition seeding
            var partitions = await SeedPartitionsAsync(mu, request.FeedRanges, ctx0.KeyspaceName, ctx0.TableName);
            if (partitions == null)
                return TaskResult.Success;

            // Stage 2: Schema sync
            var columns = await SchemaManager.SyncSchemaAsync(
                ctx0.SourceSession, _ensureTargetSession(),
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

            // Stage 3: Worker execution
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

            // Stage 4: Finalize
            return FinalizeResults(ctx, mu, request, priorCopied, stopwatch.Elapsed);
        }

        // ── Partition seeding ──

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
                pendingRanges = feedRanges.Where(r => !completed.Contains(r)).ToList();
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
                if (checkpoints.TryGetValue(range, out var base64Token) && base64Token != null)
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

        // ── Worker loop ──

        private async Task RunWorkerAsync(int workerId, PipelineContext ctx, int pageSize)
        {
            ctx.Tracker.WorkerStarted();
            PageReader? reader = null;
            PageWriter? writer = null;
            try
            {
                reader = new PageReader(_log, ctx.Worker.SourceConnection, ctx.Worker.Context.KeyspaceName,
                    ctx.Worker.Columns.Select(c => c.Name).ToList(), pageSize, workerId, _cancellation);
                writer = new PageWriter(_log, ctx.Worker.TargetConnection, ctx.Worker.Columns,
                    ctx.Worker.Context.TargetKeyspaceName, ctx.Worker.Context.TargetTableName, pageSize, workerId, _cancellation);

                while (!_cancellation.Token.IsCancellationRequested && Volatile.Read(ref ctx.Counters.FatalErrorFlag) == 0)
                {
                    var partition = await TakeNextPartitionAsync(ctx);
                    if (partition == null) break;

                    try
                    {
                        if (!partition.IsExhausted)
                        {
                            var result = await reader.ReadAsync(partition, ctx);
                            if (result == null)
                            {
                                _log.WriteLine($"[W{workerId}] FATAL: Read failed — failing job", LogType.Error);
                                Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                                SafeCancel();
                                break;
                            }

                            if (!result.IsLastPage)
                                await ctx.PartitionPool.Writer.WriteAsync(partition, _cancellation.Token);

                            await writer.WriteAsync(result.Rows, result.WorkChunk, ctx);
                        }

                        SavePartitionCheckpoint(partition, ctx);
                        if (partition.IsExhausted) MarkRangeCompleted(partition, ctx);
                    }
                    catch (OperationCanceledException)
                    {
                        ctx.Counters.WorkerErrors.Add(TaskResult.Canceled);
                        SavePartitionCheckpoint(partition, ctx);
                        ctx.PartitionPool.Writer.TryComplete();
                    }
                    catch (Exception ex)
                    {
                        _log.WriteLine($"[W{workerId}] Error: {ex.GetType().Name}: {ex.Message}", LogType.Error);

                        if (ExceptionClassifier.IsFatal(ex))
                        {
                            _log.WriteLine($"[W{workerId}] FATAL — failing job", LogType.Error);
                            Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                            SafeCancel();
                            ctx.Counters.WorkerErrors.Add(TaskResult.Abort);
                        }
                        else
                        {
                            ctx.Counters.WorkerErrors.Add(TaskResult.Retry);
                        }

                        SavePartitionCheckpoint(partition, ctx);
                        ctx.Tracker.RangeCompleted(partition.FeedRange, TaskResult.Retry);
                        ctx.PartitionPool.Writer.TryComplete();
                    }
                    finally
                    {
                        ctx.Tracker.UpdateMigrationUnit();
                    }
                }
            }
            finally
            {
                MigrationUtilities.SafeDispose(writer, "worker PageWriter");
                MigrationUtilities.SafeDispose(reader, "worker PageReader");
                ctx.Tracker.WorkerExited();
            }
        }

        // ── Helpers ──

        private void SafeCancel()
        {
            try { _cancellation.Cancel(); }
            catch (Exception ex) { Console.Error.WriteLine($"[WARN] BulkCopyRunner cancel failed: {ex.Message}"); }
        }

        private static void SavePartitionCheckpoint(Partition partition, PipelineContext ctx)
        {
            lock (ctx.Ranges.Checkpoints)
            {
                var token = partition.GetResumeToken();
                if (token != null)
                    ctx.Ranges.Checkpoints[partition.FeedRange] = Convert.ToBase64String(token);
                else if (partition.LastPagingState != null)
                    ctx.Ranges.Checkpoints[partition.FeedRange] = Convert.ToBase64String(partition.LastPagingState);
            }
        }

        private static void MarkRangeCompleted(Partition partition, PipelineContext ctx)
        {
            lock (ctx.Ranges.Checkpoints)
            {
                ctx.Ranges.Checkpoints.Remove(partition.FeedRange);
                ctx.Ranges.Completed.Add(partition.FeedRange);
            }
            ctx.Tracker.RangeCompleted(partition.FeedRange, TaskResult.Success);
            if (ctx.Ranges.Completed.Count >= ctx.Ranges.FeedRanges.Count)
                ctx.PartitionPool.Writer.TryComplete();
        }

        private async Task<Partition?> TakeNextPartitionAsync(PipelineContext ctx)
        {
            try
            {
                if (await ctx.PartitionPool.Reader.WaitToReadAsync(_cancellation.Token))
                    if (ctx.PartitionPool.Reader.TryRead(out var p))
                        return p;
            }
            catch (OperationCanceledException) { }
            return null;
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
