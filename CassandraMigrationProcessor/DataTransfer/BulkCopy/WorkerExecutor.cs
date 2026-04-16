using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Creates the pipeline context, launches N workers,
/// waits for completion, and determines the outcome.
/// </summary>
internal class WorkerExecutor
{
    private readonly MigrationLog _log;
    private readonly Job _job;
    private readonly PipelineConfig _pipelineConfig;
    private readonly CancellationToken _ct;

    public WorkerExecutor(MigrationLog log, Job job, PipelineConfig pipelineConfig, CancellationToken ct)
    {
        _log = log;
        _job = job;
        _pipelineConfig = pipelineConfig;
        _ct = ct;
    }

    public record ExecutionResult(
        CopyProgressTracker Tracker,
        PipelineContext Context,
        TimeSpan Elapsed);

    public async Task<ExecutionResult> ExecuteAsync(
        PipelineRequest request, PartitionSeeder.SeedResult seed,
        SchemaMigrator.SchemaResult schema, ISession targetSession)
    {
        int workerCount = _pipelineConfig.WorkerCount;
        int pageSize = _pipelineConfig.PageSize;
        var ctx0 = request.Context;
        long priorCopied = request.TableMigration.CopyRowsCopied;

        var tracker = new CopyProgressTracker(_log, workerCount, priorCopied,
            request.TableMigration,
            new ProgressConfig(request.ChunkIndex, request.InitialPercent, request.ContributionFactor, request.TotalRowCount));

        var stopwatch = Stopwatch.StartNew();

        var ctx = new PipelineContext(
            seed.Pool,
            new WorkerConfig(_job.SourceConnection, _job.TargetConnection, schema.Columns, ctx0),
            new RangeState(seed.Completed, seed.Checkpoints, request.FeedRanges),
            new PipelineCounters(),
            tracker);

        _log.WriteLine($"Launching {workerCount} workers for {ctx0.KeyspaceName}.{ctx0.TableName} ({seed.PendingCount} feed ranges, page size={pageSize})...", LogType.Info);
        using var pool = new WorkerPool(_log, workerCount);
        pool.Start(workerId => new BulkCopyWorker(_log, _ct, workerId, pageSize).RunAsync(ctx));
        await pool.WaitForCompletionAsync();
        ctx.PartitionPool.Writer.TryComplete();

        return new ExecutionResult(tracker, ctx, stopwatch.Elapsed);
    }

    public TaskResult Finalize(ExecutionResult execution, PipelineRequest request)
    {
        execution.Tracker.LogFinal();
        execution.Tracker.UpdateMigrationUnit();
        // Force-flush checkpoint to disk (UpdateMigrationUnit uses a
        // timer and may skip if called too soon after the last save)
        MigrationJobContext.Instance.SaveMigrationUnit(request.TableMigration, true);
        LogPipelineSummary(execution, request);
        return DetermineOutcome(execution.Context.Counters, execution.Tracker.TotalFailed);
    }

    private void LogPipelineSummary(ExecutionResult execution, PipelineRequest request)
    {
        long sessionWritten = execution.Tracker.TotalCopied - request.TableMigration.CopyRowsCopied;
        double speed = execution.Elapsed.TotalSeconds > 0 ? sessionWritten / execution.Elapsed.TotalSeconds : 0;
        int completedCount;
        lock (execution.Context.Ranges.Checkpoints) { completedCount = execution.Context.Ranges.Completed.Count; }

        _log.WriteLine($"Pipeline complete for {request.Context.KeyspaceName}.{request.Context.TableName}: " +
            $"session={sessionWritten:N0} written, {execution.Tracker.TotalFailed:N0} failed | " +
            $"cumulative={execution.Tracker.TotalCopied:N0} | {completedCount}/{request.FeedRanges.Count} ranges | " +
            $"{execution.Elapsed.TotalSeconds:F1}s ({speed:F0} rows/sec)", LogType.Info);
    }

    private static TaskResult DetermineOutcome(PipelineCounters counters, long failedCount)
    {
        if (Volatile.Read(ref counters.FatalErrorFlag) != 0)
            return TaskResult.Abort;
        if (counters.WorkerErrors.Any(r => r == TaskResult.Abort))
            return TaskResult.Abort;
        if (counters.WorkerErrors.Any(r => r == TaskResult.Canceled))
            return TaskResult.Canceled;
        return failedCount > 0 ? TaskResult.Retry : TaskResult.Success;
    }
}
