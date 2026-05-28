using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;
/// <summary>
/// Resolved pipeline configuration. Merges values from
/// Job (per-job overrides) → AppSettings
/// (app config) → MigrationDefaults (compile-time).
/// Created once per pipeline run, immutable after construction.
/// </summary>
public record PipelineConfig(
    int WorkerCount,
    int PageSize,
    int ChangeFeedPollIntervalMs,
    int MaxReadRetries,
    int MaxWriteRetries)
{
    /// <summary>
    /// Resolves configuration from job overrides, app settings, and defaults.
    /// Priority: Job > Settings > Defaults.
    /// </summary>
    public static PipelineConfig Resolve(Job job, AppSettings settings)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(settings);

        int workerCount = job.MaxFeedRangeParallelism > 0
            ? job.MaxFeedRangeParallelism
            : AutoWorkerCount();

        int pageSize = job.PageSize > 0
            ? job.PageSize
            : settings.CqlCopyPageSize;

        int cfPollMs = settings.ChangeFeedPollIntervalMs > 0
            ? settings.ChangeFeedPollIntervalMs
            : 5000;

        int maxReadRetries = job.MaxReadRetries > 0
            ? job.MaxReadRetries
            : MigrationDefaults.DefaultMaxReadRetries;

        int maxWriteRetries = job.MaxWriteRetries > 0
            ? job.MaxWriteRetries
            : MigrationDefaults.DefaultMaxWriteRetries;

        return new PipelineConfig(
            WorkerCount: workerCount,
            PageSize: pageSize,
            ChangeFeedPollIntervalMs: cfPollMs,
            MaxReadRetries: maxReadRetries,
            MaxWriteRetries: maxWriteRetries);
    }

    /// <summary>
    /// Auto-sizes the shared worker pool. The pool is shared across all
    /// tables in the job, so its size is bounded by the host's compute
    /// budget — <see cref="Job.ParallelThreads"/> (max concurrent tables
    /// in orchestration) is intentionally not a factor here, because
    /// scaling the pool down as more tables run concurrently would
    /// reduce total throughput rather than divide it.
    /// </summary>
    private static int AutoWorkerCount()
    {
        int totalBudget = Environment.ProcessorCount * MigrationDefaults.WorkerMultiplier;
        return Math.Max(MigrationDefaults.MinWorkers, totalBudget);
    }
}
