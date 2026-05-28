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
    {        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(settings);

        // No job-level override: size the shared worker pool to the host's
        // compute budget. Intentionally independent of job.ParallelThreads —
        // the pool is shared across all tables, so dividing by table-fanout
        // would shrink total throughput rather than partition it.
        int workerCount = job.WorkerCount > 0
            ? job.WorkerCount
            : Math.Max(MigrationDefaults.MinWorkers,
                Environment.ProcessorCount * MigrationDefaults.WorkerMultiplier);

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
}
