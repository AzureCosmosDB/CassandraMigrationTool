using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using JobWriteConsistencyLevel =
    CassandraMigrationProcessor.Models.TargetWriteConsistencyLevel;

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
    int MaxWriteRetries,
    ConsistencyLevel TargetWriteConsistencyLevel)
{
    /// <summary>
    /// Resolves configuration from job overrides, app settings, and defaults.
    /// Priority: Job > Settings > Defaults.
    /// </summary>
    public static PipelineConfig Resolve(Job job, AppSettings settings)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(settings);

        // No job-level override: size the shared worker pool to the host's
        // compute budget.
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

        if (!Enum.IsDefined(job.TargetWriteConsistencyLevel))
            throw new ArgumentOutOfRangeException(
                nameof(job.TargetWriteConsistencyLevel),
                job.TargetWriteConsistencyLevel,
                "Unsupported target write consistency level. Expected one of: " +
                string.Join(", ", Enum.GetNames<JobWriteConsistencyLevel>()) + ".");

        return new PipelineConfig(
            WorkerCount: workerCount,
            PageSize: pageSize,
            ChangeFeedPollIntervalMs: cfPollMs,
            MaxReadRetries: maxReadRetries,
            MaxWriteRetries: maxWriteRetries,
            TargetWriteConsistencyLevel: ToDriverConsistencyLevel(
                job.TargetWriteConsistencyLevel));
    }

    private static ConsistencyLevel ToDriverConsistencyLevel(
        JobWriteConsistencyLevel consistencyLevel)
        => consistencyLevel switch
        {
            JobWriteConsistencyLevel.LocalOne => ConsistencyLevel.LocalOne,
            JobWriteConsistencyLevel.One => ConsistencyLevel.One,
            JobWriteConsistencyLevel.Two => ConsistencyLevel.Two,
            JobWriteConsistencyLevel.Three => ConsistencyLevel.Three,
            JobWriteConsistencyLevel.Quorum => ConsistencyLevel.Quorum,
            JobWriteConsistencyLevel.LocalQuorum => ConsistencyLevel.LocalQuorum,
            JobWriteConsistencyLevel.EachQuorum => ConsistencyLevel.EachQuorum,
            JobWriteConsistencyLevel.All => ConsistencyLevel.All,
            JobWriteConsistencyLevel.Any => ConsistencyLevel.Any,
            _ => throw new ArgumentOutOfRangeException(
                nameof(consistencyLevel),
                consistencyLevel,
                "Unsupported target write consistency level. Expected one of: " +
                string.Join(", ", Enum.GetNames<JobWriteConsistencyLevel>()) + ".")
        };
}
