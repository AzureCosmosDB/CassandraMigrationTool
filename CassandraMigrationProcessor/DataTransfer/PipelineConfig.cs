using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Resolved pipeline configuration. Merges values from
    /// MigrationJob (per-job overrides) → MigrationSettings
    /// (app config) → MigrationDefaults (compile-time).
    /// Created once per pipeline run, immutable after construction.
    /// </summary>
    internal record PipelineConfig(
        int WorkerCount,
        int PageSize,
        int MaxFeedRangeParallelism,
        int ChangeFeedPollIntervalMs,
        int CheckpointIntervalSeconds)
    {
        /// <summary>
        /// Resolves configuration from job overrides, app settings, and defaults.
        /// Priority: Job > Settings > Defaults.
        /// </summary>
        public static PipelineConfig Resolve(MigrationJob job, MigrationSettings settings)
        {
            int workerCount = job.MaxFeedRangeParallelism > 0
                ? job.MaxFeedRangeParallelism
                : AutoWorkerCount(job.ParallelThreads);

            int pageSize = job.PageSize > 0
                ? job.PageSize
                : settings.CqlCopyPageSize > 0
                    ? settings.CqlCopyPageSize
                    : MigrationDefaults.DefaultPageSize;

            int cfPollMs = settings.ChangeFeedPollIntervalMs > 0
                ? settings.ChangeFeedPollIntervalMs
                : 5000;

            return new PipelineConfig(
                WorkerCount: workerCount,
                PageSize: pageSize,
                MaxFeedRangeParallelism: Math.Max(1, job.MaxFeedRangeParallelism),
                ChangeFeedPollIntervalMs: cfPollMs,
                CheckpointIntervalSeconds: MigrationDefaults.CheckpointIntervalSeconds);
        }

        private static int AutoWorkerCount(int parallelTables)
        {
            int totalBudget = Environment.ProcessorCount * MigrationDefaults.WorkerMultiplier;
            int tables = Math.Max(1, parallelTables);
            return Math.Max(MigrationDefaults.MinWorkers, totalBudget / tables);
        }
    }
}
