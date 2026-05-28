using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Per-job worker configuration. Carries the <see cref="Job"/> so worker
/// sessions are built via the Job-aware factory overloads — that path
/// lazily fetches AAD tokens / ARM credentials and caches them onto the
/// job before any session is opened. Snapshotting <c>ConnectionOptions</c>
/// here would skip that step and crash workers with a null-password
/// error when source AAD is enabled.
/// </summary>
internal record WorkerConfig(
    Job Job,
    TokenRefreshManager? TokenRefreshManager,
    bool EnableReplay,
    int ReplayCooldownMs);

/// <summary>
/// Wires the job-wide configuration, partition channel, and control
/// flags together for every worker.
/// </summary>
/// Job-level control flags shared by every worker: a fatal-error
/// latch, a hook that cancels the job-wide CTS when fatal is tripped,
/// and the collected per-worker outcomes. Per-table progress
/// counters live on <see cref="TableResources.Tracker"/>.
/// </summary>
internal class JobControlFlags
{
    public int FatalErrorFlag;
    public ConcurrentBag<TaskResult> WorkerErrors { get; } = new();

    /// <summary>
    /// Wired by <see cref="JobPipeline"/> to cancel the job-wide CTS.
    /// Workers invoke this together with setting <see cref="FatalErrorFlag"/>
    /// so all coordinators waiting on per-table <c>BulkDrainSignal</c>
    /// (under the pipeline CTS) unblock immediately instead of hanging
    /// until external cancel.
    /// </summary>
    public Action? TriggerFatalShutdown { get; set; }

    /// <summary>
    /// Idempotent fatal trip: sets the latch and cancels the job CTS.
    /// Safe to call from any worker / strategy.
    /// </summary>
    public void TripFatal()
    {
        Interlocked.Exchange(ref FatalErrorFlag, 1);
        try { TriggerFatalShutdown?.Invoke(); } catch { }
    }
}

public record ProgressConfig(
    int ChunkIndex,
    double InitialPercent,
    double ContributionFactor,
    long TotalRowCount);

/// <summary>
/// Shared (job-wide) state passed to every worker. Holds the
/// <see cref="DataTransfer.PartitionManager"/> that all tables seed into and
/// every worker pulls from, plus the connection/replay configuration and
/// global control flags. Per-table state is resolved via
/// <see cref="Partition.Resources"/>.
/// </summary>
internal record PipelineContext(
    PartitionManager Partitions,
    WorkerConfig Worker,
    JobControlFlags Flags)
{
    public Job Job => Worker.Job;
    public bool EnableReplay => Worker.EnableReplay;
}
