using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Job-wide worker pipeline. Holds one shared partition channel and one
/// pool of N <see cref="DataCopyWorker"/> tasks. Tables register their
/// <see cref="TableResources"/> and seed partitions into the shared
/// channel; workers pick up any partition regardless of source table.
/// Lifetime: created at job start, disposed on job shutdown.
/// </summary>
internal sealed class JobPipeline : IDisposable
{
    private readonly MigrationLog _log;
    private readonly PipelineConfig _pipelineConfig;
    private readonly CancellationTokenSource _cts;
    private readonly WorkerPool _pool;
    public PipelineContext Context { get; }

    public JobPipeline(MigrationLog log, Job job, PipelineConfig pipelineConfig, TokenRefreshManager? tokenRefreshManager, CancellationToken externalToken)
    {
        _log = log;
        _pipelineConfig = pipelineConfig;
        _cts = externalToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(externalToken)
            : new CancellationTokenSource();

        bool enableReplay = MigrationUtilities.IsOnline(job);
        var partitions = new PartitionManager();

        Context = new PipelineContext(
            partitions,
            new WorkerConfig(job, tokenRefreshManager,
                EnableReplay: enableReplay,
                ReplayCooldownMs: pipelineConfig.ChangeFeedPollIntervalMs),
            new JobControlFlags());

        // Wire fatal trip into our CTS so coordinators waiting on
        // per-table BulkDrainSignal under this token unblock as soon
        // as any worker raises a fatal error. If the pipeline has
        // already been disposed the CTS is gone and cancellation is
        // moot — we explicitly check IsCancellationRequested-style
        // disposal via try/catch so we can log it instead of leaving
        // a silent empty catch that could hide unrelated bugs.
        Context.Flags.TriggerFatalShutdown = () =>
        {
            try
            {
                _cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                _log.WriteLine(
                    "Fatal shutdown trigger fired after pipeline disposal — cancellation already moot.",
                    LogType.Info);
            }
        };

        _pool = new WorkerPool(_log, pipelineConfig.WorkerCount);
    }

    public void Start()
    {
        int pageSize = _pipelineConfig.PageSize;
        int maxReadRetries = _pipelineConfig.MaxReadRetries;
        int maxWriteRetries = _pipelineConfig.MaxWriteRetries;
        _log.WriteLine(
            $"Job pipeline: {_pipelineConfig.WorkerCount} shared workers " +
            $"(replay={Context.EnableReplay}, page size={pageSize})",
            LogType.Info);
        _pool.Start(workerId => new DataCopyWorker(_log, _cts.Token, workerId, pageSize, maxReadRetries, maxWriteRetries).RunAsync(Context));
    }

    /// <summary>Completes the partition pool; workers will drain and exit.</summary>
    public void CompletePartitionChannel() => Context.Partitions.Complete();

    /// <summary>Waits for all workers to finish (offline mode only).</summary>
    public Task WaitForCompletionAsync() => _pool.WaitForCompletionAsync();

    /// <summary>True when every worker task has exited (faulted, cancelled, or returned).</summary>
    public bool AllWorkersExited => _pool.AllExited;

    /// <summary>Number of workers that completed with faults.</summary>
    public int FaultedWorkerCount => _pool.FaultedCount;

    public void Stop() => _cts.Cancel();

    public void Dispose()
    {
        // Dispose can race with the fatal-shutdown wiring above. Only
        // ObjectDisposedException is expected here (double-dispose);
        // anything else should surface.
        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException) { /* already disposed */ }
        MigrationUtilities.SafeDispose(_pool, "JobPipeline WorkerPool");
        _cts.Dispose();
    }
}
