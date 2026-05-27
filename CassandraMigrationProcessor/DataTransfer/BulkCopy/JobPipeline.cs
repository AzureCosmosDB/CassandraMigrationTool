using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

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

    public JobPipeline(MigrationLog log, Job job, PipelineConfig pipelineConfig, CancellationToken externalToken)
    {
        _log = log;
        _pipelineConfig = pipelineConfig;
        _cts = externalToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(externalToken)
            : new CancellationTokenSource();

        bool enableReplay = MigrationUtilities.IsOnline(job);
        // Bounded channel sized to the worker pool plus a small backlog —
        // partitions are recycled hot, so the channel only ever holds
        // a handful at a time even with many tables registered.
        int capacity = Math.Max(pipelineConfig.WorkerCount * 4, 64);
        var pool = Channel.CreateBounded<Partition>(new BoundedChannelOptions(capacity)
            { FullMode = BoundedChannelFullMode.Wait });

        Context = new PipelineContext(
            pool,
            new WorkerConfig(job.SourceConnection, job.TargetConnection,
                EnableReplay: enableReplay,
                ReplayCooldownMs: pipelineConfig.ChangeFeedPollIntervalMs),
            new PipelineCounters());

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

    /// <summary>Completes the partition channel; workers will drain and exit.</summary>
    public void CompletePartitionChannel() => Context.PartitionPool.Writer.TryComplete();

    /// <summary>Waits for all workers to finish (offline mode only).</summary>
    public Task WaitForCompletionAsync() => _pool.WaitForCompletionAsync();

    public void Stop() => _cts.Cancel();

    public void Dispose()
    {
        try { _cts.Cancel(); } catch { }
        MigrationUtilities.SafeDispose(_pool, "JobPipeline WorkerPool");
        _cts.Dispose();
    }
}
