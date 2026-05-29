using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Job-wide worker pipeline. Holds one shared partition channel and one
/// pool of N <see cref="DataCopyWorker"/> tasks. Tables register their
/// <see cref="TableResources"/> and seed partitions into the shared
/// channel; workers pick up any partition regardless of source table.
/// Lifetime: created at job start, disposed on job shutdown.
/// </summary>
internal sealed class JobPipeline : IDisposable, IAsyncDisposable
{
    private readonly MigrationLog _log;
    private readonly PipelineConfig _pipelineConfig;
    private readonly CancellationTokenSource _cts;
    private readonly WorkerPool _workerPool;
    private readonly CooldownScheduler _cooldown;
    private readonly Action _pauseHandler;
    public PipelineContext Context { get; }

    public JobPipeline(MigrationLog log, Job job, PipelineConfig pipelineConfig, JobPartitioning partitioning, TokenRefreshManager? tokenRefreshManager, CancellationToken externalToken)
    {
        _log = log;
        _pipelineConfig = pipelineConfig;
        _cts = externalToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(externalToken)
            : new CancellationTokenSource();

        // Pause is a soft flag, so coordinators blocked on
        // BulkDrainSignal.Task.WaitAsync(_cts.Token) would hang until
        // the external cancellation token fires (i.e. forever, in
        // practice). Cancel the pipeline CTS the moment a pause is
        // requested so those waits unblock and the worker pool tears
        // down promptly. JobManager already maps "cancelled while
        // pause flag is set" to JobStatus.Paused.
        var pipelineCts = _cts;
        _pauseHandler = () =>
        {
            try { pipelineCts.Cancel(); }
            catch (ObjectDisposedException) { /* job already torn down */ }
        };
        MigrationJobContext.Instance.PauseRequested += _pauseHandler;

        bool enableReplay = job.IsOnline;
        var partitions = new PartitionManager(partitioning.AllPartitions);
        var readerConfig = new ReaderConfig(pipelineConfig.PageSize, pipelineConfig.MaxReadRetries);
        var writerConfig = new WriterConfig(pipelineConfig.MaxWriteRetries);
        _cooldown = new CooldownScheduler(
            log,
            partitions,
            pipelineConfig.ChangeFeedPollIntervalMs,
            _cts.Token,
            onLoopFault: () =>
            {
                // CooldownScheduler's background loop has crashed and
                // can no longer drain its queue. If we let workers keep
                // handing partitions to Schedule(), online jobs would
                // park forever short of completion. Cancel the pipeline
                // so workers exit and JobManager can either restart or
                // surface the error.
                try { _cts.Cancel(); }
                catch (ObjectDisposedException) { /* already disposed */ }
            });

        // Wire fatal trip into our CTS at construction so coordinators
        // waiting on per-table BulkDrainSignal under this token unblock
        // as soon as any worker raises a fatal error. ObjectDisposedException
        // during teardown is the only race we expect; everything else is
        // surfaced via TripFatal's logging.
        var cts = _cts;
        var flags = new JobControlFlags(() =>
        {
            try { cts.Cancel(); }
            catch (ObjectDisposedException)
            {
                _log.WriteLine(
                    "Fatal shutdown trigger fired after pipeline disposal — cancellation already moot.",
                    LogType.Info);
            }
        });

        Context = new PipelineContext(
            partitions,
            new JobSessionFactory(log, job, tokenRefreshManager),
            readerConfig,
            writerConfig,
            EnableReplay: enableReplay,
            Cooldown: _cooldown,
            flags);

        _workerPool = new WorkerPool(_log, pipelineConfig.WorkerCount);
    }

    public void Start()
    {
        _log.WriteLine(
            $"Job pipeline: {_pipelineConfig.WorkerCount} shared workers " +
            $"(replay={Context.EnableReplay}, page size={Context.ReaderConfig.PageSize})",
            LogType.Info);
        _workerPool.Start(workerId => new DataCopyWorker(_log, _cts.Token, workerId).RunAsync(Context));
    }

    /// <summary>Completes the partition pool; workers will drain and exit.</summary>
    public void CompletePartitionChannel() => Context.Partitions.Complete();

    /// <summary>Waits for all workers to finish (offline mode only).</summary>
    public Task WaitForCompletionAsync() => _workerPool.WaitForCompletionAsync();

    /// <summary>True when every worker task has exited (faulted, cancelled, or returned).</summary>
    public bool AllWorkersExited => _workerPool.AllExited;

    /// <summary>Number of workers that completed with faults.</summary>
    public int FaultedWorkerCount => _workerPool.FaultedCount;

    /// <summary>
    /// Pipeline-wide cancellation token. Linked to the external
    /// (job-level) token, and additionally tripped by:
    /// (a) <see cref="MigrationJobContext.PauseRequested"/>, and
    /// (b) <see cref="JobControlFlags.TripFatal"/>.
    /// Coordinators link their own CTS to this token so fatal trips
    /// cascade into per-table drain waits; without this hop, the
    /// fatal callback only cancels this pipeline's CTS while a
    /// coordinator's sibling CTS (linked only to the external token)
    /// would hang forever on <c>BulkDrainSignal.Task.WaitAsync</c>.
    /// </summary>
    public CancellationToken Token => _cts.Token;

    public void Stop() => _cts.Cancel();

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        MigrationJobContext.Instance.PauseRequested -= _pauseHandler;

        // Stop the cooldown scheduler BEFORE we cancel/dispose so any
        // queued partitions are surfaced (logged) rather than vanishing
        // inside a torn-down task.
        await _cooldown.DisposeAsync().ConfigureAwait(false);

        // Dispose can race with the fatal-shutdown wiring above. Only
        // ObjectDisposedException is expected here (double-dispose);
        // anything else should surface.
        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException) { /* already disposed */ }
        MigrationUtilities.SafeDispose(_workerPool, "JobPipeline WorkerPool");
        _cts.Dispose();
    }
}
