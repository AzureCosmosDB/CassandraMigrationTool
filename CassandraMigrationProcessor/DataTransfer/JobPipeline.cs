using CassandraMigrationProcessor.CassandraDriver;
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
    private readonly JobControl _control;
    private readonly WorkerPool _workerPool;
    private readonly PartitionManager _partitions;
    public PipelineContext Context { get; }

    public JobPipeline(MigrationLog log, Job job, PipelineConfig pipelineConfig,
        JobPartitioning partitioning, SourceSessionWrapper sourceSession,
        ISessionFactory sessionFactory,
        JobControl control)
    {
        _log = log;
        _pipelineConfig = pipelineConfig;
        _control = control;

        bool enableReplay = job.IsOnline;
        _partitions = new PartitionManager(
            partitioning.AllPartitions,
            log,
            pipelineConfig.ChangeFeedPollIntervalMs,
            _control.Token,
            _control);
        var readerConfig = new ReaderConfig(pipelineConfig.PageSize, pipelineConfig.MaxReadRetries,
            pipelineConfig.PreserveCellTtlAndWritetime, pipelineConfig.UseJsonCopy);
        var writerConfig = new WriterConfig(
            pipelineConfig.MaxWriteRetries,
            pipelineConfig.TargetWriteConsistencyLevel,
            pipelineConfig.PreserveCellTtlAndWritetime,
            pipelineConfig.UseJsonCopy);

        Context = new PipelineContext(
            _partitions,
            sourceSession,
            sessionFactory,
            readerConfig,
            writerConfig,
            EnableReplay: enableReplay,
            control);

        _workerPool = new WorkerPool(_log, pipelineConfig.WorkerCount);
    }

    public void Start()
    {
        _log.WriteLine(
            $"Job pipeline: {_pipelineConfig.WorkerCount} shared workers " +
            $"(replay={Context.EnableReplay}, page size={Context.ReaderConfig.PageSize}, " +
            $"target write consistency={_pipelineConfig.TargetWriteConsistencyLevel})",
            LogType.Info);
        _workerPool.Start(workerId => new DataCopyWorker(_log, _control.Token, workerId).RunAsync(Context));
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
    /// Pipeline-wide cancellation token — the shared
    /// <see cref="JobControl.Token"/>. Any of pause / stop / cutover /
    /// fatal-fault trips this single token; no linked-CTS hop is
    /// needed for fatal-trip cascade because every worker, coordinator,
    /// and writer observes the same token directly.
    /// </summary>
    public CancellationToken Token => _control.Token;

    public void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public async ValueTask DisposeAsync()
    {
        // Dispose the partition manager BEFORE the worker pool so any
        // in-flight cooldown delays cancel cleanly. JobControl is owned
        // by JobManager — we never cancel or dispose it here.
        await _partitions.DisposeAsync().ConfigureAwait(false);
        MigrationUtilities.SafeDispose(_workerPool, "JobPipeline WorkerPool");
    }
}
