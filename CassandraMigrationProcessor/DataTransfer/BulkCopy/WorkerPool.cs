using CassandraMigrationProcessor.Infrastructure;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;
/// <summary>
/// Manages a pool of copy workers that process partitions
/// from the partition pool channel.
/// </summary>
internal class WorkerPool : IDisposable
{
    private readonly MigrationLog _log;
    private readonly int _workerCount;
    private Task[]? _workers;

    public WorkerPool(MigrationLog log, int workerCount)
    {
        _log = log;
        _workerCount = workerCount;
    }

    /// <summary>
    /// Starts all workers. Each worker creates its own sessions,
    /// takes partitions from the pool, reads pages, and writes rows.
    /// </summary>
    public void Start(Func<int, Task> workerFactory)
    {
        _workers = Enumerable.Range(0, _workerCount)
            .Select(id => Task.Run(() => workerFactory(id)))
            .ToArray();
    }

    /// <summary>
    /// Waits for all workers to complete. Swallows cancellation
    /// exceptions (workers exit gracefully on cancel).
    /// </summary>
    public async Task WaitForCompletionAsync()
    {
        if (_workers == null) return;
        try { await Task.WhenAll(_workers); }
        catch (OperationCanceledException) { }
    }

    /// <summary>
    /// Number of workers that completed with faults.
    /// </summary>
    public int FaultedCount => _workers?.Count(t => t.IsFaulted) ?? 0;

    public void Dispose()
    {
        // Workers are fire-and-forget tasks — nothing to dispose
        // Sessions are owned by PageReader/PageWriter inside workers
        _workers = null;
    }
}
