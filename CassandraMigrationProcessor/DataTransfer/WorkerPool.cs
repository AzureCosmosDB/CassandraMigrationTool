using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer;
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
        catch (OperationCanceledException)
        {
            _log.WriteLine("Workers cancelled — graceful shutdown");
        }
        catch
        {
            // await Task.WhenAll only re-throws the FIRST faulted task's
            // inner exception, not AggregateException. Inspect each task
            // directly to surface every worker fault.
        }
        finally
        {
            foreach (var t in _workers.Where(t => t.IsFaulted && t.Exception != null))
            {
                foreach (var inner in t.Exception!.Flatten().InnerExceptions.Where(inner => inner is not OperationCanceledException))
                {
                    _log.WriteLine(
                        $"Worker faulted: {inner.GetType().FullName}: {inner.Message}",
                        LogType.Error);
                    if (inner.StackTrace != null)
                        _log.WriteLine($"  at {inner.StackTrace}", LogType.Error);
                    if (inner.InnerException != null)
                        _log.WriteLine(
                            $"  Inner: {inner.InnerException.GetType().FullName}: {inner.InnerException.Message}",
                            LogType.Error);
                }
            }
        }
    }

    /// <summary>
    /// Number of workers that completed with faults.
    /// </summary>
    public int FaultedCount => _workers?.Count(t => t.IsFaulted) ?? 0;

    /// <summary>True if every worker task has finished (success, fault, or cancel).</summary>
    public bool AllExited => _workers != null && _workers.All(t => t.IsCompleted);

    public void Dispose()
    {
        // Workers are fire-and-forget tasks — nothing to dispose
        // Sessions are owned by PageReader/PageWriter inside workers
        _workers = null;
    }
}
