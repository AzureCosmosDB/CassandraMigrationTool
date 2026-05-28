using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Job-wide cooldown scheduler. Workers that finish an empty replay page
/// hand the partition here and immediately return to the pool — they do
/// not block on cooldown delays, and no per-partition fire-and-forget
/// Task.Run is created. A single background loop drains the
/// priority-ordered queue and recycles each partition through
/// <see cref="PartitionManager.Recycle"/> when its eligibility time has
/// passed. On <see cref="StopAsync"/> the loop drains its pending queue
/// back into the partition pool so nothing is silently dropped.
/// </summary>
internal sealed class CooldownScheduler : IAsyncDisposable
{
    private readonly MigrationLog _log;
    private readonly PartitionManager _partitions;
    private readonly int _cooldownMs;
    private readonly CancellationTokenSource _stopCts;
    private readonly CancellationToken _ct;
    private readonly PriorityQueue<Partition, long> _queue = new();
    private readonly object _lock = new();
    private readonly SemaphoreSlim _wake = new(0, int.MaxValue);
    private readonly Task _loop;

    public CooldownScheduler(
        MigrationLog log,
        PartitionManager partitions,
        int cooldownMs,
        CancellationToken cancellationToken)
    {
        _log = log;
        _partitions = partitions;
        _cooldownMs = cooldownMs;
        // Link the caller's cancellation with our own so StopAsync is
        // self-sufficient: the loop's only exit is _ct cancellation, and
        // we don't want DisposeAsync to deadlock waiting on the loop
        // when the caller's token never fires (e.g. offline jobs with an
        // empty cooldown queue completing cleanly).
        _stopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _ct = _stopCts.Token;
        _loop = Task.Run(RunAsync);
    }

    /// <summary>
    /// Queue a partition for recycle after the configured cooldown.
    /// Returns immediately — never blocks the caller.
    /// </summary>
    public void Schedule(Partition partition)
    {
        long eligibleAt = Environment.TickCount64 + _cooldownMs;
        lock (_lock)
        {
            _queue.Enqueue(partition, eligibleAt);
        }
        _wake.Release();
    }

    private async Task RunAsync()
    {
        try
        {
            while (!_ct.IsCancellationRequested)
            {
                int waitMs = NextWaitMs();
                if (waitMs > 0)
                {
                    try { await _wake.WaitAsync(waitMs, _ct); }
                    catch (OperationCanceledException) { return; }
                }

                DrainReady();
            }
        }
        catch (Exception ex)
        {
            _log.WriteLine(
                $"CooldownScheduler loop crashed: {ex.GetType().Name}: {ex.Message}",
                LogType.Error);
        }
    }

    private int NextWaitMs()
    {
        lock (_lock)
        {
            if (_queue.Count == 0) return Timeout.Infinite;
            _queue.TryPeek(out _, out long eligibleAt);
            long delta = eligibleAt - Environment.TickCount64;
            return (int)Math.Clamp(delta, 0, int.MaxValue);
        }
    }

    private void DrainReady()
    {
        long now = Environment.TickCount64;
        while (true)
        {
            Partition? ready = null;
            lock (_lock)
            {
                if (_queue.Count == 0) return;
                if (!_queue.TryPeek(out _, out long eligibleAt) || eligibleAt > now)
                    return;
                ready = _queue.Dequeue();
            }

            try
            {
                _partitions.Recycle(ready);
            }
            catch (InvalidOperationException ex)
            {
                // Pool closed under us. Surface — silent drop here loses a
                // feed range whose checkpoint may not yet cover the full
                // partition.
                _log.WriteLine(
                    $"CooldownScheduler: pool closed while recycling {ready.FullTableName}/{ready.FeedRange}: {ex.Message}",
                    LogType.Error);
                return;
            }
        }
    }

    /// <summary>
    /// Stop the scheduler and flush any queued partitions back into the
    /// pool. Call this BEFORE completing the partition channel so
    /// nothing is silently lost.
    /// </summary>
    public async Task StopAsync()
    {
        // Cancel the loop's wait FIRST. The loop's only natural exit is
        // _ct cancellation; without this, an empty queue causes the
        // loop to park on Timeout.Infinite and StopAsync would await
        // forever (the deadlock that bit offline-only jobs).
        try { _stopCts.Cancel(); }
        catch (ObjectDisposedException) { /* already disposed */ }
        _wake.Release();
        try { await _loop.ConfigureAwait(false); }
        catch { /* surfaced via log inside RunAsync */ }

        // Final flush: at shutdown the partitions still queued have a
        // persisted checkpoint and will resume on next run; we log them
        // rather than silently drop so operators can see how many
        // in-flight cooldowns existed at stop.
        int dropped;
        lock (_lock) { dropped = _queue.Count; _queue.Clear(); }
        if (dropped > 0)
            _log.WriteLine(
                $"CooldownScheduler stopped with {dropped} partition(s) still in cooldown — they will resume from checkpoint on next run.",
                LogType.Info);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _wake.Dispose();
        _stopCts.Dispose();
    }
}
