using System.Threading.Channels;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Owns the job-wide partition channel. The full set of partitions is
/// fixed at construction; there is no runtime seeding path. Recycle
/// after a configurable cooldown is implemented per-partition with
/// <see cref="Task.Delay(int, CancellationToken)"/> — there is no
/// shared scheduler loop. Lifetime is owned by <see cref="JobPipeline"/>.
/// </summary>
internal sealed class PartitionManager : IAsyncDisposable
{
    private readonly Channel<Partition> _channel;
    private readonly MigrationLog _log;
    private readonly int _cooldownMs;
    private readonly CancellationTokenSource _cooldownStopCts;
    // Capture the cooldown token once at ctor time. CancellationToken
    // is a struct that remains safe to pass to Task.Delay even after
    // the source is disposed; reading _cooldownStopCts.Token post-
    // dispose throws ObjectDisposedException synchronously, which a
    // fire-and-forget RecycleAfterDelayAsync would silently swallow
    // and drop the partition. Issue surfaced in partitioning review.
    private readonly CancellationToken _cooldownCt;
    private readonly JobControl? _control;

    public PartitionManager(
        IReadOnlyList<Partition> initialPartitions,
        MigrationLog log,
        int cooldownMs,
        CancellationToken cancellationToken,
        JobControl? control = null)
    {
        ArgumentNullException.ThrowIfNull(initialPartitions);
        ArgumentNullException.ThrowIfNull(log);

        _log = log;
        _cooldownMs = cooldownMs;
        _control = control;

        // Unbounded: total partitions are bounded by job scope. A
        // bounded channel would risk ctor deadlock since the writes
        // happen before any worker exists to drain.
        _channel = Channel.CreateUnbounded<Partition>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false,
        });

        foreach (var p in initialPartitions)
            _channel.Writer.TryWrite(p);

        // Link the caller's cancellation with ours so DisposeAsync is
        // self-sufficient: in-flight per-partition cooldown Task.Delay
        // calls cancel together when disposal cancels this CTS.
        _cooldownStopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _cooldownCt = _cooldownStopCts.Token;
    }

    /// <summary>
    /// Recycle a partition back into the pool. Throws if the pool was
    /// already completed — silent drop would lose a partition whose
    /// checkpoint may not yet cover the full feed range.
    /// </summary>
    public void Recycle(Partition partition)
    {
        if (!_channel.Writer.TryWrite(partition))
            throw new InvalidOperationException(
                "PartitionManager.Recycle: pool is completed; recycle is only valid while the pool is open.");
    }

    /// <summary>
    /// Recycle a partition after the configured cooldown interval.
    /// Each recycle owns its own <see cref="Task.Delay(int, CancellationToken)"/>;
    /// disposal cancels the shared CTS so in-flight delays exit
    /// promptly. Returns immediately; never blocks the caller.
    /// </summary>
    public void RecycleAfterCooldown(Partition partition)
    {
        _ = RecycleAfterDelayAsync(partition);
    }

    private async Task RecycleAfterDelayAsync(Partition partition)
    {
        try
        {
            await Task.Delay(_cooldownMs, _cooldownCt).ConfigureAwait(false);
            Recycle(partition);
        }
        catch (OperationCanceledException)
        {
            // Disposal in flight — partition stays on its persisted
            // checkpoint and resumes from there on the next run.
            _log.WriteLine(
                $"PartitionManager: cooldown for {partition.Table.FullTableName}/{partition.FeedRange} cancelled at shutdown; will resume from checkpoint.",
                LogType.Info);
        }
        catch (InvalidOperationException ex)
        {
            // Pool completed under us — partition's checkpoint is
            // persisted; resume picks it up. Logged at Error to match
            // pre-PR DrainReadyCooldowns severity so operator
            // dashboards alerting on Error level still surface this.
            _log.WriteLine(
                $"PartitionManager: pool closed while recycling {partition.Table.FullTableName}/{partition.FeedRange}: {ex.Message}",
                LogType.Error);
        }
        catch (Exception ex)
        {
            // Any other fault from the cooldown path must escalate.
            // The pre-PR CooldownLoopTask.ContinueWith observer would
            // have surfaced this as MigrationFatalException via
            // IControl.ReportFault; without this catch the fire-and-
            // forget pattern would silently swallow the fault and the
            // partition would be lost without any operator signal.
            _log.WriteLine(
                $"PartitionManager: unexpected fault recycling {partition.Table.FullTableName}/{partition.FeedRange}: {ex.GetType().Name}: {ex.Message}",
                LogType.Error);
            _control?.ReportFault(new MigrationFatalException(
                $"PartitionManager cooldown faulted for {partition.Table.FullTableName}/{partition.FeedRange}",
                ex));
        }
    }

    /// <summary>
    /// Worker pull. Blocks until a partition is available or the
    /// channel completes. Returns null only when the channel has been
    /// completed AND drained. Throws OperationCanceledException on
    /// cancel so callers can distinguish "job finished normally" from
    /// "I was asked to stop".
    /// </summary>
    public async Task<Partition?> TakeAsync(CancellationToken cancellationToken)
    {
        while (await _channel.Reader.WaitToReadAsync(cancellationToken))
        {
            if (_channel.Reader.TryRead(out var partition))
                return partition;
        }
        return null;
    }

    /// <summary>
    /// Signal no more partitions will be enqueued. Existing items remain
    /// readable until drained; subsequent <see cref="TakeAsync"/> calls
    /// return null once the channel is empty.
    /// </summary>
    public void Complete() => _channel.Writer.TryComplete();

    public ValueTask DisposeAsync()
    {
        try { _cooldownStopCts.Cancel(); }
        catch (ObjectDisposedException) { /* already disposed */ }
        _channel.Writer.TryComplete();
        _cooldownStopCts.Dispose();
        return ValueTask.CompletedTask;
    }
}
