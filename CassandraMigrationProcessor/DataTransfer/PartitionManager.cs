using System.Threading.Channels;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Owns the job-wide partition channel and is the only type that touches it.
/// Seeders, workers, and the pipeline interact through the narrow API below
/// so the underlying transport (channel today, possibly a bounded queue or
/// priority structure later) stays an implementation detail.
/// <para>
/// Lifetime: created and disposed by <see cref="JobPipeline"/>.
/// </para>
/// </summary>
internal sealed class PartitionManager
{
    private readonly Channel<Partition> _channel;

    public PartitionManager()
    {
        // Unbounded: total partitions = registered feed ranges across all
        // tables (already bounded by job scope). A bounded channel would
        // risk deadlock — seeder and workers both writing into a full pool
        // with no slack — or silent drops if worker recycle used TryWrite
        // on a full channel.
        _channel = Channel.CreateUnbounded<Partition>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false,
        });
    }

    /// <summary>
    /// Initial seed at table start. Awaitable so a future bounded transport
    /// can apply backpressure to the seeder without changing callers.
    /// </summary>
    public ValueTask SeedAsync(Partition partition, CancellationToken cancellationToken = default)
        => _channel.Writer.WriteAsync(partition, cancellationToken);

    /// <summary>
    /// Worker recycle path used by the synchronous in-flight handoff.
    /// Recycling MUST succeed during normal operation; the only path
    /// that throws is when the pool was already completed (shutdown
    /// or fatal cascade), in which case the caller is about to exit
    /// on its next loop iteration anyway. Throwing surfaces the
    /// contract violation rather than silently losing the partition.
    /// </summary>
    public void Recycle(Partition partition)
    {
        if (!_channel.Writer.TryWrite(partition))
            throw new InvalidOperationException(
                "PartitionManager.Recycle: pool is completed; recycle is only valid while the pool is open.");
    }

    /// <summary>
    /// Best-effort recycle for deferred paths (e.g. replay cooldown
    /// callbacks) that legitimately race with pool completion at
    /// shutdown. Returns false instead of throwing when the channel
    /// is closed so the caller does not need an empty catch around
    /// an expected condition. False is only ever returned when the
    /// channel writer has been completed — for any other failure
    /// the underlying exception still propagates.
    /// </summary>
    public bool TryRecycle(Partition partition)
        => _channel.Writer.TryWrite(partition);

    /// <summary>
    /// Worker pull. Blocks until a partition is available or the channel is
    /// completed. Returns null only when the channel has been completed AND
    /// drained. Throws OperationCanceledException on cancel so the caller can
    /// distinguish "job finished normally" (null) from "I was asked to stop"
    /// (OCE) — otherwise a cancel mid-job is silently mistaken for completion.
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
}
