using System.Threading.Channels;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Owns the job-wide partition channel and is the only type that touches it.
/// Workers and the pipeline interact through the narrow API below so the
/// underlying transport (channel today, possibly a bounded queue or
/// priority structure later) stays an implementation detail.
/// <para>
/// The full set of partitions is determined at job init and supplied to
/// the ctor; there is no runtime seeding path. Treating the partition
/// set as immutable after construction removes the bug surface where a
/// late seed could race against pool completion.
/// </para>
/// <para>
/// Lifetime: created and disposed by <see cref="JobPipeline"/>.
/// </para>
/// </summary>
internal sealed class PartitionManager
{
    private readonly Channel<Partition> _channel;

    public PartitionManager(IReadOnlyList<Partition> initialPartitions)
    {
        ArgumentNullException.ThrowIfNull(initialPartitions);

        // Unbounded: total partitions = registered feed ranges across all
        // tables (already bounded by job scope). A bounded channel would
        // risk deadlock at construction if the initial set exceeds the
        // bound, since the ctor would have to block on writes before any
        // worker exists to drain.
        _channel = Channel.CreateUnbounded<Partition>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false,
        });

        foreach (var p in initialPartitions)
        {
            if (!_channel.Writer.TryWrite(p))
                throw new InvalidOperationException(
                    "PartitionManager: unbounded channel TryWrite failed at construction.");
        }
    }

    /// <summary>
    /// Recycle a partition back into the pool for re-pickup by any worker.
    /// Throws if the pool was already completed — recycling is only valid
    /// while the pool is open. Treating "pool closed" as a silent drop
    /// would lose the partition (its checkpoint may not yet cover the
    /// full feed range), so callers must observe the exception.
    /// </summary>
    public void Recycle(Partition partition)
    {
        if (!_channel.Writer.TryWrite(partition))
            throw new InvalidOperationException(
                "PartitionManager.Recycle: pool is completed; recycle is only valid while the pool is open.");
    }

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
