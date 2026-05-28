using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

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
    /// Worker recycle path. Recycling MUST succeed during normal
    /// operation; the only path that throws is when the pool was
    /// already completed (shutdown or fatal cascade), in which case
    /// the caller is about to exit on its next loop iteration anyway.
    /// Throwing surfaces the contract violation rather than silently
    /// losing the partition.
    /// </summary>
    public void Recycle(Partition partition)
    {
        if (!_channel.Writer.TryWrite(partition))
            throw new InvalidOperationException(
                "PartitionManager.Recycle: pool is completed; recycle is only valid while the pool is open.");
    }

    /// <summary>
    /// Worker pull. Blocks until a partition is available, the channel is
    /// completed, or the token fires. Returns null on completion / cancel.
    /// </summary>
    public async Task<Partition?> TakeAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _channel.Reader.WaitToReadAsync(cancellationToken))
            {
                if (_channel.Reader.TryRead(out var partition))
                    return partition;
            }
        }
        catch (OperationCanceledException) { }
        return null;
    }

    /// <summary>
    /// Signal no more partitions will be enqueued. Existing items remain
    /// readable until drained; subsequent <see cref="TakeAsync"/> calls
    /// return null once the channel is empty.
    /// </summary>
    public void Complete() => _channel.Writer.TryComplete();
}
