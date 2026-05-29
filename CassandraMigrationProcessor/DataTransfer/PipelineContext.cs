using System.Collections.Concurrent;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Job-level control flags shared by every worker: a fatal-error
/// latch, a hook that cancels the job-wide CTS when fatal is tripped,
/// and the collected per-worker outcomes. Per-table progress
/// counters live on <see cref="TableResources.Tracker"/>.
/// </summary>
internal class JobControlFlags
{
    public int FatalErrorFlag;
    public ConcurrentBag<TaskResult> WorkerErrors { get; } = new();

    /// <summary>
    /// Wired by <see cref="JobPipeline"/> at construction so workers
    /// can cancel the job-wide CTS when raising a fatal error. Set
    /// once via the constructor — never mutated after — so there is
    /// no half-built window where a fatal trip silently no-ops.
    /// </summary>
    private readonly Action _triggerFatalShutdown;

    public JobControlFlags(Action triggerFatalShutdown)
    {
        ArgumentNullException.ThrowIfNull(triggerFatalShutdown);
        _triggerFatalShutdown = triggerFatalShutdown;
    }

    /// <summary>
    /// Idempotent fatal trip: sets the latch and cancels the job CTS.
    /// Safe to call from any worker / strategy.
    /// </summary>
    public void TripFatal()
    {
        Interlocked.Exchange(ref FatalErrorFlag, 1);
        // The shutdown callback is owned by JobPipeline and only throws
        // ObjectDisposedException during teardown races. Anything else
        // is a real bug and we want it visible, but TripFatal must remain
        // safe to call from arbitrary worker paths, so we surface it via
        // the console rather than re-raising into the caller (which may
        // itself be in a catch block reacting to the original fault).
        try
        {
            _triggerFatalShutdown();
        }
        catch (ObjectDisposedException) { /* shutdown already torn down */ }
        catch (Exception ex)
        {
            Console.Error.WriteLine(
                $"TripFatal: shutdown callback threw {ex.GetType().Name}: {ex.Message}");
        }
    }
}

/// <summary>
/// Shared (job-wide) state passed to every worker. Holds the
/// <see cref="DataTransfer.PartitionManager"/> that all tables seed into and
/// every worker pulls from, the connection capability used by readers and
/// writers, reader / writer tunables, the replay configuration knobs, and
/// global control flags. Per-table state is resolved through
/// <see cref="Partition"/> pass-through accessors.
/// </summary>
internal record PipelineContext(
    PartitionManager Partitions,
    ISessionFactory SessionFactory,
    ReaderConfig ReaderConfig,
    WriterConfig WriterConfig,
    bool EnableReplay,
    CooldownScheduler Cooldown,
    JobControlFlags Flags);
