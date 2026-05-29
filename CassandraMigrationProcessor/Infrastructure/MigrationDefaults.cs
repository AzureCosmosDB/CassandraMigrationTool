namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Compile-time default tuning constants shared across the migration
/// pipeline (worker counts, retry limits, checkpoint cadence, page sizes).
/// </summary>
public static class MigrationDefaults
{
    // Heuristic multiplier used to size the shared worker pool when
    // Job.WorkerCount is not set: WorkerCount = max(MinWorkers,
    // ProcessorCount * WorkerMultiplier). Cassandra workers spend most
    // of their time awaiting network I/O (paged reads and writes), so
    // heavy oversubscription per CPU core sustains throughput. Tuned
    // empirically; override via Job.WorkerCount on small or shared hosts.
    public const int WorkerMultiplier = 13;
    public const int MinWorkers = 4;
    public const int MaxConsecutiveAuthErrors = 3;
    public const int MaxTableRetries = 3;
    public const double ProgressCapPercent = 99.9;
    public const int CheckpointIntervalSeconds = 10;
    public const int MaxReconnectAttempts = 50;

    // Per-page read retry count when the source SELECT fails transiently
    // (overloaded, timeout, no host available). Used by PageReader.
    public const int DefaultMaxReadRetries = 3;

    // Per-row write retry count when the target write fails transiently.
    // Used by PageWriter.
    public const int DefaultMaxWriteRetries = 5;
}
