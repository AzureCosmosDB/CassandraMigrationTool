namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Compile-time default tuning constants shared across the migration
/// pipeline (worker counts, retry limits, checkpoint cadence, page sizes).
/// </summary>
public static class MigrationDefaults
{
    public const int WorkerMultiplier = 8;
    public const int MinWorkers = 4;
    public const int MaxConsecutiveAuthErrors = 3;
    public const int MaxTableRetries = 3;
    public const double ProgressCapPercent = 99.9;
    public const int CheckpointIntervalSeconds = 10;
    public const int MaxReconnectAttempts = 50;

    // Per-page read retry count when the source SELECT fails
    // transiently. Used by PageReader.
    public const int DefaultMaxReadRetries = 3;

    // Per-row write retry count for transient target failures.
    public const int DefaultMaxWriteRetries = 5;

    // Schema/query read timeout (ms) applied to system_schema /
    // COUNT(*) statements.
    public const int SchemaQueryTimeoutMs = 30_000;

    public const int PartitionDiscoveryParallelism = 8;

    // Extended fallback read timeout (ms) for the row-count COUNT(*)
    // when the initial attempt exceeds SchemaQueryTimeoutMs.
    public const int RowCountFallbackTimeoutMs = 120_000;

    // Default retry budget for the shared transient-fault helper
    // (RetryExecutor.ExecuteAsync).
    public const int TransientRetryMaxAttempts = 3;
    public const int TransientRetryBaseDelayMs = 2000;
}
