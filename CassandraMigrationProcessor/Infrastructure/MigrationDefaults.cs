namespace CassandraMigrationProcessor.Infrastructure;

/// <summary>
/// Compile-time default tuning constants shared across the migration
/// pipeline (worker counts, retry limits, checkpoint cadence, page sizes).
/// </summary>
public static class MigrationDefaults
{
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

    // Schema/query read timeout (ms) applied via SetReadTimeoutMillis on
    // SimpleStatements that query system_schema / count rows.
    public const int SchemaQueryTimeoutMs = 30_000;

    // Default retry budget for the shared transient-fault retry helper
    // (RetryExecutor.ExecuteAsync). Caller-agnostic — schema, query, and
    // any other Cassandra-touching code paths can reuse these defaults
    // or override per call site.
    public const int TransientRetryMaxAttempts = 3;
    public const int TransientRetryBaseDelayMs = 2000;
}
