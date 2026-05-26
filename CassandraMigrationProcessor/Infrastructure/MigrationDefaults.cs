namespace CassandraMigrationProcessor.Infrastructure;
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
}
