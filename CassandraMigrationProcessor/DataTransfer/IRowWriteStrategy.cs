namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Per-row write strategy used by <see cref="PageWriter"/> and the
/// change-feed replay loop. Implementations own the bind-and-execute
/// logic for one source row, including retry behaviour and the
/// consistency level appropriate for the target table shape (plain
/// INSERT vs. counter read-modify-write UPDATE).
/// <para>
/// Callers handle row fan-out, byte accounting, and tracker updates;
/// the strategy handles "how do I correctly write this one row?".
/// Implementations are stateless across calls and must use
/// <see cref="System.Threading.Interlocked"/> when updating the shared
/// <see cref="WriteCounters"/>.
/// </para>
/// </summary>
internal interface IRowWriteStrategy
{
    Task<WriteOutcome> WriteRowAsync(object[] sourceRow, WriteCounters counters, CancellationToken cancellationToken);
}

/// <summary>
/// Per-page accumulator updated atomically by strategy implementations.
/// </summary>
internal sealed class WriteCounters
{
    public int Done;
    public int Failed;
    public long LatencySum;

    // Last per-row exception observed by RowWriteRetry after retries
    // were exhausted (Failed or Fatal outcome). Surfaced so that when
    // the worker promotes a page-level write-retry exhaustion to a
    // job-wide fatal, the underlying driver exception can ride along
    // as the MigrationFatalException inner — without this, only the
    // per-worker log retained the original error detail.
    public Exception? LastException;
}
