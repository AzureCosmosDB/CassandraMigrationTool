using System;
using System.Threading;
using System.Threading.Tasks;

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
    Task WriteRowAsync(object[] sourceRow, Action onFatal, WriteCounters counters, int rowIndex, CancellationToken cancellationToken);
}

/// <summary>
/// Per-page accumulator updated atomically by strategy implementations.
/// </summary>
internal sealed class WriteCounters
{
    public int Done;
    public int Failed;
    public long LatencySum;
}
