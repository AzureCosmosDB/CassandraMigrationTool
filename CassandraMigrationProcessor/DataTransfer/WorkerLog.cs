using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Thin worker-scoped wrapper around <see cref="MigrationLog"/> that
/// always prepends the <c>[W{id}]</c> prefix. Lets workers, page
/// reader/writer, and strategies share a single logger reference
/// without each call site re-formatting the worker id into the message.
/// All writes delegate to the underlying <see cref="MigrationLog"/>,
/// which remains the single sink/lock owner for the migration.
/// </summary>
internal sealed class WorkerLog
{
    private readonly MigrationLog _inner;
    private readonly string _prefix;

    public WorkerLog(MigrationLog inner, int workerId)
    {
        _inner = inner;
        _prefix = $"[W{workerId}] ";
    }

    public void WriteLine(string message, LogType logType = LogType.Info)
        => _inner.WriteLine(_prefix + message, logType);

    /// <summary>
    /// Exposes the underlying log sink for code paths (e.g. UDT
    /// registration helpers, schema utilities) that take a
    /// <see cref="MigrationLog"/> directly and shouldn't be prefixed.
    /// </summary>
    public MigrationLog Inner => _inner;
}
