using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Tunable knobs for a single <see cref="PageWriter"/>: the page size
/// reported into <see cref="CopyProgressTracker.SetPipelineState"/> and
/// the per-row write retry budget handed to the row write strategies.
/// Carried as a record so the caller passes one capability instead of
/// two loose ints.
/// </summary>
/// <summary>
/// Tunables for <see cref="PageWriter"/>: write retry budget. Page size
/// is reader-only; writers don't page.
/// </summary>
internal record WriterConfig(int MaxWriteRetries);

/// <summary>
/// Writes extracted rows to the target cluster. The target session is
/// keyspace-agnostic; per-table prepared statements and write strategies
/// are lazily built and cached per table on first encounter. A single
/// worker can therefore service partitions from any table without
/// rebuilding state per page.
/// </summary>
internal sealed class PageWriter : IDisposable
{
    private readonly WorkerLog _log;
    private readonly CancellationToken _ct;
    private readonly ISession _targetSession;
    private readonly int _maxWriteRetries;
    private readonly ISessionFactory _sessionFactory;

    private readonly ConcurrentDictionary<string, Task<IRowWriteStrategy>> _strategyCache = new();
    private readonly ConcurrentDictionary<string, Task> _udtRegistrations = new();

    private PageWriter(WorkerLog log, ISessionFactory sessionFactory, ISession targetSession,
        WriterConfig config, CancellationToken cancellationToken)
    {
        _log = log;
        _ct = cancellationToken;
        _sessionFactory = sessionFactory;
        _maxWriteRetries = config.MaxWriteRetries;
        _targetSession = targetSession;
    }

    public static async Task<PageWriter> CreateAsync(WorkerLog log, ISessionFactory sessionFactory, WriterConfig config, CancellationToken cancellationToken)
    {
        var targetSession = await sessionFactory.CreateTargetSessionAsync();
        return new PageWriter(log, sessionFactory, targetSession, config, cancellationToken);
    }

    public void Dispose() => MigrationUtilities.SafeDisposeSession(_targetSession, "PageWriter target session");

    private Task<IRowWriteStrategy> GetStrategyAsync(Partition partition)
    {
        return _strategyCache.GetOrAdd(partition.FullTableName, async _ =>
        {
            await EnsureTargetUdtsRegisteredAsync(partition);
            return await RowWriteStrategyFactory.CreateAsync(
                _log, _targetSession, partition.Columns,
                partition.Spec.TargetKeyspaceName, partition.Spec.TargetTableName,
                _maxWriteRetries,
                partition.IsCounterTable);
        });
    }

    private Task EnsureTargetUdtsRegisteredAsync(Partition partition)
    {
        return _udtRegistrations.GetOrAdd(partition.Spec.TargetKeyspaceName, async ks =>
        {
            ISession? sourceSession = null;
            try
            {
                sourceSession = _sessionFactory.CreateSourceSession();
                var allUdts = await SchemaManager.GetUserDefinedTypesAsync(sourceSession, partition.Spec.KeyspaceName);
                var requiredUdts = SchemaManager.FilterUdtsReferencedByTable(
                    allUdts, partition.Columns.Select(c => c.Type));
                await DynamicUdtRegistrar.RegisterAsync(_targetSession, ks, requiredUdts);
            }
            catch (Exception ex)
            {
                // Do NOT swallow: missing UDT mapping on the target makes
                // the driver serialize fields incorrectly. Fail fast so
                // the row never reaches the wire mis-shaped.
                _log.WriteLine($"FATAL: UDT mapping registration on target failed for {ks}: {ex.Message}", LogType.Error);
                throw;
            }
            finally
            {
                if (sourceSession != null)
                    MigrationUtilities.SafeDisposeSession(sourceSession, "PageWriter UDT discovery session");
            }
        });
    }

    public async Task WriteAsync(List<object[]> rows,
        WorkChunk workChunk,
        Partition partition,
        PipelineContext ctx)
    {
        if (rows.Count == 0)
        {
            workChunk.IsCompleted = true;
            return;
        }

        var strategy = await GetStrategyAsync(partition);

        var stopwatch = Stopwatch.StartNew();
        var counters = new WriteCounters();
        var writeTasks = new List<Task>(rows.Count);
        Action onFatal = () => ctx.Flags.TripFatal();

        for (int i = 0; i < rows.Count; i++)
        {
            if (_ct.IsCancellationRequested
                || Volatile.Read(ref ctx.Flags.FatalErrorFlag) != 0)
                break;

            writeTasks.Add(strategy.WriteRowAsync(rows[i], onFatal, counters, i, _ct));
        }

        await Task.WhenAll(writeTasks);

        if (counters.Failed == 0) workChunk.IsCompleted = true;
        else
        {
            _log.WriteLine($"{counters.Failed}/{rows.Count} writes failed for {partition.FullTableName} — checkpoint NOT advanced (will retry on resume)",
                LogType.Warning);
        }

        stopwatch.Stop();
        partition.Tracker.AddWriteTime(counters.LatencySum);

        if (partition.Phase == PartitionPhase.Replay)
        {
            partition.Tracker.AddReplayApplied(counters.Done, counters.LatencySum);
            if (counters.Failed > 0)
                partition.Tracker.AddReplayErrors(counters.Failed);
        }
        else
        {
            partition.Tracker.AddCopied(counters.Done);
            partition.Tracker.AddFailed(counters.Failed);
        }

        long pageBytes = 0;
        foreach (var r in rows)
            foreach (var v in r)
            {
                if (v is byte[] b)
                    pageBytes += b.Length;
                else if (v is string s)
                    pageBytes += s.Length * 2;
                else if (v != null)
                    pageBytes += 8;
            }
        partition.Tracker.AddBytes(pageBytes);
    }
}
