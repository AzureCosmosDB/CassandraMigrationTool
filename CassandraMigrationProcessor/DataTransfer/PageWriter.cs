using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Tunables for <see cref="PageWriter"/>: per-row write retry budget.
/// Page size is reader-only; writers don't page.
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

    /// <summary>
    /// Last per-row exception observed when a page write finishes
    /// with rows still un-written (counterpart to
    /// <see cref="PageReader.LastRetryExhaustionException"/>). Workers
    /// attach this as the inner of a job-wide
    /// <see cref="MigrationFatalException"/> so the underlying driver
    /// error is preserved in the fault chain.
    /// </summary>
    internal Exception? LastWriteException { get; private set; }

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
        return _strategyCache.GetOrAdd(partition.Table.FullTableName, async _ =>
        {
            await EnsureTargetUdtsRegisteredAsync(partition);
            return await RowWriteStrategyFactory.CreateAsync(
                _log, _targetSession, partition.Table.Columns,
                partition.Table.Spec.TargetKeyspaceName, partition.Table.Spec.TargetTableName,
                _maxWriteRetries,
                partition.Table.IsCounterTable);
        });
    }

    private Task EnsureTargetUdtsRegisteredAsync(Partition partition)
    {
        // Simulated run: target session has no UDT registration surface.
        if (_targetSession is NullSession)
            return Task.CompletedTask;

        return _udtRegistrations.GetOrAdd(partition.Table.Spec.TargetKeyspaceName, async ks =>
        {
            ISession? sourceSession = null;
            try
            {
                sourceSession = _sessionFactory.CreateSourceSession();
                var allUdts = await SchemaManager.GetUserDefinedTypesAsync(sourceSession, partition.Table.Spec.KeyspaceName);
                var requiredUdts = SchemaManager.FilterUdtsReferencedByTable(
                    allUdts, partition.Table.Columns.Select(c => c.Type));
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
        var writeTasks = new List<Task<WriteOutcome>>(rows.Count);

        bool aborted = false;
        for (int i = 0; i < rows.Count; i++)
        {
            if (_ct.IsCancellationRequested
                || ctx.Control.IsFatal)
            {
                aborted = true;
                break;
            }

            writeTasks.Add(strategy.WriteRowAsync(rows[i], counters, _ct));
        }

        var outcomes = await Task.WhenAll(writeTasks);
        LastWriteException = counters.LastException;
        if (Array.IndexOf(outcomes, WriteOutcome.Fatal) >= 0)
            ctx.Control.ReportFault(new MigrationFatalException(
                $"Target write reported fatal for table {partition.Table.FullTableName}.",
                counters.LastException));

        // Only advance checkpoint if EVERY row in this page was successfully
        // written. Gating on counters.Failed == 0 was unsafe: an early break
        // (cancellation or fatal flag tripped mid-loop) leaves un-enqueued
        // rows that never fail and never succeed — Failed stays 0 but Done
        // < rows.Count, and marking IsCompleted=true would advance the
        // continuation token past unwritten rows = silent data loss on
        // resume.
        if (!aborted && counters.Done == rows.Count && counters.Failed == 0)
        {
            workChunk.IsCompleted = true;
        }
        else
        {
            int unwritten = rows.Count - (int)counters.Done;
            _log.WriteLine(
                $"{counters.Failed} failed / {unwritten - counters.Failed} unattempted / {rows.Count} total writes for {partition.Table.FullTableName} — checkpoint NOT advanced (will retry on resume)",
                LogType.Warning);
        }

        stopwatch.Stop();
        partition.Table.Tracker.AddWriteTime(counters.LatencySum);

        if (partition.Phase == PartitionPhase.Replay)
        {
            partition.Table.Tracker.AddReplayApplied(counters.Done, counters.LatencySum);
            if (counters.Failed > 0)
                partition.Table.Tracker.AddReplayErrors(counters.Failed);
        }
        else
        {
            partition.Table.Tracker.AddCopied(counters.Done);
            partition.Table.Tracker.AddFailed(counters.Failed);
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
        partition.Table.Tracker.AddBytes(pageBytes);
    }
}
