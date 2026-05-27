using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Writes extracted rows to the target Cassandra cluster concurrently,
/// tracking latency and errors. The per-row write strategy is delegated
/// to an <see cref="IRowWriteStrategy"/> chosen at construction time
/// (regular INSERT vs. counter read-modify-write UPDATE), so this class
/// only owns the shared orchestration: row fan-out, byte accounting,
/// tracker updates, and session disposal.
/// </summary>
internal sealed class PageWriter : IDisposable
{
    private readonly WorkerLog _log;
    private readonly CancellationToken _ct;
    private readonly ISession _targetSession;
    private readonly IRowWriteStrategy _rowStrategy;
    private readonly int _pageSize;

    private PageWriter(WorkerLog log, ISession targetSession, IRowWriteStrategy rowStrategy,
        int pageSize, CancellationToken cancellationToken)
    {
        _log = log;
        _ct = cancellationToken;
        _pageSize = pageSize;
        _targetSession = targetSession;
        _rowStrategy = rowStrategy;
    }

    /// <summary>
    /// Async factory. Creates the target session, prepares the
    /// insert/update statement, registers UDT mappings against the target,
    /// and selects the right <see cref="IRowWriteStrategy"/> for the
    /// table shape.
    /// </summary>
    public static async Task<PageWriter> CreateAsync(WorkerLog log, WorkerConfig config, int pageSize, int maxWriteRetries, CancellationToken cancellationToken)
    {
        var targetSession = CassandraClientFactory.CreateTargetSession(log.Inner, config.TargetConnection, "");
        var strategy = await RowWriteStrategyFactory.CreateAsync(
            log, targetSession, config.Columns,
            config.Context.TargetKeyspaceName, config.Context.TargetTableName,
            maxWriteRetries);
        var writer = new PageWriter(log, targetSession, strategy, pageSize, cancellationToken);

        ISession? sourceSession = null;
        try
        {
            sourceSession = CassandraClientFactory.CreateSourceSession(log.Inner, config.SourceConnection, config.Context.KeyspaceName);
            var allUdts = await SchemaManager.GetUserDefinedTypesAsync(sourceSession, config.Context.KeyspaceName);
            var requiredUdts = SchemaManager.FilterUdtsReferencedByTable(
                allUdts, config.Columns.Select(c => c.Type));
            await DynamicUdtRegistrar.RegisterAsync(targetSession, config.Context.TargetKeyspaceName, requiredUdts);
        }
        catch (Exception ex)
        {
            log.WriteLine($"UDT mapping registration on target failed: {ex.Message}", LogType.Warning);
        }
        finally
        {
            if (sourceSession != null)
                MigrationUtilities.SafeDispose(sourceSession, "PageWriter UDT discovery session");
        }
        return writer;
    }

    public void Dispose() => MigrationUtilities.SafeDispose(_targetSession, "PageWriter target session");

    /// <summary>
    /// Writes extracted rows to the target cluster in parallel,
    /// tracking progress and handling errors.
    /// </summary>
    public async Task WriteAsync(List<object[]> rows,
        WorkChunk workChunk,
        PipelineContext ctx)
    {
        if (rows.Count == 0)
        {
            workChunk.IsCompleted = true;
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        var counters = new WriteCounters();
        var writeTasks = new List<Task>(rows.Count);
        Action onFatal = () => Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);

        for (int i = 0; i < rows.Count; i++)
        {
            if (_ct.IsCancellationRequested
                || Volatile.Read(ref ctx.Counters.FatalErrorFlag) != 0)
                break;

            writeTasks.Add(_rowStrategy.WriteRowAsync(rows[i], onFatal, counters, i));
        }

        ctx.Tracker.SetPipelineState(ctx.Ranges.FeedRanges.Count
                - ctx.Ranges.Completed.Count,
            _pageSize);
        await Task.WhenAll(writeTasks);

        // Only mark chunk completed if ALL rows succeeded.
        // Failed rows mean this page must be retried on resume.
        if (counters.Failed == 0) workChunk.IsCompleted = true;
        else
        {
            _log.WriteLine($"{counters.Failed}/{rows.Count} writes failed — checkpoint NOT advanced (will retry on resume)",
                LogType.Warning);
        }

        stopwatch.Stop();
        ctx.Tracker.AddWriteTime(counters.LatencySum, rows.Count);
        ctx.Tracker.AddCopied(counters.Done);
        ctx.Tracker.AddFailed(counters.Failed);

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
        ctx.Tracker.AddBytes(pageBytes);
    }
}
