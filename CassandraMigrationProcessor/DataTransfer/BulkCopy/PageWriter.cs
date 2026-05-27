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
    private readonly MigrationLog _log;
    private readonly CancellationToken _ct;
    private readonly ISession _targetSession;
    private readonly IRowWriteStrategy _rowStrategy;
    private readonly int _workerId;
    private readonly int _pageSize;

    private PageWriter(MigrationLog log, ISession targetSession, IRowWriteStrategy rowStrategy,
        int pageSize, int workerId, CancellationToken cancellationToken)
    {
        _log = log;
        _ct = cancellationToken;
        _workerId = workerId;
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
    public static async Task<PageWriter> CreateAsync(MigrationLog log, WorkerConfig config, int pageSize, int workerId, int maxWriteRetries, CancellationToken cancellationToken)
    {
        var targetSession = CassandraClientFactory.CreateTargetSession(log, config.TargetConnection, "");
        var (ps, bindOrder) = await CassandraQueries.PrepareInsertAsync(
            targetSession, config.Context.TargetKeyspaceName, config.Context.TargetTableName, config.Columns);

        var sourceIndexByName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < config.Columns.Count; i++)
            sourceIndexByName[config.Columns[i].Name] = i;
        var bindOrderToSourceIndex = new int[bindOrder.Count];
        for (int i = 0; i < bindOrder.Count; i++)
            bindOrderToSourceIndex[i] = sourceIndexByName[bindOrder[i]];

        // Counter detection: Cassandra forbids mixing counter and
        // non-counter regular columns in the same table, so the presence
        // of any single counter column means every non-PK column is a
        // counter and we need the UPDATE-shaped path with read-modify-write.
        // Each strategy owns its own prep work; PageWriter only routes.
        var counterColumns = config.Columns
            .Where(c => string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase))
            .Select(c => c.Name)
            .ToList();
        bool isCounterTable = counterColumns.Count > 0;

        IRowWriteStrategy strategy = isCounterTable
            ? await CounterRowWriteStrategy.CreateAsync(log, targetSession, ps, bindOrderToSourceIndex,
                bindOrder, config.Context.TargetKeyspaceName, config.Context.TargetTableName,
                counterColumns, workerId, maxWriteRetries)
            : new RegularRowWriteStrategy(log, targetSession, ps, bindOrderToSourceIndex,
                workerId, maxWriteRetries);

        var writer = new PageWriter(log, targetSession, strategy, pageSize, workerId, cancellationToken);

        ISession? sourceSession = null;
        try
        {
            sourceSession = CassandraClientFactory.CreateSourceSession(log, config.SourceConnection, config.Context.KeyspaceName);
            var allUdts = await SchemaManager.GetUserDefinedTypesAsync(sourceSession, config.Context.KeyspaceName);
            var requiredUdts = SchemaManager.FilterUdtsReferencedByTable(
                allUdts, config.Columns.Select(c => c.Type));
            await DynamicUdtRegistrar.RegisterAsync(targetSession, config.Context.TargetKeyspaceName, requiredUdts);
        }
        catch (Exception ex)
        {
            log.WriteLine($"[W{workerId}] UDT mapping registration on target failed: {ex.Message}", LogType.Warning);
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

        for (int i = 0; i < rows.Count; i++)
        {
            if (_ct.IsCancellationRequested
                || Volatile.Read(ref ctx.Counters.FatalErrorFlag) != 0)
                break;

            writeTasks.Add(_rowStrategy.WriteRowAsync(rows[i], ctx, counters, i));
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
            _log.WriteLine($"[W{_workerId}] {counters.Failed}/{rows.Count} writes failed — checkpoint NOT advanced (will retry on resume)",
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
