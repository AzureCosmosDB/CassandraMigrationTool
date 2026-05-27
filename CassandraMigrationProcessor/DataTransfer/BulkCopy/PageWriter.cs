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
/// tracking latency and errors. Abstract base; <see cref="CreateAsync"/>
/// returns the appropriate concrete writer for the target table shape:
/// <see cref="RegularPageWriter"/> for normal tables (INSERT-shaped path)
/// or <see cref="CounterPageWriter"/> for counter tables (read-modify-write
/// UPDATE-shaped path). The two paths differ in prepared statements,
/// per-row work (counter rows need a SELECT first), and consistency
/// level, so separating them keeps each implementation small and the
/// branchy "is this a counter row?" check out of the hot loop.
/// </summary>
internal abstract class PageWriter : IDisposable
{
    protected readonly MigrationLog _log;
    protected readonly CancellationToken _ct;
    protected readonly ISession _targetSession;
    protected readonly PreparedStatement _preparedInsert;
    protected readonly int[] _bindOrderToSourceIndex;
    protected readonly int _workerId;
    private readonly int _pageSize;
    protected readonly int _maxWriteRetries;

    protected const int WriteTimeoutMs = 60_000;
    protected const int RetryDelayMs = 500;

    protected PageWriter(MigrationLog log, ISession targetSession, PreparedStatement preparedInsert,
        int[] bindOrderToSourceIndex, int pageSize, int workerId, int maxWriteRetries,
        CancellationToken cancellationToken)
    {
        _log = log;
        _ct = cancellationToken;
        _workerId = workerId;
        _pageSize = pageSize;
        _maxWriteRetries = maxWriteRetries;
        _targetSession = targetSession;
        _preparedInsert = preparedInsert;
        _bindOrderToSourceIndex = bindOrderToSourceIndex;
    }

    /// <summary>
    /// Async factory. Creates the target session, prepares the
    /// insert/update statement, registers UDT mappings against the target,
    /// and returns the appropriate concrete <see cref="PageWriter"/> for
    /// the table shape.
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
        bool isCounterTable = config.Columns.Any(c =>
            string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase));

        PageWriter writer;
        if (isCounterTable)
        {
            // Bind order from PrepareInsertAsync for counters is
            // (counter cols ..., key cols ...), so we can compute the
            // split by counting the leading counter columns.
            var counterNames = new HashSet<string>(
                config.Columns
                    .Where(c => string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase))
                    .Select(c => c.Name),
                StringComparer.OrdinalIgnoreCase);
            int counterBindCount = 0;
            for (int i = 0; i < bindOrder.Count; i++)
            {
                if (counterNames.Contains(bindOrder[i])) counterBindCount++;
                else break;
            }

            var selectCounterCols = string.Join(", ",
                bindOrder.Take(counterBindCount).Select(n => $"\"{n}\""));
            var whereKeyCols = string.Join(" AND ",
                bindOrder.Skip(counterBindCount).Select(n => $"\"{n}\" = ?"));
            var selectCql =
                $"SELECT {selectCounterCols} " +
                $"FROM \"{config.Context.TargetKeyspaceName}\".\"{config.Context.TargetTableName}\" " +
                $"WHERE {whereKeyCols}";
            var targetSelectByPk = await targetSession.PrepareAsync(selectCql);

            writer = new CounterPageWriter(log, targetSession, ps, bindOrderToSourceIndex,
                pageSize, workerId, maxWriteRetries,
                counterBindCount, targetSelectByPk, cancellationToken);
        }
        else
        {
            writer = new RegularPageWriter(log, targetSession, ps, bindOrderToSourceIndex,
                pageSize, workerId, maxWriteRetries, cancellationToken);
        }

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

    protected static bool IsIdentityMap(int[] map)
    {
        for (int i = 0; i < map.Length; i++)
            if (map[i] != i) return false;
        return true;
    }

    protected class WriteCounters
    {
        public int Done;
        public int Failed;
        public long LatencySum;
    }

    /// <summary>
    /// Implements the per-row write strategy for this writer's table
    /// shape (plain INSERT vs. counter read-modify-write UPDATE).
    /// Implementations are responsible for their own retry loop and
    /// for updating <paramref name="counters"/> via
    /// <see cref="Interlocked"/>.
    /// </summary>
    protected abstract Task WriteRowAsync(object[] sourceRow, PipelineContext ctx, WriteCounters counters, int rowIndex);

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

            writeTasks.Add(WriteRowAsync(rows[i], ctx, counters, i));
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
