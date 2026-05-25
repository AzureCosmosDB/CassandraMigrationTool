using Cassandra;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;

/// <summary>
/// Writes extracted rows to the target Cassandra cluster
/// concurrently, tracking latency and errors.
/// </summary>
internal class PageWriter : IDisposable
{
    private readonly MigrationLog _log;
    private readonly CancellationToken _ct;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsert;
    private readonly int[] _bindOrderToSourceIndex;
    private readonly int _workerId;
    private readonly int _pageSize;
    private readonly int _maxWriteRetries;

    // Counter-table specific state. _isCounterTable is true when the target
    // table has any column of CQL type "counter". Counter columns are
    // null-skippable per-row: a null counter cell on the source means that
    // column was never incremented and binding it into "counter = counter
    // + ?" is illegal (server returns "Invalid null value for counter
    // increment"). For each row we determine the subset of non-null
    // counters and issue an UPDATE that touches only those columns.
    private readonly bool _isCounterTable;
    private readonly List<CounterColumn> _counterCols = new();
    private readonly List<KeyColumn> _keyCols = new();
    private readonly string _targetKeyspace;
    private readonly string _targetTable;
    private readonly ConcurrentDictionary<long, PreparedStatement> _counterStatementCache = new();

    private readonly record struct CounterColumn(string Name, int SourceIndex);
    private readonly record struct KeyColumn(string Name, int SourceIndex);

    private const int WriteTimeoutMs = 60_000;
    private const int RetryDelayMs = 500;

    private PageWriter(MigrationLog log, ISession targetSession, PreparedStatement preparedInsert,
        int[] bindOrderToSourceIndex, int pageSize, int workerId, int maxWriteRetries,
        bool isCounterTable,
        List<CounterColumn> counterCols, List<KeyColumn> keyCols,
        string targetKeyspace, string targetTable,
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
        _isCounterTable = isCounterTable;
        _counterCols = counterCols;
        _keyCols = keyCols;
        _targetKeyspace = targetKeyspace;
        _targetTable = targetTable;
    }

    /// <summary>
    /// Async factory. Creates the target session, prepares the insert
    /// statement, and registers dynamic UDT mappings against the target
    /// using the source keyspace's UDT definitions — those are the shapes
    /// the reader produces and what the target needs to be able to bind.
    /// Source and target UDTs are identical because
    /// <see cref="SchemaManager.SyncSchemaAsync"/> replicated them.
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

        // Counter-table detection: any column of CQL type "counter" forces
        // the UPDATE-shaped write path. Pre-compute counter and key column
        // metadata so we can rebuild a per-row UPDATE that omits any
        // counter column whose source cell is null (Cassandra rejects null
        // counter increments).
        bool isCounterTable = config.Columns.Any(c =>
            string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase));
        var counterCols = new List<CounterColumn>();
        var keyCols = new List<KeyColumn>();
        if (isCounterTable)
        {
            foreach (var c in config.Columns)
            {
                if (string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase))
                    counterCols.Add(new CounterColumn(c.Name, sourceIndexByName[c.Name]));
            }
            // Key columns in PK order (partition first, then clustering by position).
            foreach (var c in config.Columns
                .Where(c => c.Kind == "partition_key" || c.Kind == "clustering")
                .OrderBy(c => c.Kind == "partition_key" ? 0 : 1)
                .ThenBy(c => c.Position))
            {
                keyCols.Add(new KeyColumn(c.Name, sourceIndexByName[c.Name]));
            }
        }

        var writer = new PageWriter(log, targetSession, ps, bindOrderToSourceIndex, pageSize, workerId, maxWriteRetries,
            isCounterTable, counterCols, keyCols,
            config.Context.TargetKeyspaceName, config.Context.TargetTableName,
            cancellationToken);

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

    private static bool IsIdentityMap(int[] map)
    {
        for (int i = 0; i < map.Length; i++)
            if (map[i] != i) return false;
        return true;
    }

    private class WriteCounters
    {
        public int Done;
        public int Failed;
        public long LatencySum;
    }

    private async Task WriteRowAsync(BoundStatement bound, PipelineContext ctx, WriteCounters counters, int rowIndex)
    {
        for (int attempt = 1; attempt <= _maxWriteRetries; attempt++)
        {
            var writeStart = Stopwatch.GetTimestamp();
            try
            {
                await _targetSession.ExecuteAsync(bound);
                long elapsed = (Stopwatch.GetTimestamp() - writeStart) * 1000 / Stopwatch.Frequency;
                Interlocked.Add(ref counters.LatencySum, elapsed);
                Interlocked.Increment(ref counters.Done);
                return; // success
            }
            catch (Exception ex)
            {
                if (ExceptionClassifier.IsFatal(ex))
                {
                    _log.WriteLine($"[W{_workerId}] FATAL row {rowIndex}: {ex.GetType().Name}: {ex.Message}",
                        LogType.Error);
                    Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                    Interlocked.Increment(ref counters.Failed);
                    return;
                }

                if (ExceptionClassifier.IsTransient(ex) && attempt < _maxWriteRetries)
                {
                    await Task.Delay(RetryDelayMs * attempt);
                    continue; // retry
                }

                // Non-transient or final retry exhausted
                Interlocked.Increment(ref counters.Failed);
                _log.WriteLine($"[W{_workerId}] Row {rowIndex} FAILED after {attempt} attempt(s): {ex.GetType().Name}: {ex.Message}",
                    LogType.Error);

                if (!ExceptionClassifier.IsTransient(ex))
                {
                    Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                }
                return;
            }
        }
    }

    /// <summary>
    /// Writes extracted rows to the target cluster in
    /// parallel, tracking progress and handling errors.
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

            var sourceRow = rows[i];

            BoundStatement? bound;
            if (_isCounterTable)
            {
                bound = await BindCounterRowAsync(sourceRow, i, counters);
                if (bound == null) continue; // row skipped (all counters null)
            }
            else
            {
                bound = BindRegularRow(sourceRow);
            }
            bound.SetReadTimeoutMillis(WriteTimeoutMs);
            bound.SetConsistencyLevel(ConsistencyLevel.LocalOne);

            writeTasks.Add(WriteRowAsync(bound, ctx, counters, i));
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

    private BoundStatement BindRegularRow(object[] sourceRow)
    {
        object[] bindValues;
        if (_bindOrderToSourceIndex.Length == sourceRow.Length
            && IsIdentityMap(_bindOrderToSourceIndex))
        {
            bindValues = sourceRow;
        }
        else
        {
            bindValues = new object[_bindOrderToSourceIndex.Length];
            for (int b = 0; b < _bindOrderToSourceIndex.Length; b++)
                bindValues[b] = sourceRow[_bindOrderToSourceIndex[b]];
        }
        return _preparedInsert.Bind(bindValues);
    }

    /// <summary>
    /// Per-row counter binding. Counter cells that are null on the source
    /// (a counter column that was never incremented for this row) cannot
    /// be re-bound as <c>counter = counter + null</c> — Cassandra rejects
    /// that with <c>InvalidQueryException: Invalid null value for counter
    /// increment</c>. We build (and cache) a per-mask prepared statement
    /// that includes only the counter columns whose values are non-null
    /// for this row.
    ///
    /// Returns <c>null</c> when every counter column on the row is null,
    /// in which case there is no counter data to migrate (no Cassandra
    /// counter row would exist on the source without at least one
    /// increment). The skipped row counts toward the per-page success
    /// total so checkpointing advances normally.
    /// </summary>
    private async Task<BoundStatement?> BindCounterRowAsync(object[] sourceRow, int rowIndex, WriteCounters counters)
    {
        long mask = 0;
        for (int j = 0; j < _counterCols.Count; j++)
        {
            if (sourceRow[_counterCols[j].SourceIndex] != null)
                mask |= 1L << j;
        }

        if (mask == 0)
        {
            // No non-null counters — nothing to migrate for this row.
            Interlocked.Increment(ref counters.Done);
            return null;
        }

        var ps = await GetOrPrepareCounterStatementAsync(mask);
        int counterCount = System.Numerics.BitOperations.PopCount((ulong)mask);
        var bindValues = new object[counterCount + _keyCols.Count];
        int b = 0;
        for (int j = 0; j < _counterCols.Count; j++)
        {
            if ((mask & (1L << j)) != 0)
                bindValues[b++] = sourceRow[_counterCols[j].SourceIndex];
        }
        for (int j = 0; j < _keyCols.Count; j++)
            bindValues[b++] = sourceRow[_keyCols[j].SourceIndex];

        return ps.Bind(bindValues);
    }

    private async Task<PreparedStatement> GetOrPrepareCounterStatementAsync(long mask)
    {
        if (_counterStatementCache.TryGetValue(mask, out var cached))
            return cached;

        var setParts = new List<string>();
        for (int j = 0; j < _counterCols.Count; j++)
        {
            if ((mask & (1L << j)) != 0)
                setParts.Add($"\"{_counterCols[j].Name}\" = \"{_counterCols[j].Name}\" + ?");
        }
        var whereParts = _keyCols.Select(k => $"\"{k.Name}\" = ?");
        var cql = $"UPDATE \"{_targetKeyspace}\".\"{_targetTable}\" "
                  + $"SET {string.Join(", ", setParts)} "
                  + $"WHERE {string.Join(" AND ", whereParts)}";

        var prepared = await _targetSession.PrepareAsync(cql);
        // GetOrAdd handles the (unlikely) concurrent-prepare race: only
        // one cached statement per mask survives.
        return _counterStatementCache.GetOrAdd(mask, prepared);
    }
}
