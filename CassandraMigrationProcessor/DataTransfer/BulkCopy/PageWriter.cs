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

    // Counter-table specific state. Counter columns are migrated using a
    // read-modify-write delta to make the operation idempotent under
    // retries and resume (counter UPDATEs are NOT idempotent: a "transient"
    // server timeout may have applied or not, and replaying produces
    // double-counts). Approach:
    //   1) SELECT current target counter values for this row's PK.
    //   2) Compute delta = origin - target for each counter column.
    //   3) UPDATE c = c + delta. Bind Cassandra.Unset.Value when origin is
    //      null (column was never incremented on source — Cassandra
    //      rejects null counter increments) or when delta is 0 (target
    //      already correct — skip the cell, no-op).
    //   4) Skip the row entirely when no counter has a non-zero delta.
    // This matches the approach used by Datastax/cassandra-data-migrator
    // (CopyJobSession.bind + TargetUpdateStatement.bind) and lets the
    // migration tolerate page-level retries and resume after partial
    // writes without producing double-counts.
    private readonly bool _isCounterTable;
    private readonly int _counterBindCount;
    private readonly PreparedStatement? _targetSelectByPk;

    private const int WriteTimeoutMs = 60_000;
    private const int RetryDelayMs = 500;

    private PageWriter(MigrationLog log, ISession targetSession, PreparedStatement preparedInsert,
        int[] bindOrderToSourceIndex, int pageSize, int workerId, int maxWriteRetries,
        bool isCounterTable, int counterBindCount, PreparedStatement? targetSelectByPk,
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
        _counterBindCount = counterBindCount;
        _targetSelectByPk = targetSelectByPk;
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
        // the UPDATE-shaped write path produced by PrepareInsertAsync. Bind
        // order for counter tables is (counter cols ..., key cols ...), so
        // we just need to know how many leading bind slots are counters.
        // We also prepare a SELECT on the target by PK so we can compute
        // origin-minus-target deltas per row (read-modify-write).
        bool isCounterTable = config.Columns.Any(c =>
            string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase));
        int counterBindCount = 0;
        PreparedStatement? targetSelectByPk = null;
        if (isCounterTable)
        {
            var counterNames = new HashSet<string>(
                config.Columns
                    .Where(c => string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase))
                    .Select(c => c.Name),
                StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < bindOrder.Count; i++)
            {
                if (counterNames.Contains(bindOrder[i])) counterBindCount++;
                else break; // counter cols always come first by PrepareInsertAsync contract
            }

            var selectCounterCols = string.Join(", ",
                bindOrder.Take(counterBindCount).Select(n => $"\"{n}\""));
            var whereKeyCols = string.Join(" AND ",
                bindOrder.Skip(counterBindCount).Select(n => $"\"{n}\" = ?"));
            var selectCql =
                $"SELECT {selectCounterCols} " +
                $"FROM \"{config.Context.TargetKeyspaceName}\".\"{config.Context.TargetTableName}\" " +
                $"WHERE {whereKeyCols}";
            targetSelectByPk = await targetSession.PrepareAsync(selectCql);
        }

        var writer = new PageWriter(log, targetSession, ps, bindOrderToSourceIndex, pageSize, workerId, maxWriteRetries,
            isCounterTable, counterBindCount, targetSelectByPk,
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

            if (_isCounterTable)
            {
                writeTasks.Add(WriteCounterRowAsync(sourceRow, ctx, counters, i));
            }
            else
            {
                var bound = BindRegularRow(sourceRow);
                bound.SetReadTimeoutMillis(WriteTimeoutMs);
                bound.SetConsistencyLevel(ConsistencyLevel.LocalOne);
                writeTasks.Add(WriteRowAsync(bound, ctx, counters, i));
            }
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
    /// Read-modify-write counter row migration. Counter UPDATEs in
    /// Cassandra are NOT idempotent: a transient timeout may have applied
    /// the increment server-side or not, and a naive retry produces
    /// double-counts. To get idempotency we first SELECT the current
    /// target counter values, then bind the per-column delta
    /// <c>(origin - target)</c> into <c>counter = counter + ?</c>. After
    /// the write the target equals the origin snapshot regardless of how
    /// many times we retry (or whether the previous attempt partially
    /// succeeded), because the delta is recomputed against the current
    /// state on every attempt.
    ///
    /// Cells are bound as <see cref="Cassandra.Unset"/>.Value when:
    /// (a) the origin counter is null — never incremented on source;
    ///     Cassandra rejects null counter increments, and we don't have
    ///     a safe way to "unset" a target counter so we leave it as-is;
    /// (b) the computed delta is 0 — target is already correct, skip the
    ///     cell entirely.
    /// When every counter column ends up unset (origin all-null OR all
    /// deltas zero) the row is skipped without issuing the UPDATE.
    /// </summary>
    private async Task WriteCounterRowAsync(object[] sourceRow, PipelineContext ctx, WriteCounters counters, int rowIndex)
    {
        for (int attempt = 1; attempt <= _maxWriteRetries; attempt++)
        {
            var rowStart = Stopwatch.GetTimestamp();
            try
            {
                // 1) SELECT current target counter values for this PK.
                var keyValues = new object[_bindOrderToSourceIndex.Length - _counterBindCount];
                for (int k = 0; k < keyValues.Length; k++)
                    keyValues[k] = sourceRow[_bindOrderToSourceIndex[_counterBindCount + k]];

                var selectBound = _targetSelectByPk!.Bind(keyValues);
                selectBound.SetReadTimeoutMillis(WriteTimeoutMs);
                selectBound.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                var rs = await _targetSession.ExecuteAsync(selectBound);
                Row? targetRow = null;
                foreach (var r in rs) { targetRow = r; break; }

                // 2) Compute deltas. Unset on null-origin or zero-delta.
                var bindValues = new object[_bindOrderToSourceIndex.Length];
                bool anyDelta = false;
                for (int b = 0; b < _counterBindCount; b++)
                {
                    var originRaw = sourceRow[_bindOrderToSourceIndex[b]];
                    if (originRaw == null)
                    {
                        bindValues[b] = Unset.Value;
                        continue;
                    }
                    long origin = Convert.ToInt64(originRaw);
                    long target = 0;
                    if (targetRow != null)
                    {
                        var t = targetRow.GetValue<long?>(b);
                        if (t.HasValue) target = t.Value;
                    }
                    long delta = origin - target;
                    if (delta == 0)
                    {
                        bindValues[b] = Unset.Value;
                        continue;
                    }
                    bindValues[b] = delta;
                    anyDelta = true;
                }
                for (int b = _counterBindCount; b < bindValues.Length; b++)
                    bindValues[b] = sourceRow[_bindOrderToSourceIndex[b]];

                if (!anyDelta)
                {
                    // Target is already correct (or origin had no counter
                    // increments to migrate). Skip the UPDATE.
                    long elapsedNoop = (Stopwatch.GetTimestamp() - rowStart) * 1000 / Stopwatch.Frequency;
                    Interlocked.Add(ref counters.LatencySum, elapsedNoop);
                    Interlocked.Increment(ref counters.Done);
                    return;
                }

                // 3) Apply UPDATE c = c + delta.
                var bound = _preparedInsert.Bind(bindValues);
                bound.SetReadTimeoutMillis(WriteTimeoutMs);
                bound.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                await _targetSession.ExecuteAsync(bound);

                long elapsed = (Stopwatch.GetTimestamp() - rowStart) * 1000 / Stopwatch.Frequency;
                Interlocked.Add(ref counters.LatencySum, elapsed);
                Interlocked.Increment(ref counters.Done);
                return;
            }
            catch (Exception ex)
            {
                if (ExceptionClassifier.IsFatal(ex))
                {
                    _log.WriteLine($"[W{_workerId}] FATAL counter row {rowIndex}: {ex.GetType().Name}: {ex.Message}",
                        LogType.Error);
                    Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                    Interlocked.Increment(ref counters.Failed);
                    return;
                }

                if (ExceptionClassifier.IsTransient(ex) && attempt < _maxWriteRetries)
                {
                    // The read-modify-write loop re-reads the target on
                    // every retry, so a partial apply from the previous
                    // attempt is reconciled by the next delta.
                    await Task.Delay(RetryDelayMs * attempt);
                    continue;
                }

                Interlocked.Increment(ref counters.Failed);
                _log.WriteLine($"[W{_workerId}] Counter row {rowIndex} FAILED after {attempt} attempt(s): {ex.GetType().Name}: {ex.Message}",
                    LogType.Error);

                if (!ExceptionClassifier.IsTransient(ex))
                {
                    Interlocked.Exchange(ref ctx.Counters.FatalErrorFlag, 1);
                }
                return;
            }
        }
    }
}
