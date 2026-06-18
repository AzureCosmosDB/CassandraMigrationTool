using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Row-write strategy for counter target tables. Counter UPDATEs in
/// Cassandra are NOT idempotent, so retries use read-modify-write per
/// row:
/// <list type="number">
///   <item>SELECT current target counter values for this row's PK.</item>
///   <item>Compute delta = origin − target per counter column.</item>
///   <item>UPDATE c = c + delta, binding <see cref="Unset"/>.Value when
///     origin is null, or when delta is 0 <em>and</em> the target column
///     already exists (otherwise we must still emit <c>c = c + 0</c> so
///     a source counter whose value is exactly 0 materialises on the
///     target rather than reading back as <c>NULL</c>).</item>
///   <item>Skip the row when every counter ends up unset.</item>
/// </list>
/// Both SELECT and UPDATE run at LocalQuorum so the read sees the
/// latest target writes — required for retry correctness across
/// replicas.
/// </summary>
internal sealed class CounterRowWriteStrategy : IRowWriteStrategy
{
    private readonly WorkerLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsert;
    private readonly int[] _bindOrderToSourceIndex;
    private readonly RetryPolicy _retryPolicy;
    private readonly int _counterBindCount;
    private readonly PreparedStatement _targetSelectByPk;
    private readonly string _rowKind;

    private CounterRowWriteStrategy(WorkerLog log, ISession targetSession, PreparedStatement preparedInsert,
        int[] bindOrderToSourceIndex, RetryPolicy retryPolicy,
        int counterBindCount, PreparedStatement targetSelectByPk, string tableLabel)
    {
        _log = log;
        _targetSession = targetSession;
        _preparedInsert = preparedInsert;
        _bindOrderToSourceIndex = bindOrderToSourceIndex;
        _retryPolicy = retryPolicy;
        _counterBindCount = counterBindCount;
        _targetSelectByPk = targetSelectByPk;
        _rowKind = $"Counter row[{tableLabel}]";
    }

    /// <summary>
    /// Async factory. Prepares the counter UPDATE on the target via
    /// <see cref="CassandraQueries.PrepareCounterUpdateAsync"/>, builds
    /// the bind-order → source-index map, and prepares the SELECT-by-PK
    /// used for read-modify-write.
    /// </summary>
    public static async Task<CounterRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession,
        List<CassandraColumn> columns,
        string targetKeyspace, string targetTable, RetryPolicy retryPolicy)
    {
        var (ps, bindOrder) = await CassandraQueries.PrepareCounterUpdateAsync(
            targetSession, targetKeyspace, targetTable, columns);
        var bindOrderToSourceIndex = RowWriteStrategyFactory.BuildBindOrderToSourceIndex(bindOrder, columns);

        // PrepareCounterUpdateAsync emits counter columns first in
        // bindOrder, so the count of counter columns is the
        // counter-bind prefix length for RMW.
        int counterBindCount = columns.Count(c =>
            string.Equals(c.Type, "counter", StringComparison.OrdinalIgnoreCase));

        var selectCounterCols = string.Join(", ",
            bindOrder.Take(counterBindCount).Select(n => $"\"{n}\""));
        var whereKeyCols = string.Join(" AND ",
            bindOrder.Skip(counterBindCount).Select(n => $"\"{n}\" = ?"));
        var selectCql =
            $"SELECT {selectCounterCols} " +
            $"FROM \"{targetKeyspace}\".\"{targetTable}\" " +
            $"WHERE {whereKeyCols}";
        var targetSelectByPk = await targetSession.PrepareAsync(selectCql);

        return new CounterRowWriteStrategy(log, targetSession, ps, bindOrderToSourceIndex,
            retryPolicy, counterBindCount, targetSelectByPk,
            tableLabel: $"{targetKeyspace}.{targetTable}");
    }

    public Task<WriteOutcome> WriteRowAsync(
        object[] sourceRow,
        WriteCounters counters,
        CdcRowMetadata? metadata,
        CancellationToken cancellationToken)
    {
        // Counter UPDATEs reject USING TIMESTAMP / USING TTL — Cassandra
        // forbids both on counter mutations. The metadata is therefore
        // ignored here; counter tables fall back to the typed path
        // gated at the reader layer (see PageReader.ShouldUseJsonForTable).
        return RowWriteRetry.ExecuteAsync(
            attempt: () => ReadModifyWriteAsync(sourceRow, cancellationToken),
            policy: _retryPolicy,
            log: _log, rowKind: _rowKind,
            counters: counters,
            cancellationToken: cancellationToken);
    }

    public Task<WriteOutcome> WriteJsonRowAsync(
        string cleanedJson,
        WriteCounters counters,
        CdcRowMetadata? metadata,
        CancellationToken cancellationToken)
    {
        // Defensive: counter tables should never be routed through the
        // JSON path (the reader excludes them upfront), and INSERT JSON
        // cannot apply +delta semantics. Surface a hard error so a
        // future plumbing bug is caught immediately rather than
        // producing a wrong destination value.
        throw new NotSupportedException(
            "Counter tables cannot be written via INSERT JSON; the reader " +
            "must route counter pages through the typed object[] path.");
    }

    private async Task ReadModifyWriteAsync(object[] sourceRow, CancellationToken cancellationToken)
    {
        // 1) SELECT current target counter values for this PK.
        var keyValues = new object[_bindOrderToSourceIndex.Length - _counterBindCount];
        for (int k = 0; k < keyValues.Length; k++)
            keyValues[k] = sourceRow[_bindOrderToSourceIndex[_counterBindCount + k]];

        var selectBound = _targetSelectByPk.Bind(keyValues);
        selectBound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        selectBound.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        var rs = await _targetSession.ExecuteAsync(selectBound).WaitAsync(cancellationToken);
        Row? targetRow = null;
        foreach (var r in rs) { targetRow = r; break; }

        // 2) Compute deltas. Unset on null-origin. When delta is 0 we
        //    only skip the UPDATE if the target column already exists
        //    (otherwise the column never gets materialised and reads
        //    back as NULL — a source counter of exactly 0 must still
        //    emit `c = c + 0`).
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
            bool targetExists = false;
            if (targetRow != null)
            {
                var t = targetRow.GetValue<long?>(b);
                if (t.HasValue)
                {
                    target = t.Value;
                    targetExists = true;
                }
            }
            long delta = origin - target;
            if (delta == 0 && targetExists)
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
            // Target already correct — skip the UPDATE.
            return;
        }

        // 3) Apply UPDATE c = c + delta.
        var bound = _preparedInsert.Bind(bindValues);
        bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        bound.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        await _targetSession.ExecuteAsync(bound).WaitAsync(cancellationToken);
    }
}
