using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Row-write strategy for non-counter (regular) target tables. Writes
/// each row via a prepared
/// <c>INSERT INTO … JSON ? USING TIMESTAMP ? AND TTL ?</c> statement
/// bound from a cleaned <c>SELECT JSON *</c> envelope. The destination
/// server handles all type marshalling, so this path transparently
/// supports every CQL type the source table contains (UDT, tuple,
/// decimal, varint, duration, nested collections) without any
/// per-type code on the migrator.
/// <para>
/// When <see cref="CdcRowMetadata"/> is supplied the strategy binds
/// the source writetime + remaining TTL; otherwise it binds wall-clock
/// micros + <c>0</c> (no TTL), which is semantically identical to
/// omitting the clause.
/// </para>
/// <para>
/// Cell-level preservation: when enabled and the source row carries
/// per-column writetime/TTL that diverges from the row-level values
/// (<see cref="CdcRowMetadata.HasPerColumnDivergence"/>), the row is
/// written as one baseline <c>INSERT … JSON ? DEFAULT UNSET</c> carrying
/// the source row-liveness metadata, followed by timestamped/TTL-bearing
/// <c>UPDATE</c>s for divergent cells. UPDATE preserves the baseline
/// row-liveness marker while still applying each cell's own metadata.
/// Uniform rows with a source marker keep the single-statement fast path;
/// UPDATE-only rows are replayed without an INSERT so marker absence is
/// preserved.
/// </para>
/// </summary>
internal sealed class RegularRowWriteStrategy : IRowWriteStrategy
{
    private readonly WorkerLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsertJson;

    // Non-null only when cell-level preservation is enabled: the DEFAULT
    // UNSET baseline insert, ordered primary-key metadata, and lazy cache
    // of one JSON UPDATE statement per divergent column.
    private readonly PreparedStatement? _preparedInsertJsonPartial;
    private readonly IReadOnlyList<CassandraColumn>? _primaryKeyColumns;
    private readonly HashSet<string>? _primaryKeyColumnNames;
    private readonly IReadOnlyDictionary<string, CassandraColumn>? _columnsByName;
    private readonly ConcurrentDictionary<string, Lazy<Task<PreparedStatement>>>
        _preparedColumnUpdates = new(StringComparer.Ordinal);
    private readonly string _targetKeyspace;
    private readonly string _targetTable;

    private readonly RetryPolicy _retryPolicy;
    private readonly ConsistencyLevel _writeConsistencyLevel;
    private readonly string _rowKind;

    private RegularRowWriteStrategy(WorkerLog log, ISession targetSession,
        PreparedStatement preparedInsertJson,
        PreparedStatement? preparedInsertJsonPartial,
        IReadOnlyList<CassandraColumn>? primaryKeyColumns,
        IReadOnlyDictionary<string, CassandraColumn>? columnsByName,
        RetryPolicy retryPolicy,
        ConsistencyLevel writeConsistencyLevel,
        string targetKeyspace,
        string targetTable)
    {
        _log = log;
        _targetSession = targetSession;
        _preparedInsertJson = preparedInsertJson;
        _preparedInsertJsonPartial = preparedInsertJsonPartial;
        _primaryKeyColumns = primaryKeyColumns;
        _columnsByName = columnsByName;
        _primaryKeyColumnNames = primaryKeyColumns is null
            ? null
            : new HashSet<string>(
                primaryKeyColumns.Select(c => c.Name), StringComparer.Ordinal);
        _retryPolicy = retryPolicy;
        _writeConsistencyLevel = writeConsistencyLevel;
        _targetKeyspace = targetKeyspace;
        _targetTable = targetTable;
        _rowKind = $"JSON row[{targetKeyspace}.{targetTable}]";
    }

    /// <summary>
    /// Async factory. Prepares the <c>INSERT … JSON ?</c> statement
    /// against the target (and, when <paramref name="preserveCellLevel"/>
    /// is set, the DEFAULT UNSET partial variant used for per-cell
    /// splitting). The factory shape mirrors
    /// <see cref="CounterRowWriteStrategy.CreateAsync"/> so
    /// <see cref="RowWriteStrategyFactory"/> can dispatch generically.
    /// </summary>
    public static async Task<RegularRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession,
        List<CassandraColumn> columns,
        string targetKeyspace, string targetTable, RetryPolicy retryPolicy,
        ConsistencyLevel writeConsistencyLevel,
        bool preserveCellLevel)
    {
        var psJson = await CassandraQueries.PrepareInsertJsonAsync(
            targetSession, targetKeyspace, targetTable, columns);

        PreparedStatement? psPartial = null;
        IReadOnlyList<CassandraColumn>? pkColumns = null;
        IReadOnlyDictionary<string, CassandraColumn>? columnsByName = null;
        if (preserveCellLevel)
        {
            // Prepared eagerly so a target that cannot honour DEFAULT UNSET
            // fails fast and visibly at strategy creation rather than
            // silently degrading cell-level fidelity mid-migration.
            psPartial = await CassandraQueries.PrepareInsertJsonPartialAsync(
                targetSession, targetKeyspace, targetTable, columns);
            pkColumns = columns
                .Where(c => c.Kind is "partition_key" or "clustering")
                .OrderBy(c => c.Kind == "partition_key" ? 0 : 1)
                .ThenBy(c => c.Position)
                .ToArray();
            columnsByName = columns.ToDictionary(c => c.Name, StringComparer.Ordinal);
        }

        return new RegularRowWriteStrategy(
            log, targetSession, psJson, psPartial, pkColumns, columnsByName, retryPolicy,
            writeConsistencyLevel, targetKeyspace, targetTable);
    }

    public Task<WriteOutcome> WriteRowAsync(
        object[] sourceRow,
        WriteCounters counters,
        CdcRowMetadata? metadata,
        CancellationToken cancellationToken)
    {
        // Regular tables are read via SELECT JSON * and written via
        // WriteJsonRowAsync. The typed binary path is reserved for
        // counter tables (CounterRowWriteStrategy).
        throw new NotSupportedException(
            "Regular tables must be written via WriteJsonRowAsync; " +
            "the typed object[] path is reserved for counter tables.");
    }

    public Task<WriteOutcome> WriteJsonRowAsync(
        string cleanedJson,
        WriteCounters counters,
        CdcRowMetadata? metadata,
        CancellationToken cancellationToken)
    {
        // Cell-level split path: rows with divergent cells need independent
        // UPDATEs, and UPDATE-only source rows must avoid creating a marker
        // through the normal INSERT path.
        if (_preparedInsertJsonPartial is not null
            && metadata is not null
            && (metadata.HasPerColumnDivergence || !metadata.HasRowMarker))
        {
            return WriteSplitRowAsync(cleanedJson, metadata, counters, cancellationToken);
        }

        int ttlSeconds = ResolveTtlSeconds(metadata);
        long ts = metadata?.WritetimeMicros ?? NowMicros();
        // Bind layout: (envelope, writetime, ttl).
        var bound = _preparedInsertJson.Bind(cleanedJson, ts, ttlSeconds);
        bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        bound.SetConsistencyLevel(_writeConsistencyLevel);

        return RowWriteRetry.ExecuteAsync(
            attempt: () => _targetSession.ExecuteAsync(bound).WaitAsync(cancellationToken),
            policy: _retryPolicy,
            log: _log, rowKind: _rowKind,
            counters: counters,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Restore the source row-liveness marker with one baseline partial
    /// INSERT, then replay each divergent cell with UPDATE so newer
    /// TTL-bearing cells cannot replace that marker. UPDATE-only source rows
    /// skip the baseline INSERT and replay every value through UPDATE so no
    /// destination marker is introduced. All statements share one retry
    /// budget and count as a single row.
    /// </summary>
    private async Task<WriteOutcome> WriteSplitRowAsync(
        string cleanedJson,
        CdcRowMetadata metadata,
        WriteCounters counters,
        CancellationToken cancellationToken)
    {
        var plan = BuildSplitWritePlan(
            cleanedJson,
            metadata,
            _primaryKeyColumns!,
            _primaryKeyColumnNames!,
            _columnsByName!);
        var attempts = new List<Func<Task>>(
            plan.CellWrites.Count + (plan.WriteBaseline ? 1 : 0));
        long fallbackTs = NowMicros();

        var preparedCellWrites = new List<(CellWrite Cell, PreparedStatement Prepared)>(
            plan.CellWrites.Count);
        foreach (var cellWrite in plan.CellWrites)
        {
            preparedCellWrites.Add((
                cellWrite,
                await GetPreparedColumnUpdateAsync(
                    cellWrite.Column,
                    cancellationToken)));
        }

        long nowEpochSeconds = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        if (plan.WriteBaseline)
        {
            int baselineTtlSeconds =
                ResolveTtlSecondsFromExpiry(
                    metadata.ExpiryEpochSeconds, nowEpochSeconds);
            var baseline = _preparedInsertJsonPartial!.Bind(
                plan.BaselineJson,
                metadata.WritetimeMicros!.Value,
                baselineTtlSeconds);
            baseline.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
            baseline.SetConsistencyLevel(_writeConsistencyLevel);
            attempts.Add(() =>
                _targetSession.ExecuteAsync(baseline).WaitAsync(cancellationToken));
        }

        foreach (var preparedCellWrite in preparedCellWrites)
        {
            var cellWrite = preparedCellWrite.Cell;
            int ttlSeconds =
                ResolveTtlSecondsFromExpiry(
                    cellWrite.Key.Expiry, nowEpochSeconds);
            long ts = cellWrite.Key.Writetime ?? fallbackTs;
            var keyValues = cellWrite.Column.Kind == "static"
                ? plan.PartitionKeyValues
                : plan.PrimaryKeyValues;
            var bindValues = new object[3 + keyValues.Count];
            bindValues[0] = ts;
            bindValues[1] = ttlSeconds;
            bindValues[2] = cellWrite.JsonValue;
            for (int i = 0; i < keyValues.Count; i++)
                bindValues[i + 3] = keyValues[i];

            var bound = preparedCellWrite.Prepared.Bind(bindValues);
            bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
            bound.SetConsistencyLevel(_writeConsistencyLevel);
            attempts.Add(() => _targetSession.ExecuteAsync(bound).WaitAsync(cancellationToken));
        }

        return await RowWriteRetry.ExecuteRowGroupsAsync(
            attempts, _retryPolicy, _log, _rowKind, counters, cancellationToken);
    }

    internal readonly record struct GroupKey(long? Writetime, long? Expiry);

    internal readonly record struct CellWrite(
        GroupKey Key, CassandraColumn Column, string JsonValue);

    internal sealed record SplitWritePlan(
        string BaselineJson,
        bool WriteBaseline,
        IReadOnlyList<string> PrimaryKeyValues,
        IReadOnlyList<string> PartitionKeyValues,
        IReadOnlyList<CellWrite> CellWrites);

    /// <summary>
    /// Parse the cleaned envelope into a baseline INSERT payload and the
    /// divergent cells that must be replayed with UPDATE.
    /// </summary>
    internal static SplitWritePlan BuildSplitWritePlan(
        string cleanedJson,
        CdcRowMetadata metadata,
        IReadOnlyList<CassandraColumn> primaryKeyColumns,
        IReadOnlySet<string> primaryKeyColumnNames,
        IReadOnlyDictionary<string, CassandraColumn> columnsByName)
    {
        using var doc = JsonDocument.Parse(cleanedJson);
        var root = doc.RootElement;

        var pkByName = new Dictionary<string, JsonProperty>(StringComparer.Ordinal);
        var baselineProps = new List<JsonProperty>();
        var cellWrites = new List<CellWrite>();
        var perColumn = metadata.PerColumn
            ?? new Dictionary<string, CdcCellMetadata>(StringComparer.Ordinal);
        bool writeBaseline = metadata.HasRowMarker;

        foreach (var prop in root.EnumerateObject())
        {
            string columnName = CqlIdentifier.Unquote(prop.Name);
            if (primaryKeyColumnNames.Contains(columnName))
            {
                pkByName[columnName] = prop;
                continue;
            }

            bool hasCellMetadata = perColumn.TryGetValue(columnName, out var cell);
            if (hasCellMetadata || !writeBaseline)
            {
                if (!columnsByName.TryGetValue(columnName, out var column))
                    throw new JsonException(
                        $"Row payload contains unknown column '{prop.Name}'.");
                if (!hasCellMetadata)
                {
                    cell = new CdcCellMetadata(
                        metadata.WritetimeMicros,
                        metadata.ExpiryEpochSeconds);
                }
                cellWrites.Add(new CellWrite(
                    new GroupKey(cell.WritetimeMicros, cell.ExpiryEpochSeconds),
                    column,
                    prop.Value.GetRawText()));
            }
            else
            {
                baselineProps.Add(prop);
            }
        }

        var pkProps = new List<JsonProperty>(primaryKeyColumns.Count);
        var pkValues = new List<string>(primaryKeyColumns.Count);
        var partitionKeyValues = new List<string>();
        foreach (var keyColumn in primaryKeyColumns)
        {
            if (!pkByName.TryGetValue(keyColumn.Name, out var prop))
                throw new JsonException(
                    $"Row payload is missing primary-key column '{keyColumn.Name}'.");
            pkProps.Add(prop);
            pkValues.Add(prop.Value.GetRawText());
            if (keyColumn.Kind == "partition_key")
                partitionKeyValues.Add(prop.Value.GetRawText());
        }

        return new SplitWritePlan(
            WriteSubsetJson(pkProps, baselineProps),
            writeBaseline,
            pkValues,
            partitionKeyValues,
            cellWrites);
    }

    private async Task<PreparedStatement> GetPreparedColumnUpdateAsync(
        CassandraColumn column,
        CancellationToken cancellationToken)
    {
        var lazy = _preparedColumnUpdates.GetOrAdd(
            column.Name,
            _ => new Lazy<Task<PreparedStatement>>(
                () => PrepareColumnUpdateWithRetryAsync(column),
                LazyThreadSafetyMode.ExecutionAndPublication));
        var preparationTask = lazy.Value;
        try
        {
            return await preparationTask.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            _preparedColumnUpdates.TryRemove(
                new KeyValuePair<string, Lazy<Task<PreparedStatement>>>(
                    column.Name, lazy));
            throw;
        }
    }

    private async Task<PreparedStatement> PrepareColumnUpdateWithRetryAsync(
        CassandraColumn column)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await CassandraQueries.PrepareUpdateJsonColumnAsync(
                    _targetSession,
                    _targetKeyspace,
                    _targetTable,
                    column,
                    _primaryKeyColumns!);
            }
            catch (Exception ex) when (
                attempt < _retryPolicy.MaxAttempts
                && ExceptionClassifier.IsTransient(ex))
            {
                await Task.Delay(_retryPolicy.DelayBeforeRetry(attempt));
            }
        }
    }

    /// <summary>
    /// Emit a JSON object containing the primary-key columns followed by
    /// the supplied data columns, copying each value verbatim from the
    /// source document.
    /// </summary>
    private static string WriteSubsetJson(
        List<JsonProperty> pkProps, List<JsonProperty> dataProps)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer,
            new JsonWriterOptions { SkipValidation = true }))
        {
            writer.WriteStartObject();
            foreach (var pk in pkProps) pk.WriteTo(writer);
            foreach (var col in dataProps) col.WriteTo(writer);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static long NowMicros()
        => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000L;

    private static int ResolveTtlSeconds(CdcRowMetadata? metadata)
    {
        if (metadata is not { HasTtl: true } md) return 0;
        return ResolveTtlSecondsFromExpiry(md.ExpiryEpochSeconds);
    }

    /// <summary>
    /// Replay-clamp policy: a cell/row with a remaining TTL &gt; 0 carries
    /// that value forward; an already-expired one is written with TTL=1 so
    /// the destination tombstones it via the same LWW path the source
    /// took; no TTL writes with TTL=0 (no expiry).
    /// </summary>
    private static int ResolveTtlSecondsFromExpiry(long? expiryEpochSeconds)
        => ResolveTtlSecondsFromExpiry(
            expiryEpochSeconds,
            DateTimeOffset.UtcNow.ToUnixTimeSeconds());

    private static int ResolveTtlSecondsFromExpiry(
        long? expiryEpochSeconds,
        long nowEpochSeconds)
    {
        if (expiryEpochSeconds is not long expiry) return 0;
        long remaining = expiry - nowEpochSeconds;
        return remaining > 0 ? (int)Math.Min(remaining, int.MaxValue) : 1;
    }
}
