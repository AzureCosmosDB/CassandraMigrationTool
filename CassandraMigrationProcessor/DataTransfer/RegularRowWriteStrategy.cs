using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Cassandra;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Row-write strategy for non-counter (regular) target tables, fed by a
/// cleaned <c>SELECT JSON *</c> envelope plus the row's change-feed
/// metadata. The destination server handles all type marshalling, so every
/// CQL type the source table contains is supported without per-type code.
/// <para>
/// Writetime and TTL are always preserved at the finest granularity CQL can
/// express:
/// <list type="bullet">
///   <item>Uniform rows (one marker writetime/expiry shared by every cell,
///         no non-frozen list) are written with a single
///         <c>INSERT … JSON ? USING TIMESTAMP ? AND TTL ?</c>.</item>
///   <item>Otherwise the row-liveness marker is restored by a baseline
///         <c>INSERT … JSON ? DEFAULT UNSET</c> holding the cells that share
///         the marker's metadata, and every other cell is replayed with
///         <c>UPDATE</c>s grouped by (writetime, expiry). UPDATE never
///         creates a marker, so rows the source created by UPDATE only stay
///         marker-less on the target.</item>
///   <item>Non-frozen sets/maps/UDTs whose elements or fields expire at
///         different times are written as an assignment followed by
///         <c>c = c + …</c> / <c>u.f = …</c> updates carrying each group's
///         own TTL.</item>
///   <item>Static-only partitions (no clustering row) write only their static
///         columns, addressed by the partition key.</item>
///   <item>Non-frozen lists are not idempotent under retry (every write mints
///         new element keys), so they are always rewritten wholesale at a
///         fresh, strictly increasing timestamp. Element TTLs are preserved;
///         the list's writetime is not.</item>
/// </list>
/// </para>
/// </summary>
internal sealed class RegularRowWriteStrategy : IRowWriteStrategy
{
    /// <summary>Cassandra's maximum TTL (20 years).</summary>
    internal const int MaxTtlSeconds = 630_720_000;

    /// <summary>
    /// Last expiry (epoch seconds) Cassandra versions before 5.0 can store;
    /// later expiries are rejected (CASSANDRA-14092).
    /// </summary>
    internal const long MaxLegacyExpiryEpochSeconds = int.MaxValue - 1;

    /// <summary>Safety margin kept below <see cref="MaxLegacyExpiryEpochSeconds"/>.</summary>
    internal const int LegacyExpiryMarginSeconds = 300;

    /// <summary>
    /// Upper bound on cached UPDATE statements; beyond it multi-column groups
    /// fall back to per-column statements, whose count is bounded by schema.
    /// </summary>
    private const int MaxCachedStatements = 512;

    private readonly WorkerLog _log;
    private readonly ISession _targetSession;
    private readonly PreparedStatement _preparedInsertJson;
    private Lazy<Task<PreparedStatement>> _preparedInsertJsonPartial;
    private readonly List<CassandraColumn> _columns;
    private readonly IReadOnlyList<CassandraColumn> _primaryKeyColumns;
    private readonly IReadOnlyDictionary<string, CassandraColumn> _columnsByName;
    private readonly bool _needsPlanForEveryRow;
    private readonly bool _clampToLegacyExpiry;
    private int _ttlClampWarned;
    private readonly ConcurrentDictionary<string, Lazy<Task<PreparedStatement>>>
        _preparedUpdates = new(StringComparer.Ordinal);
    private readonly string _targetKeyspace;
    private readonly string _targetTable;

    private readonly RetryPolicy _retryPolicy;
    private readonly ConsistencyLevel _writeConsistencyLevel;
    private readonly string _rowKind;

    private RegularRowWriteStrategy(WorkerLog log, ISession targetSession,
        PreparedStatement preparedInsertJson,
        List<CassandraColumn> columns,
        RetryPolicy retryPolicy,
        ConsistencyLevel writeConsistencyLevel,
        string targetKeyspace,
        string targetTable,
        bool clampToLegacyExpiry)
    {
        _log = log;
        _targetSession = targetSession;
        _preparedInsertJson = preparedInsertJson;
        _columns = columns;
        _primaryKeyColumns = OrderPrimaryKey(columns);
        _columnsByName = columns.ToDictionary(c => c.Name, StringComparer.Ordinal);
        // Lists and static-only partitions are only visible in the row
        // payload, so tables that can contain them inspect every row before
        // choosing the single-INSERT fast path.
        _needsPlanForEveryRow = columns.Any(c => IsNonFrozenList(c) || c.Kind == "static");
        _clampToLegacyExpiry = clampToLegacyExpiry;
        _retryPolicy = retryPolicy;
        _writeConsistencyLevel = writeConsistencyLevel;
        _targetKeyspace = targetKeyspace;
        _targetTable = targetTable;
        _rowKind = $"JSON row[{targetKeyspace}.{targetTable}]";
        _preparedInsertJsonPartial = NewLazyInsertJsonPartial();
    }

    /// <summary>
    /// Async factory. Prepares the <c>INSERT … JSON ?</c> statement and
    /// detects whether the target needs pre-5.0 expiry clamping. The factory
    /// shape mirrors <see cref="CounterRowWriteStrategy.CreateAsync"/> so
    /// <see cref="RowWriteStrategyFactory"/> can dispatch generically.
    /// </summary>
    public static async Task<RegularRowWriteStrategy> CreateAsync(
        WorkerLog log, ISession targetSession,
        List<CassandraColumn> columns,
        string targetKeyspace, string targetTable, RetryPolicy retryPolicy,
        ConsistencyLevel writeConsistencyLevel)
    {
        var psJson = await CassandraQueries.PrepareInsertJsonAsync(
            targetSession, targetKeyspace, targetTable, columns);

        return new RegularRowWriteStrategy(
            log, targetSession, psJson, columns, retryPolicy,
            writeConsistencyLevel, targetKeyspace, targetTable,
            TargetNeedsLegacyExpiryClamp(targetSession));
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
        bool isReplay,
        CancellationToken cancellationToken)
    {
        metadata ??= new CdcRowMetadata(NowMicros(), null);

        if (!_needsPlanForEveryRow
            && metadata.HasRowMarker
            && !metadata.HasPerColumnDivergence)
        {
            return WriteFullInsertAsync(cleanedJson, metadata, counters, cancellationToken);
        }

        var plan = BuildRowWritePlan(
            cleanedJson, metadata, isReplay, _primaryKeyColumns, _columnsByName, NowMicros());
        if (plan.UseFullInsert)
            return WriteFullInsertAsync(cleanedJson, metadata, counters, cancellationToken);
        return WritePlanAsync(plan, counters, cancellationToken);
    }

    private Task<WriteOutcome> WriteFullInsertAsync(
        string cleanedJson,
        CdcRowMetadata metadata,
        WriteCounters counters,
        CancellationToken cancellationToken)
    {
        // Bind layout: (envelope, writetime, ttl).
        var bound = _preparedInsertJson.Bind(
            cleanedJson,
            metadata.WritetimeMicros ?? metadata.RowWriteTimestampMicros ?? NowMicros(),
            ResolveTtlSeconds(metadata.ExpiryEpochSeconds, NowEpochSeconds()));
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
    /// Execute a multi-statement plan. Statements run sequentially (baseline,
    /// grouped assignments, element additions, UDT field updates, then list
    /// rewrites) and count as one row.
    /// </summary>
    private async Task<WriteOutcome> WritePlanAsync(
        RowWritePlan plan,
        WriteCounters counters,
        CancellationToken cancellationToken)
    {
        long nowEpochSeconds = NowEpochSeconds();
        var attempts = new List<Func<Task>>();

        if (plan.BaselineJson is not null)
        {
            var partial = await GetPreparedInsertJsonPartialAsync(cancellationToken);
            AddAttempt(attempts,
                partial.Bind(
                    plan.BaselineJson,
                    plan.BaselineWritetimeMicros,
                    ResolveTtlSeconds(plan.BaselineExpiryEpochSeconds, nowEpochSeconds)),
                cancellationToken);
        }

        foreach (var group in plan.Groups)
        {
            var keys = plan.KeyValuesFor(group.IsStatic);
            int ttl = ResolveTtlSeconds(group.ExpiryEpochSeconds, nowEpochSeconds);
            string key = GroupStatementKey(group.IsStatic, group.Columns);
            if (group.Columns.Count == 1
                || _preparedUpdates.ContainsKey(key)
                || _preparedUpdates.Count < MaxCachedStatements)
            {
                var ps = await GetPreparedUpdateAsync(key,
                    () => CassandraQueries.BuildUpdateJsonColumnsCql(
                        _targetKeyspace, _targetTable, group.Columns, _primaryKeyColumns),
                    cancellationToken);
                AddAttempt(attempts,
                    ps.Bind(BuildBindValues(group.WritetimeMicros, ttl, group.JsonValues, keys)),
                    cancellationToken);
                continue;
            }

            for (int i = 0; i < group.Columns.Count; i++)
            {
                var ps = await GetColumnUpdateAsync(group.Columns[i], cancellationToken);
                AddAttempt(attempts,
                    ps.Bind(BuildBindValues(group.WritetimeMicros, ttl, [group.JsonValues[i]], keys)),
                    cancellationToken);
            }
        }

        foreach (var add in plan.Adds)
        {
            var ps = await GetAddUpdateAsync(add.Column, cancellationToken);
            AddAttempt(attempts,
                ps.Bind(BuildBindValues(
                    add.WritetimeMicros,
                    ResolveTtlSeconds(add.ExpiryEpochSeconds, nowEpochSeconds),
                    [add.JsonValue],
                    plan.KeyValuesFor(add.Column.Kind == "static"))),
                cancellationToken);
        }

        foreach (var field in plan.FieldWrites)
        {
            var ps = await GetPreparedUpdateAsync(
                ".\0" + field.Column.Name + "\0" + field.Field,
                () => CassandraQueries.BuildUpdateJsonUdtFieldCql(
                    _targetKeyspace, _targetTable, field.Column, field.Field, _primaryKeyColumns),
                cancellationToken);
            AddAttempt(attempts,
                ps.Bind(BuildBindValues(
                    field.WritetimeMicros,
                    ResolveTtlSeconds(field.ExpiryEpochSeconds, nowEpochSeconds),
                    [field.JsonValue],
                    plan.KeyValuesFor(field.Column.Kind == "static"))),
                cancellationToken);
        }

        foreach (var list in plan.Lists)
        {
            var set = await GetColumnUpdateAsync(list.Column, cancellationToken);
            var append = list.Runs.Count > 1
                ? await GetAddUpdateAsync(list.Column, cancellationToken)
                : null;
            var keys = plan.KeyValuesFor(list.Column.Kind == "static");
            attempts.Add(() => WriteListAsync(list, set, append, keys, cancellationToken));
        }

        return await RowWriteRetry.ExecuteRowGroupsAsync(
            attempts, _retryPolicy, _log, _rowKind, counters, cancellationToken);
    }

    /// <summary>
    /// Rewrite one non-frozen list. Timestamps are reserved afresh on every
    /// attempt, so a retried write always supersedes (rather than duplicates)
    /// the elements of an earlier attempt that timed out but was applied.
    /// The first contiguous run of equally-expiring elements replaces the
    /// list; later runs are appended at successive timestamps.
    /// </summary>
    private async Task WriteListAsync(
        ListWrite list,
        PreparedStatement set,
        PreparedStatement? append,
        IReadOnlyList<string> keys,
        CancellationToken cancellationToken)
    {
        long start = MonotonicTimestamp.Reserve(Math.Max(1, list.Runs.Count));
        long nowEpochSeconds = NowEpochSeconds();

        if (list.Runs.Count == 0)
        {
            await ExecuteAsync(set.Bind(BuildBindValues(start, 0, ["null"], keys)), cancellationToken);
            return;
        }

        for (int i = 0; i < list.Runs.Count; i++)
        {
            var run = list.Runs[i];
            var ps = i == 0 ? set : append!;
            await ExecuteAsync(
                ps.Bind(BuildBindValues(
                    start + i,
                    ResolveTtlSeconds(run.ExpiryEpochSeconds, nowEpochSeconds),
                    [run.JsonValue],
                    keys)),
                cancellationToken);
        }
    }

    private Task ExecuteAsync(BoundStatement bound, CancellationToken cancellationToken)
    {
        bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        bound.SetConsistencyLevel(_writeConsistencyLevel);
        return _targetSession.ExecuteAsync(bound).WaitAsync(cancellationToken);
    }

    private void AddAttempt(
        List<Func<Task>> attempts, BoundStatement bound, CancellationToken cancellationToken)
    {
        bound.SetReadTimeoutMillis(RowWriteRetry.WriteTimeoutMs);
        bound.SetConsistencyLevel(_writeConsistencyLevel);
        attempts.Add(() => _targetSession.ExecuteAsync(bound).WaitAsync(cancellationToken));
    }

    private static object?[] BuildBindValues(
        long writetimeMicros, int ttlSeconds,
        IReadOnlyList<string> jsonValues, IReadOnlyList<string> keyValues)
    {
        var values = new object?[2 + jsonValues.Count + keyValues.Count];
        values[0] = writetimeMicros;
        values[1] = ttlSeconds;
        for (int i = 0; i < jsonValues.Count; i++)
            values[2 + i] = jsonValues[i];
        for (int i = 0; i < keyValues.Count; i++)
            values[2 + jsonValues.Count + i] = keyValues[i];
        return values;
    }

    // ---------------------------------------------------------------- plan

    internal sealed record GroupWrite(
        long WritetimeMicros,
        long? ExpiryEpochSeconds,
        bool IsStatic,
        IReadOnlyList<CassandraColumn> Columns,
        IReadOnlyList<string> JsonValues);

    internal readonly record struct AddWrite(
        CassandraColumn Column, long WritetimeMicros, long? ExpiryEpochSeconds, string JsonValue);

    internal readonly record struct FieldWrite(
        CassandraColumn Column, string Field, long WritetimeMicros,
        long? ExpiryEpochSeconds, string JsonValue);

    internal readonly record struct ListRun(long? ExpiryEpochSeconds, string JsonValue);

    /// <summary>A list rewrite; no runs means "set the list to null".</summary>
    internal sealed record ListWrite(
        CassandraColumn Column, IReadOnlyList<ListRun> Runs);

    internal sealed record RowWritePlan(
        bool UseFullInsert,
        bool IsStaticOnly,
        string? BaselineJson,
        long BaselineWritetimeMicros,
        long? BaselineExpiryEpochSeconds,
        IReadOnlyList<string> PrimaryKeyValues,
        IReadOnlyList<string> PartitionKeyValues,
        IReadOnlyList<GroupWrite> Groups,
        IReadOnlyList<AddWrite> Adds,
        IReadOnlyList<FieldWrite> FieldWrites,
        IReadOnlyList<ListWrite> Lists)
    {
        public IReadOnlyList<string> KeyValuesFor(bool isStatic) =>
            isStatic ? PartitionKeyValues : PrimaryKeyValues;
    }

    /// <summary>
    /// Decide how one source row is written. A pure function of the payload,
    /// its metadata and the schema, so it can be tested without a target.
    /// </summary>
    internal static RowWritePlan BuildRowWritePlan(
        string cleanedJson,
        CdcRowMetadata metadata,
        bool isReplay,
        IReadOnlyList<CassandraColumn> primaryKeyColumns,
        IReadOnlyDictionary<string, CassandraColumn> columnsByName,
        long nowMicros)
    {
        using var doc = JsonDocument.Parse(cleanedJson);
        var root = doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
            throw new JsonException("Row payload must be a JSON object.");

        var keyProps = new Dictionary<string, JsonProperty>(StringComparer.Ordinal);
        var dataProps = new List<(JsonProperty Prop, CassandraColumn Column)>();
        foreach (var prop in root.EnumerateObject())
        {
            string name = CqlIdentifier.Unquote(prop.Name);
            if (!columnsByName.TryGetValue(name, out var column))
                throw new JsonException($"Row payload contains unknown column '{prop.Name}'.");
            if (column.Kind is "partition_key" or "clustering")
                keyProps[name] = prop;
            else
                dataProps.Add((prop, column));
        }

        // A partition holding only static cells is returned with null
        // clustering values; it has no clustering row to write.
        bool isStaticOnly = primaryKeyColumns.Any(c =>
            c.Kind == "clustering"
            && (!keyProps.TryGetValue(c.Name, out var p) || p.Value.ValueKind == JsonValueKind.Null));

        var keyPropList = new List<JsonProperty>(primaryKeyColumns.Count);
        var primaryKeyValues = new List<string>(primaryKeyColumns.Count);
        var partitionKeyValues = new List<string>();
        foreach (var keyColumn in primaryKeyColumns)
        {
            bool isPartitionKey = keyColumn.Kind == "partition_key";
            if (isStaticOnly && !isPartitionKey)
                continue;
            if (!keyProps.TryGetValue(keyColumn.Name, out var prop))
                throw new JsonException(
                    $"Row payload is missing primary-key column '{keyColumn.Name}'.");
            keyPropList.Add(prop);
            primaryKeyValues.Add(prop.Value.GetRawText());
            if (isPartitionKey)
                partitionKeyValues.Add(prop.Value.GetRawText());
        }

        bool writeBaseline = metadata.HasRowMarker && !isStaticOnly;
        long rowWritetime = metadata.WritetimeMicros ?? metadata.RowWriteTimestampMicros ?? nowMicros;
        long? markerExpiry = metadata.HasRowMarker ? metadata.ExpiryEpochSeconds : null;
        var perColumn = metadata.PerColumn;

        var baselineProps = new List<JsonProperty>();
        var groups = new List<GroupWrite>();
        var groupIndex = new Dictionary<(long, long?, bool), int>();
        var adds = new List<AddWrite>();
        var fieldWrites = new List<FieldWrite>();
        var lists = new List<ListWrite>();

        void AddToGroup(long wt, long? expiry, CassandraColumn column, string json)
        {
            bool isStatic = column.Kind == "static";
            var key = (wt, expiry, isStatic);
            if (!groupIndex.TryGetValue(key, out int idx))
            {
                idx = groups.Count;
                groupIndex[key] = idx;
                groups.Add(new GroupWrite(
                    wt, expiry, isStatic, new List<CassandraColumn>(), new List<string>()));
            }
            ((List<CassandraColumn>)groups[idx].Columns).Add(column);
            ((List<string>)groups[idx].JsonValues).Add(json);
        }

        foreach (var (prop, column) in dataProps)
        {
            if (isStaticOnly && column.Kind != "static")
                continue;

            var value = prop.Value;
            bool isNull = value.ValueKind == JsonValueKind.Null;
            CdcCellMetadata cell = default;
            bool hasCell = perColumn is not null && perColumn.TryGetValue(column.Name, out cell);
            if (!hasCell)
            {
                // Null cells have no source metadata: their tombstone is
                // written at the row's writetime without TTL.
                cell = isNull
                    ? new CdcCellMetadata(rowWritetime, null)
                    : new CdcCellMetadata(metadata.WritetimeMicros ?? rowWritetime, markerExpiry);
            }
            long wt = cell.WritetimeMicros ?? rowWritetime;

            // A null list is cleared explicitly on replay: the target copy was
            // written at a fresh timestamp no source-writetime tombstone wins.
            if (IsNonFrozenList(column) && (!isNull || isReplay))
            {
                lists.Add(new ListWrite(column, isNull ? [] : BuildListRuns(value, cell)));
                continue;
            }

            // Write high enough to also cover the column's newest source
            // deletion, so a value, element or field deleted after it was
            // copied is removed on the destination too. A live simple cell is
            // always newer than its own deletion and keeps its writetime; a
            // multi-cell column is rewritten just above the deletion.
            if (metadata.Deletions is { } deletions
                && deletions.TryGetValue(column.Name, out var deletion)
                && (isNull || deletion.IsComplex)
                && deletion.CoveringWriteMicros > wt)
            {
                wt = deletion.CoveringWriteMicros;
            }

            if (writeBaseline && !hasCell && wt == rowWritetime)
            {
                baselineProps.Add(prop);
                continue;
            }

            if (!isNull && cell.ElementExpiries is { } elementExpiries
                && TrySplitElements(value, elementExpiries, out var elementGroups))
            {
                AddToGroup(wt, elementGroups[0].Expiry, column, elementGroups[0].Json);
                for (int i = 1; i < elementGroups.Count; i++)
                    adds.Add(new AddWrite(column, wt, elementGroups[i].Expiry, elementGroups[i].Json));
                continue;
            }

            if (!isNull && cell.FieldExpiries is { } fieldExpiries
                && TrySplitFields(value, fieldExpiries,
                    out var baseFields, out var baseExpiry, out var extraFields))
            {
                AddToGroup(wt, baseExpiry, column, baseFields);
                foreach (var (field, expiry, json) in extraFields)
                    fieldWrites.Add(new FieldWrite(column, field, wt, expiry, json));
                continue;
            }

            AddToGroup(wt, cell.ExpiryEpochSeconds, column, value.GetRawText());
        }

        bool useFullInsert = writeBaseline
            && groups.Count == 0 && adds.Count == 0 && fieldWrites.Count == 0 && lists.Count == 0;

        return new RowWritePlan(
            useFullInsert,
            isStaticOnly,
            writeBaseline ? WriteObject(keyPropList.Concat(baselineProps)) : null,
            rowWritetime,
            markerExpiry,
            primaryKeyValues,
            partitionKeyValues,
            groups,
            adds,
            fieldWrites,
            lists);
    }

    /// <summary>
    /// Split a list into contiguous runs of equally-expiring elements so
    /// element order is preserved when the runs are appended in turn.
    /// </summary>
    private static List<ListRun> BuildListRuns(JsonElement value, CdcCellMetadata cell)
    {
        if (cell.ElementExpiries is not { } expiries
            || value.ValueKind != JsonValueKind.Array
            || value.GetArrayLength() != expiries.Count)
        {
            return [new ListRun(cell.ExpiryEpochSeconds, value.GetRawText())];
        }

        var runs = new List<ListRun>();
        var elements = value.EnumerateArray().ToArray();
        int start = 0;
        for (int i = 1; i <= elements.Length; i++)
        {
            if (i < elements.Length && expiries[i] == expiries[start])
                continue;
            runs.Add(new ListRun(expiries[start], WriteArray(elements[start..i])));
            start = i;
        }
        return runs;
    }

    /// <summary>
    /// Group the elements of a set (JSON array) or map (JSON object) by
    /// expiry, in first-appearance order. Fails when the element count does
    /// not match the metadata, in which case the column is written uniformly.
    /// </summary>
    private static bool TrySplitElements(
        JsonElement value,
        IReadOnlyList<long?> expiries,
        out List<(long? Expiry, string Json)> groups)
    {
        groups = [];
        if (value.ValueKind == JsonValueKind.Array)
        {
            if (value.GetArrayLength() != expiries.Count) return false;
            var elements = value.EnumerateArray().ToArray();
            foreach (var expiry in DistinctInOrder(expiries))
                groups.Add((expiry, WriteArray(elements.Where((_, i) => expiries[i] == expiry))));
            return true;
        }

        if (value.ValueKind == JsonValueKind.Object)
        {
            var entries = value.EnumerateObject().ToArray();
            if (entries.Length != expiries.Count) return false;
            foreach (var expiry in DistinctInOrder(expiries))
                groups.Add((expiry, WriteObject(entries.Where((_, i) => expiries[i] == expiry))));
            return true;
        }

        return false;
    }

    /// <summary>
    /// Split a non-frozen UDT into the fields assigned together (the first
    /// field's expiry group plus every field without a cell) and the
    /// remaining fields, which are written one by one with their own TTL.
    /// </summary>
    private static bool TrySplitFields(
        JsonElement value,
        IReadOnlyDictionary<string, long?> fieldExpiries,
        out string baseJson,
        out long? baseExpiry,
        out List<(string Field, long? Expiry, string Json)> extraFields)
    {
        baseJson = string.Empty;
        baseExpiry = null;
        extraFields = [];
        if (value.ValueKind != JsonValueKind.Object) return false;

        var fields = value.EnumerateObject()
            .Select(p => (Prop: p, Name: CqlIdentifier.Unquote(p.Name)))
            .ToArray();
        if (fields.Count(f => fieldExpiries.ContainsKey(f.Name)) != fieldExpiries.Count)
            return false;

        baseExpiry = fieldExpiries[fields.First(f => fieldExpiries.ContainsKey(f.Name)).Name];
        var baseProps = new List<JsonProperty>();
        foreach (var (prop, name) in fields)
        {
            if (!fieldExpiries.TryGetValue(name, out var expiry) || expiry == baseExpiry)
                baseProps.Add(prop);
            else
                extraFields.Add((name, expiry, prop.Value.GetRawText()));
        }
        baseJson = WriteObject(baseProps);
        return true;
    }

    private static IEnumerable<long?> DistinctInOrder(IReadOnlyList<long?> values)
    {
        var seen = new HashSet<long?>();
        foreach (var v in values)
        {
            if (seen.Add(v))
                yield return v;
        }
    }

    private static string WriteObject(IEnumerable<JsonProperty> props)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { SkipValidation = true }))
        {
            writer.WriteStartObject();
            foreach (var prop in props) prop.WriteTo(writer);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static string WriteArray(IEnumerable<JsonElement> elements)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { SkipValidation = true }))
        {
            writer.WriteStartArray();
            foreach (var element in elements) element.WriteTo(writer);
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>
    /// True for a non-frozen list. Frozen collections are single cells and
    /// are written like any scalar.
    /// </summary>
    internal static bool IsNonFrozenList(CassandraColumn column) =>
        column.Type.StartsWith("list<", StringComparison.OrdinalIgnoreCase);

    private static IReadOnlyList<CassandraColumn> OrderPrimaryKey(IEnumerable<CassandraColumn> columns) =>
        columns
            .Where(c => c.Kind is "partition_key" or "clustering")
            .OrderBy(c => c.Kind == "partition_key" ? 0 : 1)
            .ThenBy(c => c.Position)
            .ToArray();

    // ---------------------------------------------------------- statements

    private static string GroupStatementKey(bool isStatic, IReadOnlyList<CassandraColumn> columns) =>
        (isStatic ? "S\0" : "R\0") + string.Join("\0", columns.Select(c => c.Name));

    private Task<PreparedStatement> GetColumnUpdateAsync(
        CassandraColumn column, CancellationToken cancellationToken) =>
        GetPreparedUpdateAsync(
            GroupStatementKey(column.Kind == "static", [column]),
            () => CassandraQueries.BuildUpdateJsonColumnsCql(
                _targetKeyspace, _targetTable, [column], _primaryKeyColumns),
            cancellationToken);

    private Task<PreparedStatement> GetAddUpdateAsync(
        CassandraColumn column, CancellationToken cancellationToken) =>
        GetPreparedUpdateAsync(
            "+\0" + column.Name,
            () => CassandraQueries.BuildAddJsonColumnCql(
                _targetKeyspace, _targetTable, column, _primaryKeyColumns),
            cancellationToken);

    private async Task<PreparedStatement> GetPreparedUpdateAsync(
        string key, Func<string> buildCql, CancellationToken cancellationToken)
    {
        var lazy = _preparedUpdates.GetOrAdd(
            key,
            _ => new Lazy<Task<PreparedStatement>>(
                () => PrepareWithRetryAsync(() => _targetSession.PrepareAsync(buildCql())),
                LazyThreadSafetyMode.ExecutionAndPublication));
        try
        {
            return await lazy.Value.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            _preparedUpdates.TryRemove(
                new KeyValuePair<string, Lazy<Task<PreparedStatement>>>(key, lazy));
            throw;
        }
    }

    private Lazy<Task<PreparedStatement>> NewLazyInsertJsonPartial()
        => new(
            () => PrepareWithRetryAsync(() => CassandraQueries.PrepareInsertJsonPartialAsync(
                _targetSession, _targetKeyspace, _targetTable, _columns)),
            LazyThreadSafetyMode.ExecutionAndPublication);

    private async Task<PreparedStatement> GetPreparedInsertJsonPartialAsync(
        CancellationToken cancellationToken)
    {
        var lazy = _preparedInsertJsonPartial;
        try
        {
            return await lazy.Value.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            Interlocked.CompareExchange(
                ref _preparedInsertJsonPartial, NewLazyInsertJsonPartial(), lazy);
            throw;
        }
    }

    private async Task<PreparedStatement> PrepareWithRetryAsync(
        Func<Task<PreparedStatement>> prepare)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return await prepare();
            }
            catch (Exception ex) when (
                attempt < _retryPolicy.MaxAttempts
                && ExceptionClassifier.IsTransient(ex))
            {
                await Task.Delay(_retryPolicy.DelayBeforeRetry(attempt));
            }
        }
    }

    // ----------------------------------------------------------------- TTL

    private static long NowMicros() => MonotonicTimestamp.NowMicros();

    private static long NowEpochSeconds() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    private int ResolveTtlSeconds(long? expiryEpochSeconds, long nowEpochSeconds)
    {
        int ttl = ResolveTtlSeconds(
            expiryEpochSeconds, nowEpochSeconds, _clampToLegacyExpiry, out bool clamped);
        if (clamped && Interlocked.Exchange(ref _ttlClampWarned, 1) == 0)
        {
            _log.WriteLine(
                $"{_rowKind}: a source TTL exceeds what the target can store (max " +
                $"{MaxTtlSeconds}s{(_clampToLegacyExpiry ? " and, on Cassandra before 5.0, expiry before 2038-01-19" : "")}). " +
                "Affected cells are written with the largest allowed TTL and expire earlier than on the source.",
                LogType.Warning);
        }
        return ttl;
    }

    /// <summary>
    /// Remaining TTL to bind for an absolute expiry. No expiry binds 0 (which
    /// also overrides any table default TTL, matching the source cell). An
    /// already-expired cell binds 1 so the target expires it through the same
    /// path the source did. TTLs the target cannot store are clamped.
    /// </summary>
    internal static int ResolveTtlSeconds(
        long? expiryEpochSeconds, long nowEpochSeconds, bool clampToLegacyExpiry, out bool clamped)
    {
        clamped = false;
        if (expiryEpochSeconds is not long expiry) return 0;
        long remaining = expiry - nowEpochSeconds;
        if (remaining <= 0) return 1;

        long max = MaxTtlSeconds;
        if (clampToLegacyExpiry)
            max = Math.Min(max, MaxLegacyExpiryEpochSeconds - LegacyExpiryMarginSeconds - nowEpochSeconds);
        max = Math.Max(1, max);
        if (remaining > max)
        {
            clamped = true;
            return (int)max;
        }
        return (int)remaining;
    }

    /// <summary>
    /// Cassandra before 5.0 rejects expiries past 2038-01-19. Clamp unless
    /// every known target host reports 5.0 or later.
    /// </summary>
    private static bool TargetNeedsLegacyExpiryClamp(ISession session)
    {
        try
        {
            var hosts = session.Cluster?.AllHosts();
            if (hosts is null || hosts.Count == 0) return true;
            return hosts.Any(h => h.CassandraVersion is null || h.CassandraVersion.Major < 5);
        }
        catch
        {
            return true;
        }
    }
}

/// <summary>
/// Process-wide strictly increasing microsecond timestamps that are never
/// behind wall-clock time. Used for writes that must supersede every earlier
/// attempt of the same write. Source writetimes are deliberately not used as
/// a floor: one future-skewed source cell would otherwise push every later
/// reservation in the process into the future.
/// </summary>
internal static class MonotonicTimestamp
{
    private static long _last;

    public static long NowMicros() =>
        (DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks) / 10;

    /// <summary>
    /// Reserve <paramref name="count"/> consecutive timestamps and return the
    /// first, which is greater than every previously reserved timestamp and
    /// not behind wall-clock time.
    /// </summary>
    public static long Reserve(int count)
    {
        if (count < 1) throw new ArgumentOutOfRangeException(nameof(count));
        while (true)
        {
            long last = Interlocked.Read(ref _last);
            long start = Math.Max(NowMicros(), last + 1);
            if (Interlocked.CompareExchange(ref _last, start + count - 1, last) == last)
                return start;
        }
    }
}
