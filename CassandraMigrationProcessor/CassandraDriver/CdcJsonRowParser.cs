using System.Buffers;
using System.Text;
using System.Text.Json;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Extracts CDC metadata (row-marker and per-cell writetime/TTL) from a
/// source <c>SELECT JSON *</c> envelope and returns the same envelope
/// stripped of the synthetic <c>__sys_*</c> system columns so the
/// destination can re-insert it via <c>INSERT INTO ... JSON ?</c>
/// without referencing columns that do not exist on its schema.
/// </summary>
/// <remarks>
/// <para>
/// Every row is decoded at cell granularity. The row-liveness marker
/// (<c>__sys_rw_ts</c>/<c>__sys_rw_ttl</c>) is reported separately from the
/// cells, and only columns whose writetime or expiry differs from the marker
/// (or whose elements/fields expire at different times) are listed in
/// <see cref="CdcRowMetadata.PerColumn"/>. Uniform rows therefore keep the
/// single-statement fast path in the writer.
/// </para>
/// <para>
/// Performance: metadata extraction and system-column stripping are done in
/// a single forward-only traversal, writing the cleaned envelope into a
/// reused buffer. Instances are cheap and intended to be reused across all
/// rows in a page (they are not thread-safe — use one per reading loop).
/// </para>
/// </remarks>
internal sealed class CdcJsonRowParser
{
    internal const string SysRwTimestampColumn = "__sys_rw_ts";
    internal const string SysRwTtlColumn = "__sys_rw_ttl";
    internal const string SysCellLevelTtlColumn = "__sys_clttl";
    internal const string SysCellLevelWritetimeColumn = "__sys_clts";
    internal const string SysCellTombstoneColumn = "__sys_clts_tmbstn";

    private static readonly byte[] SysRwTimestampUtf8 =
        Encoding.UTF8.GetBytes(SysRwTimestampColumn);
    private static readonly byte[] SysRwTtlUtf8 =
        Encoding.UTF8.GetBytes(SysRwTtlColumn);
    private static readonly byte[] SysCellLevelTtlUtf8 =
        Encoding.UTF8.GetBytes(SysCellLevelTtlColumn);
    private static readonly byte[] SysCellLevelWritetimeUtf8 =
        Encoding.UTF8.GetBytes(SysCellLevelWritetimeColumn);
    private static readonly byte[] SysCellTombstoneUtf8 =
        Encoding.UTF8.GetBytes(SysCellTombstoneColumn);
    private static ReadOnlySpan<byte> SysColumnPrefixUtf8 => "__sys_"u8;

    private readonly ArrayBufferWriter<byte> _buffer = new(initialCapacity: 4096);
    private readonly Utf8JsonWriter _writer;

    private byte[] _utf8 = new byte[8192];
    private byte[] _scratch = new byte[1024];

    // Per-row scratch state, cleared (not reallocated) for every row.
    private readonly Dictionary<string, long> _colWritetime = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<long?>> _colElementWritetimes = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<string, long?>> _colFieldWritetimes = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TtlEntry> _colTtl = new(StringComparer.Ordinal);
    private readonly HashSet<string> _absentCells = new(StringComparer.Ordinal);
    private readonly Dictionary<string, CdcCellDeletion> _colDeletion = new(StringComparer.Ordinal);
    private readonly List<string> _containerColumns = new();
    private ArrayBufferWriter<byte>? _rewriteBuffer;
    private Utf8JsonWriter? _rewriteWriter;

    /// <summary><c>[duration, offset]</c> as reported by the source.</summary>
    private readonly record struct TtlPair(long Duration, long Offset);

    /// <summary>
    /// Raw TTL detail of one column: a single pair (simple/frozen), one pair
    /// per element (non-frozen collection) or one pair per field (non-frozen
    /// UDT; <c>null</c> for fields without a cell).
    /// </summary>
    private readonly record struct TtlEntry(
        TtlPair? Scalar,
        List<TtlPair>? Elements,
        List<KeyValuePair<string, TtlPair?>>? Fields);

    public CdcJsonRowParser()
    {
        _writer = new Utf8JsonWriter(_buffer,
            new JsonWriterOptions { SkipValidation = true });
    }

    /// <summary>
    /// Parse one <c>SELECT JSON *</c> row payload, extract the CDC metadata,
    /// and return a cleaned JSON envelope without the <c>__sys_*</c> columns.
    /// </summary>
    /// <exception cref="JsonException">The payload is not valid JSON, the row
    /// root is not an object, or a system column has an unexpected shape.</exception>
    public (string CleanedJson, CdcRowMetadata Metadata) Parse(string jsonRow)
    {
        int maxBytes = Encoding.UTF8.GetMaxByteCount(jsonRow.Length);
        if (_utf8.Length < maxBytes)
            _utf8 = new byte[Math.Max(maxBytes, _utf8.Length * 2)];
        int len = Encoding.UTF8.GetBytes(jsonRow, _utf8);

        long? markerWritetime = null;
        TtlPair? markerTtl = null;
        long? latestCellWritetime = null;
        long ttlBase = 0;
        _colWritetime.Clear();
        _colElementWritetimes.Clear();
        _colFieldWritetimes.Clear();
        _colTtl.Clear();
        _absentCells.Clear();
        _colDeletion.Clear();
        _containerColumns.Clear();

        _buffer.Clear();
        _writer.Reset(_buffer);

        var reader = new Utf8JsonReader(_utf8.AsSpan(0, len));
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            throw new JsonException("Expected JSON object at row root.");

        _writer.WriteStartObject();

        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            if (reader.ValueTextEquals(SysRwTimestampUtf8))
            {
                reader.Read();
                // null = the row has no liveness marker (UPDATE-only row).
                if (reader.TokenType == JsonTokenType.Null)
                    continue;
                if (reader.TokenType != JsonTokenType.Number)
                    throw new JsonException(
                        $"Malformed row payload: '{SysRwTimestampColumn}' was " +
                        $"{reader.TokenType}, expected a Number or Null.");
                markerWritetime = reader.GetInt64();
                continue;
            }

            if (reader.ValueTextEquals(SysRwTtlUtf8))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.Null)
                    continue;
                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException(
                        $"Malformed row payload: '{SysRwTtlColumn}' was " +
                        $"{reader.TokenType}, expected an array or Null.");
                markerTtl = ParseTtlPair(ref reader, SysRwTtlColumn);
                continue;
            }

            if (reader.ValueTextEquals(SysCellLevelTtlUtf8))
            {
                // [base, {col: detail}] — walk the whole array so stripping
                // stays correct regardless of the column's position.
                reader.Read();
                if (reader.TokenType == JsonTokenType.Null)
                    continue;
                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException(
                        $"Malformed row payload: '{SysCellLevelTtlColumn}' was " +
                        $"{reader.TokenType}, expected an array or Null.");

                bool first = true;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (first)
                    {
                        if (reader.TokenType != JsonTokenType.Number)
                            throw new JsonException(
                                $"Malformed row payload: '{SysCellLevelTtlColumn}'[0] " +
                                $"was {reader.TokenType}, expected the expiry base as a Number.");
                        ttlBase = reader.GetInt64();
                        first = false;
                    }
                    else if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        ParseCellTtlDetail(ref reader);
                    }
                    else
                    {
                        reader.Skip();
                    }
                }
                continue;
            }

            if (reader.ValueTextEquals(SysCellLevelWritetimeUtf8))
            {
                // {col: micros | [element micros] | {field: micros}}. Element
                // and field writetimes are kept so they can be replayed
                // individually; the column's newest writetime is also kept.
                reader.Read();
                if (reader.TokenType == JsonTokenType.StartObject)
                {
                    while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                    {
                        string col = CqlIdentifier.Unquote(reader.GetString());
                        reader.Read();
                        if (reader.TokenType == JsonTokenType.Null)
                        {
                            _absentCells.Add(col);
                            continue;
                        }
                        if (ReadCellWritetimes(ref reader, col) is long cellWritetime)
                            _colWritetime[col] = cellWritetime;
                    }
                }
                else
                {
                    latestCellWritetime = Max(latestCellWritetime, ReadLatestWritetime(ref reader));
                }
                continue;
            }

            if (reader.ValueTextEquals(SysCellTombstoneUtf8))
            {
                // {col: micros | [collectionDeletion, {element|field: micros}]}.
                // Every deletion is kept with its own timestamp so the writer
                // can replay each one exactly.
                reader.Read();
                if (reader.TokenType == JsonTokenType.StartObject)
                {
                    while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                    {
                        string col = CqlIdentifier.Unquote(reader.GetString());
                        reader.Read();
                        if (ReadDeletion(ref reader) is { } deletion)
                            _colDeletion[col] = deletion;
                    }
                }
                else
                {
                    reader.Skip();
                }
                continue;
            }

            // Drop any other server-added __sys_* column so it cannot leak into
            // the destination INSERT as an unknown column.
            var name = reader.ValueSpan;
            if (name.Length >= SysColumnPrefixUtf8.Length
                && name.Slice(0, SysColumnPrefixUtf8.Length)
                       .SequenceEqual(SysColumnPrefixUtf8))
            {
                reader.Read();
                reader.Skip();
                continue;
            }

            var nameReader = reader;
            WritePropertyName(ref reader, _writer);
            reader.Read();
            if (reader.TokenType is JsonTokenType.StartArray or JsonTokenType.StartObject)
                _containerColumns.Add(CqlIdentifier.Unquote(nameReader.GetString()));
            CopyValue(ref reader, _writer);
        }

        _writer.WriteEndObject();
        _writer.Flush();

        if (reader.TokenType != JsonTokenType.EndObject)
            throw new JsonException("Malformed row payload: unterminated root object.");
        if (reader.Read())
            throw new JsonException("Unexpected trailing content after row object.");

        var cleaned = NeedsAbsentCellRewrite()
            ? RewriteAbsentCellsAsNull(_buffer.WrittenSpan)
            : Encoding.UTF8.GetString(_buffer.WrittenSpan);

        foreach (var cellWritetime in _colWritetime.Values)
            latestCellWritetime = Max(latestCellWritetime, cellWritetime);

        long nowEpochSeconds = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long? markerExpiry = markerWritetime.HasValue && markerTtl is TtlPair mt
            ? ResolveExpiry(mt, ttlBase, nowEpochSeconds)
            : null;

        var perColumn = BuildPerColumn(
            markerWritetime, markerExpiry, ttlBase, nowEpochSeconds);
        return (cleaned, new CdcRowMetadata(
            markerWritetime, markerExpiry, perColumn, latestCellWritetime,
            _colDeletion.Count == 0
                ? null
                : new Dictionary<string, CdcCellDeletion>(_colDeletion, StringComparer.Ordinal)));
    }

    private static long? Max(long? a, long? b) =>
        a is long x && b is long y ? Math.Max(x, y) : a ?? b;

    /// <summary>
    /// Absolute expiry of one <c>[duration, offset]</c> pair: <c>base +
    /// offset</c> when the duration is positive, <c>null</c> (no TTL)
    /// otherwise. If the source reports a positive duration without a usable
    /// base, the time the TTL was applied is unknown (it is unrelated to the
    /// cell's <c>USING TIMESTAMP</c>), so the full duration is counted from
    /// now: the cell then never expires before it does on the source.
    /// </summary>
    private static long? ResolveExpiry(TtlPair pair, long ttlBase, long nowEpochSeconds)
    {
        if (pair.Duration <= 0)
            return null;
        long expiry = ttlBase + pair.Offset;
        return expiry > 0 ? expiry : nowEpochSeconds + pair.Duration;
    }

    /// <summary>
    /// Return the largest integer anywhere in the value the reader is
    /// positioned on (a writetime, or a map/array of them), leaving the
    /// reader on that value's last token. Non-numeric tokens are ignored.
    /// </summary>
    private static long? ReadLatestWritetime(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.Number)
            return reader.TryGetInt64(out long single) ? single : null;
        if (reader.TokenType is not (JsonTokenType.StartObject or JsonTokenType.StartArray))
            return null;

        long? latest = null;
        int depth = reader.CurrentDepth;
        while (reader.Read())
        {
            if (reader.CurrentDepth == depth
                && reader.TokenType is JsonTokenType.EndObject or JsonTokenType.EndArray)
                return latest;
            if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out long value))
                latest = Max(latest, value);
        }

        throw new JsonException(
            $"Malformed row payload: '{SysCellLevelWritetimeColumn}' did not terminate.");
    }

    /// <summary>
    /// Decode one <c>__sys_clts_tmbstn</c> column entry: a number for a
    /// simple or frozen cell, <c>[collectionDeletion, {key: micros}]</c> for
    /// a non-frozen collection or UDT (keys are elements, map keys or field
    /// names), or a bare <c>{key: micros}</c> object. Null timestamps mean
    /// "no deletion". Returns <c>null</c> when the entry holds no deletion.
    /// </summary>
    private static CdcCellDeletion? ReadDeletion(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.Number)
            return reader.TryGetInt64(out long simple) ? new CdcCellDeletion(simple, false) : null;
        if (reader.TokenType is not (JsonTokenType.StartArray or JsonTokenType.StartObject))
        {
            reader.Skip();
            return null;
        }

        long? collectionDeletion = null;
        List<KeyValuePair<string, long>>? keyDeletions = null;

        void ReadKeys(ref Utf8JsonReader r)
        {
            while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
            {
                string key = r.GetString()!;
                r.Read();
                if (r.TokenType == JsonTokenType.Number && r.TryGetInt64(out long micros))
                    (keyDeletions ??= []).Add(new(key, micros));
                else
                    r.Skip();
            }
        }

        if (reader.TokenType == JsonTokenType.StartObject)
        {
            ReadKeys(ref reader);
        }
        else
        {
            bool first = true;
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            {
                if (first && reader.TokenType == JsonTokenType.Number
                    && reader.TryGetInt64(out long collection))
                    collectionDeletion = collection;
                else if (reader.TokenType == JsonTokenType.StartObject)
                    ReadKeys(ref reader);
                else
                    reader.Skip();
                first = false;
            }
        }

        if (collectionDeletion is null && keyDeletions is null)
            return null;
        long latest = collectionDeletion ?? long.MinValue;
        if (keyDeletions is not null)
            foreach (var d in keyDeletions)
                latest = Math.Max(latest, d.Value);
        return new CdcCellDeletion(latest, true, collectionDeletion, keyDeletions);
    }

    /// <summary>
    /// Read one <c>__sys_clts</c> column entry, recording element (array) or
    /// field (object) writetimes, and return the column's newest writetime.
    /// Unexpected nested shapes fall back to the newest number found.
    /// </summary>
    private long? ReadCellWritetimes(ref Utf8JsonReader reader, string col)
    {
        if (reader.TokenType is not (JsonTokenType.StartArray or JsonTokenType.StartObject))
            return ReadLatestWritetime(ref reader);

        var scan = reader;
        long? latest = ReadLatestWritetime(ref reader);

        if (scan.TokenType == JsonTokenType.StartArray)
        {
            var elements = new List<long?>();
            while (scan.Read() && scan.TokenType != JsonTokenType.EndArray)
            {
                if (scan.TokenType == JsonTokenType.Number && scan.TryGetInt64(out long t))
                    elements.Add(t);
                else if (scan.TokenType == JsonTokenType.Null)
                    elements.Add(null);
                else
                    return latest;
            }
            _colElementWritetimes[col] = elements;
            return latest;
        }

        var fields = new Dictionary<string, long?>(StringComparer.Ordinal);
        while (scan.Read() && scan.TokenType == JsonTokenType.PropertyName)
        {
            string field = CqlIdentifier.Unquote(scan.GetString());
            scan.Read();
            if (scan.TokenType == JsonTokenType.Number && scan.TryGetInt64(out long t))
                fields[field] = t;
            else if (scan.TokenType == JsonTokenType.Null)
                fields[field] = null;
            else
                return latest;
        }
        _colFieldWritetimes[col] = fields;
        return latest;
    }

    /// <summary>
    /// Decode the <c>__sys_clttl</c> detail object. Each column is a
    /// <c>[duration, offset]</c> pair, an array of pairs (one per collection
    /// element) or an object of pairs (one per UDT field). The reader is
    /// positioned on the detail <c>StartObject</c> and is left on its
    /// matching <c>EndObject</c>.
    /// </summary>
    private void ParseCellTtlDetail(ref Utf8JsonReader reader)
    {
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            string col = CqlIdentifier.Unquote(reader.GetString());
            reader.Read();
            switch (reader.TokenType)
            {
                case JsonTokenType.Null:
                    continue;
                case JsonTokenType.StartObject:
                    _colTtl[col] = new TtlEntry(null, null, ParseFieldTtls(ref reader, col));
                    continue;
                case JsonTokenType.StartArray:
                    break;
                default:
                    throw new JsonException(
                        $"Malformed row payload: '{SysCellLevelTtlColumn}' detail " +
                        $"for '{col}' was {reader.TokenType}, expected an array or object.");
            }

            // Peek on a copy of the reader to tell [dur, off] from [[dur,off],…].
            var peek = reader;
            if (!peek.Read())
                throw new JsonException(
                    $"Malformed row payload: '{SysCellLevelTtlColumn}' detail " +
                    $"for '{col}' did not terminate.");

            if (peek.TokenType == JsonTokenType.EndArray)
            {
                reader.Skip();
                continue;
            }

            if (peek.TokenType == JsonTokenType.Number)
            {
                _colTtl[col] = new TtlEntry(ParseTtlPair(ref reader, col), null, null);
                continue;
            }

            if (peek.TokenType != JsonTokenType.StartArray)
                throw new JsonException(
                    $"Malformed row payload: '{SysCellLevelTtlColumn}' detail " +
                    $"for '{col}' began with {peek.TokenType}, expected a TTL " +
                    "duration or per-element array.");

            var elements = new List<TtlPair>();
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
            {
                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException(
                        $"Malformed row payload: '{SysCellLevelTtlColumn}' element " +
                        $"metadata for '{col}' was {reader.TokenType}, expected an array.");
                elements.Add(ParseTtlPair(ref reader, col));
            }
            if (reader.TokenType != JsonTokenType.EndArray)
                throw new JsonException(
                    $"Malformed row payload: '{SysCellLevelTtlColumn}' element " +
                    $"metadata for '{col}' did not terminate.");
            _colTtl[col] = new TtlEntry(null, elements, null);
        }
    }

    private static List<KeyValuePair<string, TtlPair?>> ParseFieldTtls(
        ref Utf8JsonReader reader, string col)
    {
        var fields = new List<KeyValuePair<string, TtlPair?>>();
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            string field = CqlIdentifier.Unquote(reader.GetString());
            reader.Read();
            if (reader.TokenType == JsonTokenType.Null)
            {
                fields.Add(new(field, null));
                continue;
            }
            if (reader.TokenType != JsonTokenType.StartArray)
                throw new JsonException(
                    $"Malformed row payload: '{SysCellLevelTtlColumn}' field " +
                    $"metadata for '{col}.{field}' was {reader.TokenType}, expected an array.");
            fields.Add(new(field, ParseTtlPair(ref reader, $"{col}.{field}")));
        }
        if (reader.TokenType != JsonTokenType.EndObject)
            throw new JsonException(
                $"Malformed row payload: '{SysCellLevelTtlColumn}' field " +
                $"metadata for '{col}' did not terminate.");
        return fields;
    }

    /// <summary>
    /// Parse one <c>[duration, offset]</c> pair. The reader starts on the
    /// pair's <c>StartArray</c> and ends on its <c>EndArray</c>.
    /// </summary>
    private static TtlPair ParseTtlPair(ref Utf8JsonReader reader, string col)
    {
        if (!reader.Read() || reader.TokenType != JsonTokenType.Number)
            throw new JsonException(
                $"Malformed row payload: TTL duration for '{col}' must be a Number.");
        long duration = reader.GetInt64();
        if (duration < 0)
            throw new JsonException(
                $"Malformed row payload: TTL duration for '{col}' cannot be negative.");

        if (!reader.Read() || reader.TokenType != JsonTokenType.Number)
            throw new JsonException(
                $"Malformed row payload: TTL expiry offset for '{col}' must be a Number.");
        long offset = reader.GetInt64();

        if (!reader.Read() || reader.TokenType != JsonTokenType.EndArray)
            throw new JsonException(
                $"Malformed row payload: TTL detail for '{col}' must contain exactly two numbers.");

        return new TtlPair(duration, offset);
    }

    /// <summary>
    /// Resolve every column's writetime and expiry and return those that
    /// cannot be written together with the row marker: a different
    /// writetime or expiry, or elements/fields with differing expiries.
    /// Without a marker every live column is returned. <c>null</c> when
    /// nothing diverges.
    /// </summary>
    private Dictionary<string, CdcCellMetadata>? BuildPerColumn(
        long? markerWritetime,
        long? markerExpiry,
        long ttlBase,
        long nowEpochSeconds)
    {
        if (_colWritetime.Count == 0 && _colTtl.Count == 0)
            return null;

        bool hasMarker = markerWritetime.HasValue;
        Dictionary<string, CdcCellMetadata>? result = null;

        foreach (var col in EnumerateCellColumns())
        {
            long? wt = _colWritetime.TryGetValue(col, out var w) ? w : markerWritetime;
            var cell = ResolveCell(
                col, wt, hasMarker ? markerExpiry : null, ttlBase, nowEpochSeconds);

            if (hasMarker
                && cell.WritetimeMicros == markerWritetime
                && cell.ExpiryEpochSeconds == markerExpiry
                && !cell.HasPerElementMetadata)
                continue;

            result ??= new Dictionary<string, CdcCellMetadata>(StringComparer.Ordinal);
            result[col] = cell;
        }

        return result;
    }

    /// <summary>
    /// Resolve one column's writetime and expiry. Collection elements and
    /// UDT fields are reported individually only when they do not all share
    /// one writetime and expiry.
    /// </summary>
    private CdcCellMetadata ResolveCell(
        string col, long? wt, long? defaultExpiry, long ttlBase, long nowEpochSeconds)
    {
        bool hasTtl = _colTtl.TryGetValue(col, out var ttl);
        if (hasTtl && ttl.Scalar is TtlPair pair)
            return new CdcCellMetadata(wt, ResolveExpiry(pair, ttlBase, nowEpochSeconds));

        var elementTtls = hasTtl ? ttl.Elements : null;
        var elementWts = _colElementWritetimes.GetValueOrDefault(col);
        if (elementTtls is not null || elementWts is not null)
        {
            int count = elementTtls?.Count ?? elementWts!.Count;
            if (count == 0)
                return new CdcCellMetadata(wt, defaultExpiry);
            if (elementWts is not null && elementWts.Count != count)
                elementWts = null;

            var writetimes = new long?[count];
            var expiries = new long?[count];
            for (int i = 0; i < count; i++)
            {
                writetimes[i] = elementWts?[i] ?? wt;
                expiries[i] = elementTtls is null
                    ? defaultExpiry
                    : ResolveExpiry(elementTtls[i], ttlBase, nowEpochSeconds);
            }
            if (Uniform(expiries, out var common) && Uniform(writetimes, out _))
                return new CdcCellMetadata(wt, common);
            return new CdcCellMetadata(wt, Longest(expiries),
                ElementExpiries: expiries, ElementWritetimes: writetimes);
        }

        var fieldTtls = hasTtl ? ttl.Fields : null;
        var fieldWts = _colFieldWritetimes.GetValueOrDefault(col);
        if (fieldTtls is null && fieldWts is null)
            return new CdcCellMetadata(wt, defaultExpiry);

        // Only fields with a live cell are reported.
        var fieldExpiries = new Dictionary<string, long?>(StringComparer.Ordinal);
        var fieldWritetimes = new Dictionary<string, long?>(StringComparer.Ordinal);
        if (fieldTtls is not null)
        {
            foreach (var (field, fieldPair) in fieldTtls)
            {
                if (fieldPair is not TtlPair fp)
                    continue;
                long? fieldWt = fieldWts?.GetValueOrDefault(field) ?? wt;
                fieldWritetimes[field] = fieldWt;
                fieldExpiries[field] = ResolveExpiry(fp, ttlBase, nowEpochSeconds);
            }
        }
        else
        {
            foreach (var (field, fieldWt) in fieldWts!)
            {
                if (fieldWt is null)
                    continue;
                fieldWritetimes[field] = fieldWt;
                fieldExpiries[field] = defaultExpiry;
            }
        }

        return Uniform(fieldExpiries.Values, out var fieldCommon) && Uniform(fieldWritetimes.Values, out _)
            ? new CdcCellMetadata(wt, fieldCommon)
            : new CdcCellMetadata(wt, Longest(fieldExpiries.Values),
                FieldExpiries: fieldExpiries, FieldWritetimes: fieldWritetimes);
    }

    private static bool Uniform(IEnumerable<long?> values, out long? common)
    {
        common = null;
        bool first = true;
        foreach (var v in values)
        {
            if (first)
            {
                common = v;
                first = false;
            }
            else if (v != common)
            {
                return false;
            }
        }
        return true;
    }

    /// <summary>Longest expiry; <c>null</c> (immortal) wins over any value.</summary>
    private static long? Longest(IEnumerable<long?> values)
    {
        long? longest = null;
        foreach (var v in values)
        {
            if (v is null)
                return null;
            longest = Max(longest, v);
        }
        return longest;
    }

    private IEnumerable<string> EnumerateCellColumns()
    {
        foreach (var kv in _colWritetime)
            yield return kv.Key;
        foreach (var kv in _colTtl)
        {
            if (!_colWritetime.ContainsKey(kv.Key))
                yield return kv.Key;
        }
    }


    /// <summary>
    /// The source renders a column without a live cell as an empty
    /// collection (<c>[]</c>/<c>{}</c>) for frozen collections, but reports
    /// its <c>__sys_clts</c> entry as <c>null</c>. Writing that rendering
    /// back would create a live, non-null empty value on the destination
    /// (keeping an otherwise-expired row alive), so such columns are nulled.
    /// </summary>
    private bool NeedsAbsentCellRewrite()
    {
        if (_absentCells.Count == 0)
            return false;
        foreach (var col in _containerColumns)
        {
            if (_absentCells.Contains(col))
                return true;
        }
        return false;
    }

    private string RewriteAbsentCellsAsNull(ReadOnlySpan<byte> json)
    {
        _rewriteBuffer ??= new ArrayBufferWriter<byte>(json.Length);
        _rewriteBuffer.Clear();
        if (_rewriteWriter is null)
            _rewriteWriter = new Utf8JsonWriter(_rewriteBuffer,
                new JsonWriterOptions { SkipValidation = true });
        else
            _rewriteWriter.Reset(_rewriteBuffer);

        var reader = new Utf8JsonReader(json);
        reader.Read();
        _rewriteWriter.WriteStartObject();
        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            bool absent = _absentCells.Contains(CqlIdentifier.Unquote(reader.GetString()));
            WritePropertyName(ref reader, _rewriteWriter);
            reader.Read();
            if (absent)
            {
                reader.Skip();
                _rewriteWriter.WriteNullValue();
            }
            else
            {
                CopyValue(ref reader, _rewriteWriter);
            }
        }
        _rewriteWriter.WriteEndObject();
        _rewriteWriter.Flush();
        return Encoding.UTF8.GetString(_rewriteBuffer.WrittenSpan);
    }

    /// <summary>
    /// Write the property name the reader is currently positioned on. In the
    /// common case the name is neither escaped nor split across buffer
    /// segments, so its raw UTF-8 bytes are written directly — avoiding a
    /// managed string allocation per property. Only escaped or multi-segment
    /// names (rare) fall back to <see cref="Utf8JsonReader.GetString"/>. Both
    /// paths route through the writer's encoder, so output escaping is
    /// identical either way.
    /// </summary>
    private static void WritePropertyName(ref Utf8JsonReader reader, Utf8JsonWriter writer)
    {
        if (!reader.HasValueSequence && !reader.ValueIsEscaped)
            writer.WritePropertyName(reader.ValueSpan);
        else
            writer.WritePropertyName(reader.GetString()!);
    }

    /// <summary>
    /// Recursively copy the value the reader is currently positioned on
    /// into the writer. String and number tokens are copied as raw bytes
    /// so the source's exact representation (and escaping) is preserved
    /// without allocating a managed string per value.
    /// </summary>
    private void CopyValue(ref Utf8JsonReader reader, Utf8JsonWriter writer)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.StartObject:
                writer.WriteStartObject();
                while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                {
                    WritePropertyName(ref reader, writer);
                    reader.Read();
                    CopyValue(ref reader, writer);
                }
                writer.WriteEndObject();
                break;
            case JsonTokenType.StartArray:
                writer.WriteStartArray();
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                    CopyValue(ref reader, writer);
                writer.WriteEndArray();
                break;
            case JsonTokenType.String:
                CopyStringRaw(ref reader, writer);
                break;
            case JsonTokenType.Number:
                writer.WriteRawValue(reader.ValueSpan, skipInputValidation: true);
                break;
            case JsonTokenType.True:
                writer.WriteBooleanValue(true);
                break;
            case JsonTokenType.False:
                writer.WriteBooleanValue(false);
                break;
            case JsonTokenType.Null:
                writer.WriteNullValue();
                break;
            default:
                throw new JsonException(
                    $"Unexpected token {reader.TokenType} in row payload.");
        }
    }

    /// <summary>
    /// Copy a String token verbatim by reconstructing its original quoted
    /// form (<c>"…"</c>) and emitting it via <see cref="Utf8JsonWriter.WriteRawValue(ReadOnlySpan{byte}, bool)"/>,
    /// preserving the source's exact escaping without materializing a
    /// managed string. Falls back to <see cref="Utf8JsonReader.GetString"/>
    /// for the rare multi-segment token.
    /// </summary>
    private void CopyStringRaw(ref Utf8JsonReader reader, Utf8JsonWriter writer)
    {
        if (reader.HasValueSequence)
        {
            writer.WriteStringValue(reader.GetString());
            return;
        }

        var content = reader.ValueSpan;
        int need = content.Length + 2;
        if (_scratch.Length < need)
            _scratch = new byte[Math.Max(need, _scratch.Length * 2)];
        _scratch[0] = (byte)'"';
        content.CopyTo(_scratch.AsSpan(1));
        _scratch[content.Length + 1] = (byte)'"';
        writer.WriteRawValue(_scratch.AsSpan(0, need), skipInputValidation: true);
    }

    /// <summary>
    /// Convenience one-shot parse for callers that do not reuse an
    /// instance (e.g. unit tests). Allocates a throwaway parser.
    /// </summary>
    public static (string CleanedJson, CdcRowMetadata Metadata) ParseOnce(
        string jsonRow)
        => new CdcJsonRowParser().Parse(jsonRow);
}
