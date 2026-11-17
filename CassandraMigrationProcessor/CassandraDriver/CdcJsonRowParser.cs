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

    private static readonly byte[] SysRwTimestampUtf8 =
        Encoding.UTF8.GetBytes(SysRwTimestampColumn);
    private static readonly byte[] SysRwTtlUtf8 =
        Encoding.UTF8.GetBytes(SysRwTtlColumn);
    private static readonly byte[] SysCellLevelTtlUtf8 =
        Encoding.UTF8.GetBytes(SysCellLevelTtlColumn);
    private static readonly byte[] SysCellLevelWritetimeUtf8 =
        Encoding.UTF8.GetBytes(SysCellLevelWritetimeColumn);
    private static ReadOnlySpan<byte> SysColumnPrefixUtf8 => "__sys_"u8;

    private readonly ArrayBufferWriter<byte> _buffer = new(initialCapacity: 4096);
    private readonly Utf8JsonWriter _writer;

    private byte[] _utf8 = new byte[8192];
    private byte[] _scratch = new byte[1024];

    // Per-row scratch state, cleared (not reallocated) for every row.
    private readonly Dictionary<string, long> _colWritetime = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TtlEntry> _colTtl = new(StringComparer.Ordinal);

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
        _colTtl.Clear();

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
                // {col: micros | [element micros] | {field: micros}}. CQL cannot
                // set element/field writetimes individually, so each column is
                // reduced to its newest writetime.
                reader.Read();
                if (reader.TokenType == JsonTokenType.StartObject)
                {
                    while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                    {
                        string col = CqlIdentifier.Unquote(reader.GetString());
                        reader.Read();
                        if (ReadLatestWritetime(ref reader) is long cellWritetime)
                            _colWritetime[col] = cellWritetime;
                    }
                }
                else
                {
                    latestCellWritetime = Max(latestCellWritetime, ReadLatestWritetime(ref reader));
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

            WritePropertyName(ref reader, _writer);
            reader.Read();
            CopyValue(ref reader, _writer);
        }

        _writer.WriteEndObject();
        _writer.Flush();

        if (reader.TokenType != JsonTokenType.EndObject)
            throw new JsonException("Malformed row payload: unterminated root object.");
        if (reader.Read())
            throw new JsonException("Unexpected trailing content after row object.");

        var cleaned = Encoding.UTF8.GetString(_buffer.WrittenSpan);

        foreach (var cellWritetime in _colWritetime.Values)
            latestCellWritetime = Max(latestCellWritetime, cellWritetime);

        long nowEpochSeconds = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        long? markerExpiry = markerWritetime.HasValue && markerTtl is TtlPair mt
            ? ResolveExpiry(mt, ttlBase, markerWritetime, nowEpochSeconds)
            : null;

        var perColumn = BuildPerColumn(
            markerWritetime, markerExpiry, ttlBase, nowEpochSeconds);
        return (cleaned, new CdcRowMetadata(
            markerWritetime, markerExpiry, perColumn, latestCellWritetime));
    }

    private static long? Max(long? a, long? b) =>
        a is long x && b is long y ? Math.Max(x, y) : a ?? b;

    /// <summary>
    /// Absolute expiry of one <c>[duration, offset]</c> pair: <c>base +
    /// offset</c> when the duration is positive, <c>null</c> (no TTL)
    /// otherwise. If the source reports a positive duration without a usable
    /// base, the expiry is anchored on the cell writetime instead of failing.
    /// </summary>
    private static long? ResolveExpiry(
        TtlPair pair, long ttlBase, long? writetimeMicros, long nowEpochSeconds)
    {
        if (pair.Duration <= 0)
            return null;
        long expiry = ttlBase + pair.Offset;
        if (expiry > 0)
            return expiry;
        long anchor = writetimeMicros is long wt ? wt / 1_000_000 : nowEpochSeconds;
        return anchor + pair.Duration;
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
            CdcCellMetadata cell;
            if (_colTtl.TryGetValue(col, out var ttl))
                cell = ResolveCell(ttl, wt, ttlBase, nowEpochSeconds);
            else
                cell = new CdcCellMetadata(wt, hasMarker ? markerExpiry : null);

            if (hasMarker
                && cell.WritetimeMicros == markerWritetime
                && cell.ExpiryEpochSeconds == markerExpiry
                && !cell.HasHeterogeneousExpiry)
                continue;

            result ??= new Dictionary<string, CdcCellMetadata>(StringComparer.Ordinal);
            result[col] = cell;
        }

        return result;
    }

    private static CdcCellMetadata ResolveCell(
        TtlEntry ttl, long? wt, long ttlBase, long nowEpochSeconds)
    {
        if (ttl.Scalar is TtlPair pair)
            return new CdcCellMetadata(wt, ResolveExpiry(pair, ttlBase, wt, nowEpochSeconds));

        if (ttl.Elements is { } elements)
        {
            var expiries = new long?[elements.Count];
            for (int i = 0; i < elements.Count; i++)
                expiries[i] = ResolveExpiry(elements[i], ttlBase, wt, nowEpochSeconds);
            return Uniform(expiries, out var common)
                ? new CdcCellMetadata(wt, common)
                : new CdcCellMetadata(wt, Longest(expiries), ElementExpiries: expiries);
        }

        var fieldExpiries = new Dictionary<string, long?>(StringComparer.Ordinal);
        foreach (var (field, fieldPair) in ttl.Fields!)
        {
            if (fieldPair is TtlPair fp)
                fieldExpiries[field] = ResolveExpiry(fp, ttlBase, wt, nowEpochSeconds);
        }
        return Uniform(fieldExpiries.Values, out var fieldCommon)
            ? new CdcCellMetadata(wt, fieldCommon)
            : new CdcCellMetadata(wt, Longest(fieldExpiries.Values), FieldExpiries: fieldExpiries);
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
