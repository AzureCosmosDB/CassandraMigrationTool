using System.Buffers;
using System.Text;
using System.Text.Json;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// Extracts CDC metadata (writetime + per-row TTL expiry) from a
/// source <c>SELECT JSON *</c> envelope and returns the same envelope
/// stripped of the synthetic <c>__sys_*</c> system columns so the
/// destination can re-insert it via <c>INSERT INTO ... JSON ?</c>
/// without referencing columns that do not exist on its schema.
/// </summary>
/// <remarks>
/// <para>
/// The source surfaces per-row TTL and writetime only when both
/// <c>JSON</c> projection and a change-feed clause are present on the
/// query. Once the reader switches to <c>SELECT JSON *</c>, all type
/// marshalling — including UDTs, tuples, decimals, varints, durations,
/// and nested collections — is delegated to the destination Cassandra
/// server via <c>INSERT JSON</c>, so this class no longer has to
/// understand or re-implement CQL value coercion.
/// </para>
/// <para>
/// Performance: metadata extraction and system-column stripping are
/// done in a <em>single</em> traversal of the parsed row, writing the
/// cleaned envelope into a reused buffer instead of a fresh
/// <c>MemoryStream</c> per row. Instances are cheap and intended to be
/// reused across all rows in a page (they are not thread-safe — use one
/// per reading loop).
/// </para>
/// </remarks>
internal sealed class CdcJsonRowParser
{
    // System metadata column names emitted by the source when JSON +
    // changefeed are both requested. Kept here (not as a Constants
    // class) because they are an implementation contract of THIS
    // decoder, not of the rest of the codebase.
    internal const string SysRwTimestampColumn = "__sys_rw_ts";
    internal const string SysCellLevelTtlColumn = "__sys_clttl";
    internal const string SysCellLevelWritetimeColumn = "__sys_clts";

    // Zero-alloc UTF-8 keys for property-name comparison on the hot path.
    private static readonly byte[] SysRwTimestampUtf8 =
        Encoding.UTF8.GetBytes(SysRwTimestampColumn);
    private static readonly byte[] SysCellLevelTtlUtf8 =
        Encoding.UTF8.GetBytes(SysCellLevelTtlColumn);
    private static readonly byte[] SysCellLevelWritetimeUtf8 =
        Encoding.UTF8.GetBytes(SysCellLevelWritetimeColumn);
    private static ReadOnlySpan<byte> SysColumnPrefixUtf8 => "__sys_"u8;

    // Reused across rows in a page: the output buffer and the writer are
    // reset (not reallocated) per row, so a page of N rows allocates
    // these once instead of N times. Validation is skipped because this
    // writer only ever emits tokens copied from an already-parsed,
    // structurally-valid source envelope.
    private readonly ArrayBufferWriter<byte> _buffer = new(initialCapacity: 4096);
    private readonly Utf8JsonWriter _writer;

    // Reusable scratch buffers, grown on demand and kept for the life of
    // the parser instance (i.e. one page), so hot-loop transcoding and
    // raw-token reconstruction do not allocate per row.
    private byte[] _utf8 = new byte[8192];
    private byte[] _scratch = new byte[1024];

    // Cell-level preservation. When enabled, the parser additionally
    // decodes the per-column writetime (__sys_clts) and per-column TTL
    // (__sys_clttl[1]) maps and, when they diverge from the row-level
    // values, exposes them on CdcRowMetadata.PerColumn so the writer can
    // re-apply each cell's own USING TIMESTAMP / USING TTL. When disabled
    // (the default) the parser behaves exactly as the row-level decoder:
    // these maps fall through to the __sys_* stripping branch at zero
    // added cost. The two scratch dictionaries are reused across rows
    // (cleared, not reallocated) so a page of divergent rows does not
    // allocate a fresh map per row.
    private readonly bool _preserveCellLevel;
    private readonly Dictionary<string, long> _colWritetime;
    private readonly Dictionary<string, long?> _colExpiry;

    public CdcJsonRowParser(bool preserveCellLevel = false)
    {
        _writer = new Utf8JsonWriter(_buffer,
            new JsonWriterOptions { SkipValidation = true });
        _preserveCellLevel = preserveCellLevel;
        _colWritetime = preserveCellLevel
            ? new Dictionary<string, long>(StringComparer.Ordinal)
            : EmptyWritetime;
        _colExpiry = preserveCellLevel
            ? new Dictionary<string, long?>(StringComparer.Ordinal)
            : EmptyExpiry;
    }

    private static readonly Dictionary<string, long> EmptyWritetime = new();
    private static readonly Dictionary<string, long?> EmptyExpiry = new();

    /// <summary>
    /// Parse one <c>SELECT JSON *</c> row payload (the single
    /// <c>[json]</c> column the driver returns), extract the CDC
    /// metadata, and return a cleaned JSON envelope that contains
    /// every regular column from the source row but none of the
    /// <c>__sys_*</c> system columns (which the destination table
    /// does not define and would reject on <c>INSERT JSON</c>).
    /// Metadata capture and stripping happen in one forward-only pass
    /// over the row bytes — no intermediate document tree is built.
    /// </summary>
    /// <exception cref="JsonException">The driver returned a JSON
    /// payload we cannot parse, or the row root was not a JSON object.</exception>
    public (string CleanedJson, CdcRowMetadata Metadata) Parse(string jsonRow)
    {
        // Transcode the driver's UTF-16 string into a reused UTF-8 buffer
        // once, then read forward-only. This is the same single transcode
        // JsonDocument.Parse(string) performs internally, but without the
        // per-row document tree, LINQ closures, or MemoryStream/ToArray copy.
        int maxBytes = Encoding.UTF8.GetMaxByteCount(jsonRow.Length);
        if (_utf8.Length < maxBytes)
            _utf8 = new byte[Math.Max(maxBytes, _utf8.Length * 2)];
        int len = Encoding.UTF8.GetBytes(jsonRow, _utf8);

        long? writetime = null;
        long? expiryEpochSeconds = null;
        if (_preserveCellLevel)
        {
            _colWritetime.Clear();
            _colExpiry.Clear();
        }

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
                // A JSON null (or an absent column) is the source's legitimate
                // "no writetime" signal for this row — tolerate it and fall
                // back to the default, exactly as pre-streaming versions did.
                // Only a present, non-null value of the wrong shape means the
                // source envelope violated the contract we depend on, and only
                // that case fails loudly rather than dropping the metadata.
                if (reader.TokenType == JsonTokenType.Null)
                    continue;
                if (reader.TokenType != JsonTokenType.Number)
                    throw new JsonException(
                        $"Malformed row payload: '{SysRwTimestampColumn}' was " +
                        $"{reader.TokenType}, expected a Number or Null.");
                writetime = reader.GetInt64();
                continue;
            }

            if (reader.ValueTextEquals(SysCellLevelTtlUtf8))
            {
                // __sys_clttl shape: [<expiry-epoch-seconds-or-0>, {<details>}]
                // [0, ...] means the row has no TTL. Walk the whole array so
                // stripping stays correct regardless of the column's position
                // (SELECT JSON * does not guarantee __sys_* columns come last).
                reader.Read();
                // A JSON null (or an absent column) means the row has no
                // cell-level TTL — the source emits this for rows without a
                // TTL. Tolerate it (no expiry) as pre-streaming versions did;
                // only a present, non-null value that is not the expected
                // array is a contract violation worth failing on.
                if (reader.TokenType == JsonTokenType.Null)
                    continue;
                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException(
                        $"Malformed row payload: '{SysCellLevelTtlColumn}' was " +
                        $"{reader.TokenType}, expected an array or Null.");

                bool first = true;
                long baseExpiry = 0;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (first)
                    {
                        // First element must be the expiry epoch (number).
                        if (reader.TokenType != JsonTokenType.Number)
                            throw new JsonException(
                                $"Malformed row payload: '{SysCellLevelTtlColumn}'[0] " +
                                $"was {reader.TokenType}, expected the expiry epoch as a Number.");
                        baseExpiry = reader.GetInt64();
                        if (baseExpiry > 0) expiryEpochSeconds = baseExpiry;
                        first = false;
                    }
                    else if (_preserveCellLevel
                             && reader.TokenType == JsonTokenType.StartObject)
                    {
                        // Second element is the per-column TTL detail map
                        // {col:[ttl_duration_sec, expiry_offset_from_base_sec]}.
                        // Decode it only in cell-level mode; otherwise it is
                        // opaque and skipped below.
                        ParseCellTtlDetail(ref reader, baseExpiry);
                    }
                    else
                    {
                        // Remaining elements are an opaque detail object we do
                        // not consume; skipping them is intentional, not an
                        // error path.
                        reader.Skip();
                    }
                }
                continue;
            }

            if (_preserveCellLevel && reader.ValueTextEquals(SysCellLevelWritetimeUtf8))
            {
                // __sys_clts shape: {col: <writetime-micros> | [per-element…]}.
                // Scalar (and frozen) columns carry a single writetime number;
                // non-frozen collections carry a per-element array that CQL
                // cannot re-apply per element, so those are skipped.
                reader.Read();
                if (reader.TokenType == JsonTokenType.StartObject)
                {
                    while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
                    {
                        string col = reader.GetString()!;
                        reader.Read();
                        if (reader.TokenType == JsonTokenType.Number)
                            _colWritetime[col] = reader.GetInt64();
                        else
                            reader.Skip();
                    }
                }
                else if (reader.TokenType != JsonTokenType.Null)
                {
                    reader.Skip();
                }
                continue;
            }

            // Defensive: drop any future server-added __sys_* column so it
            // cannot leak into the destination INSERT as an unknown column.
            // Regular column names never start with "__sys_". Consume the
            // value regardless of its position or shape.
            var name = reader.ValueSpan;
            if (name.Length >= SysColumnPrefixUtf8.Length
                && name.Slice(0, SysColumnPrefixUtf8.Length)
                       .SequenceEqual(SysColumnPrefixUtf8))
            {
                reader.Read();
                reader.Skip();
                continue;
            }

            // Regular column: emit name + value into the cleaned envelope.
            WritePropertyName(ref reader, _writer);
            reader.Read();
            CopyValue(ref reader, _writer);
        }

        _writer.WriteEndObject();
        _writer.Flush();

        // Guard against trailing content after the root object. Unlike
        // JsonDocument.Parse, a forward-only reader would otherwise silently
        // ignore a second value or stray non-whitespace bytes.
        if (reader.TokenType != JsonTokenType.EndObject)
            throw new JsonException("Malformed row payload: unterminated root object.");
        if (reader.Read())
            throw new JsonException("Unexpected trailing content after row object.");

        var cleaned = Encoding.UTF8.GetString(_buffer.WrittenSpan);
        var perColumn = _preserveCellLevel
            ? BuildPerColumnDivergence(writetime, expiryEpochSeconds)
            : null;
        return (cleaned, new CdcRowMetadata(writetime, expiryEpochSeconds, perColumn));
    }

    /// <summary>
    /// Decode the <c>__sys_clttl</c> detail object
    /// <c>{col:[ttl_duration_sec, expiry_offset_from_base_sec]}</c> into
    /// <see cref="_colExpiry"/>. A per-column absolute expiry is
    /// <c>baseExpiry + offset</c>; a duration of <c>0</c> means that column
    /// has no TTL (recorded as a present-but-null entry so it is
    /// distinguishable from "inherit the row expiry"). Non-frozen
    /// collection columns present an array-of-arrays instead of a
    /// <c>[dur, offset]</c> pair; those cannot be preserved per element and
    /// are skipped, falling back to the row-level TTL for the whole column.
    /// The reader is positioned on the detail <c>StartObject</c> and is left
    /// on its matching <c>EndObject</c>.
    /// </summary>
    private void ParseCellTtlDetail(ref Utf8JsonReader reader, long baseExpiry)
    {
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            string col = reader.GetString()!;
            reader.Read();
            if (reader.TokenType != JsonTokenType.StartArray)
            {
                reader.Skip();
                continue;
            }

            // Peek the first array element on a copy of the reader to tell a
            // scalar [dur, offset] pair from a collection [[dur,off],…].
            var peek = reader;
            peek.Read();
            if (peek.TokenType != JsonTokenType.Number)
            {
                // Collection column — cannot preserve per element; skip whole.
                reader.Skip();
                continue;
            }

            reader.Read();
            long dur = reader.TokenType == JsonTokenType.Number ? reader.GetInt64() : 0;
            reader.Read();
            long offset = reader.TokenType == JsonTokenType.Number ? reader.GetInt64() : 0;
            while (reader.TokenType != JsonTokenType.EndArray)
                reader.Read();

            _colExpiry[col] = (dur > 0 && baseExpiry > 0)
                ? baseExpiry + offset
                : (long?)null;
        }
    }

    /// <summary>
    /// Fold the decoded per-column writetime/TTL maps into a divergence
    /// map. Only columns whose resolved (writetime, expiry) differs from
    /// the row-level values are returned; when every scalar column matches
    /// the row level (the overwhelmingly common case) the result is
    /// <c>null</c> and the writer keeps its single-statement fast path.
    /// </summary>
    private Dictionary<string, CdcCellMetadata>? BuildPerColumnDivergence(
        long? rowWritetime, long? rowExpiry)
    {
        if (_colWritetime.Count == 0 && _colExpiry.Count == 0)
            return null;

        Dictionary<string, CdcCellMetadata>? diverged = null;

        foreach (var col in EnumerateCellColumns())
        {
            long? cellWt = _colWritetime.TryGetValue(col, out var wt) ? wt : rowWritetime;
            long? cellExp = _colExpiry.TryGetValue(col, out var exp) ? exp : rowExpiry;

            if (cellWt != rowWritetime || cellExp != rowExpiry)
            {
                diverged ??= new Dictionary<string, CdcCellMetadata>(StringComparer.Ordinal);
                diverged[col] = new CdcCellMetadata(cellWt, cellExp);
            }
        }

        return diverged;
    }

    /// <summary>Union of the column names seen in either per-column map.</summary>
    private IEnumerable<string> EnumerateCellColumns()
    {
        foreach (var kv in _colWritetime)
            yield return kv.Key;
        foreach (var kv in _colExpiry)
            if (!_colWritetime.ContainsKey(kv.Key))
                yield return kv.Key;
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
        string jsonRow, bool preserveCellLevel = false)
        => new CdcJsonRowParser(preserveCellLevel).Parse(jsonRow);
}
