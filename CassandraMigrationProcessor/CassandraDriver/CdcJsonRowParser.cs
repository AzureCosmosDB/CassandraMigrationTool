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

    // Zero-alloc UTF-8 keys for property-name comparison on the hot path.
    private static readonly byte[] SysRwTimestampUtf8 =
        Encoding.UTF8.GetBytes(SysRwTimestampColumn);
    private static readonly byte[] SysCellLevelTtlUtf8 =
        Encoding.UTF8.GetBytes(SysCellLevelTtlColumn);
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

    public CdcJsonRowParser()
    {
        _writer = new Utf8JsonWriter(_buffer,
            new JsonWriterOptions { SkipValidation = true });
    }

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
                // Writetime is contractually a number of microseconds. Any
                // other shape means the source envelope violated the contract
                // we depend on — fail loudly rather than drop the metadata.
                if (reader.TokenType != JsonTokenType.Number)
                    throw new JsonException(
                        $"Malformed row payload: '{SysRwTimestampColumn}' was " +
                        $"{reader.TokenType}, expected a Number.");
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
                if (reader.TokenType != JsonTokenType.StartArray)
                    throw new JsonException(
                        $"Malformed row payload: '{SysCellLevelTtlColumn}' was " +
                        $"{reader.TokenType}, expected an array.");

                bool first = true;
                while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                {
                    if (first)
                    {
                        // First element must be the expiry epoch (number).
                        if (reader.TokenType != JsonTokenType.Number)
                            throw new JsonException(
                                $"Malformed row payload: '{SysCellLevelTtlColumn}'[0] " +
                                $"was {reader.TokenType}, expected the expiry epoch as a Number.");
                        long expiry = reader.GetInt64();
                        if (expiry > 0) expiryEpochSeconds = expiry;
                        first = false;
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
        return (cleaned, new CdcRowMetadata(writetime, expiryEpochSeconds));
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
    public static (string CleanedJson, CdcRowMetadata Metadata) ParseOnce(string jsonRow)
        => new CdcJsonRowParser().Parse(jsonRow);
}
