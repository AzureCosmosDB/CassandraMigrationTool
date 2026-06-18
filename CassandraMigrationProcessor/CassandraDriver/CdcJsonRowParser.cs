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
/// </remarks>
internal static class CdcJsonRowParser
{
    // System metadata column names emitted by the source when JSON +
    // changefeed are both requested. Kept here (not as a Constants
    // class) because they are an implementation contract of THIS
    // decoder, not of the rest of the codebase.
    internal const string SysRwTimestampColumn = "__sys_rw_ts";
    internal const string SysCellLevelTtlColumn = "__sys_clttl";
    private const string SysColumnPrefix = "__sys_";

    /// <summary>
    /// Parse one <c>SELECT JSON *</c> row payload (the single
    /// <c>[json]</c> column the driver returns), extract the CDC
    /// metadata, and return a cleaned JSON envelope that contains
    /// every regular column from the source row but none of the
    /// <c>__sys_*</c> system columns (which the destination table
    /// does not define and would reject on <c>INSERT JSON</c>).
    /// </summary>
    /// <param name="jsonRow">The JSON string the driver hands back
    /// from the single synthetic <c>[json]</c> column.</param>
    /// <exception cref="JsonException">The driver returned a JSON
    /// payload we cannot parse, or the row root was not a JSON object.</exception>
    public static (string CleanedJson, CdcRowMetadata Metadata) Parse(string jsonRow)
    {
        using var doc = JsonDocument.Parse(jsonRow);
        var root = doc.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
            throw new JsonException(
                $"Expected JSON object at row root, got {root.ValueKind}.");

        var metadata = ExtractMetadata(root);
        var cleaned = StripSysColumns(root);
        return (cleaned, metadata);
    }

    /// <summary>
    /// Extract the writetime + per-row TTL expiry from a parsed JSON
    /// row root. Public for tests; called by <see cref="Parse"/>.
    /// </summary>
    internal static CdcRowMetadata ExtractMetadata(JsonElement root)
    {
        long? writetime = null;
        if (root.TryGetProperty(SysRwTimestampColumn, out var wt)
            && wt.ValueKind == JsonValueKind.Number)
        {
            writetime = wt.GetInt64();
        }

        long? expiryEpochSeconds = null;
        // __sys_clttl shape: [<expiry-epoch-seconds-or-0>, {<per-cell-details>}]
        // [0, ...] means the row has no TTL.
        if (root.TryGetProperty(SysCellLevelTtlColumn, out var clttl)
            && clttl.ValueKind == JsonValueKind.Array
            && clttl.GetArrayLength() >= 1)
        {
            var first = clttl[0];
            if (first.ValueKind == JsonValueKind.Number)
            {
                long expiry = first.GetInt64();
                if (expiry > 0) expiryEpochSeconds = expiry;
            }
        }

        return new CdcRowMetadata(writetime, expiryEpochSeconds);
    }

    /// <summary>
    /// Copy the JSON envelope verbatim, omitting any property whose
    /// name starts with <c>__sys_</c>. We use the prefix (rather than
    /// hard-coding the two known names) so a future server-side
    /// addition of another synthetic metadata column cannot leak into
    /// the destination INSERT as an unknown column reference.
    /// </summary>
    private static string StripSysColumns(JsonElement root)
    {
        using var ms = new MemoryStream();
        using (var writer = new Utf8JsonWriter(ms))
        {
            writer.WriteStartObject();
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.Name.StartsWith(SysColumnPrefix, StringComparison.Ordinal))
                    continue;
                prop.WriteTo(writer);
            }
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(ms.ToArray());
    }
}
