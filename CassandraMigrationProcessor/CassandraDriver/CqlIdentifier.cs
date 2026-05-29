using System.Text;
using System.Text.RegularExpressions;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// CQL identifier (keyspace / table / UDT / column name) validator and
/// normaliser. Used before building CQL strings to keep injected /
/// exotic characters out of the wire.
/// <para>
/// CQL has two identifier forms:
/// </para>
/// <list type="bullet">
/// <item><b>Unquoted</b> — must match <c>[a-zA-Z_][a-zA-Z0-9_]*</c>;
/// case-insensitive in the wire protocol (folded to lower-case by the
/// server).</item>
/// <item><b>Quoted</b> — wrapped in <c>"..."</c>, may contain ANY
/// character except an unescaped <c>"</c>; embedded <c>"</c> is doubled
/// (<c>""</c>). Case-sensitive and may include hyphens, mixed case,
/// reserved words, unicode, even spaces.</item>
/// </list>
/// <para>
/// Storage convention in this project: identifiers are stored in their
/// <i>bare</i> form (no surrounding <c>"</c>, no <c>""</c> escaping).
/// CQL builders unconditionally wrap them in <c>"..."</c>, so any bare
/// character except a raw <c>"</c> is safe on the wire.
/// </para>
/// </summary>
public static class CqlIdentifier
{
    private static readonly Regex UnquotedAllowed =
        new(@"^[a-zA-Z0-9_\-]+$", RegexOptions.Compiled);

    /// <summary>
    /// If <paramref name="raw"/> is wrapped in CQL double-quotes,
    /// returns the bare identifier (quotes stripped, <c>""</c>
    /// un-escaped to <c>"</c>). Otherwise returns the input unchanged
    /// (with whitespace trimmed). Tolerates <c>null</c> by returning
    /// <c>string.Empty</c>.
    /// </summary>
    public static string Unquote(string? raw)
    {
        if (raw == null) return string.Empty;
        var s = raw.Trim();
        if (s.Length >= 2 && s[0] == '"' && s[^1] == '"')
        {
            // Strip outer quotes and undo the "" escape for any
            // embedded double-quote.
            return s.Substring(1, s.Length - 2).Replace("\"\"", "\"");
        }
        return s;
    }

    /// <summary>
    /// Throws <see cref="ArgumentException"/> if
    /// <paramref name="identifier"/> is not a legal CQL identifier in
    /// its bare form. Accepts EITHER:
    /// <list type="bullet">
    /// <item>An unquoted identifier matching
    /// <c>[a-zA-Z0-9_\-]+</c> — historical strict form.</item>
    /// <item>Any non-empty string that does not contain a raw <c>"</c>
    /// (since callers always wrap output in <c>"..."</c>, any other
    /// character is wire-safe). This is what enables case-sensitive,
    /// hyphenated, reserved-word and unicode table / column names
    /// like <c>MixedCase_Table-1</c>, <c>SELECT</c>, <c>日本語</c>.</item>
    /// </list>
    /// Returns the identifier unchanged on success so callers can
    /// fluently chain.
    /// </summary>
    public static string Validate(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("CQL identifier cannot be empty");

        // Fast path: strict unquoted form.
        if (UnquotedAllowed.IsMatch(identifier))
            return identifier;

        // Permissive path: legal as the inside of a "..." quoted
        // identifier. Raw " is the only character we cannot accept
        // because the CQL builders do not double-escape it; defer
        // that until a real customer needs it.
        if (identifier.Contains('"'))
            throw new ArgumentException(
                $"Invalid CQL identifier: contains unescaped double-quote.");

        return identifier;
    }

    /// <summary>
    /// Parse a CQL qualified name (<c>keyspace.table</c>) where either
    /// side may be a quoted identifier. Both returned components are in
    /// their bare form (outer quotes stripped, <c>""</c> escapes
    /// resolved to <c>"</c>) and have been run through
    /// <see cref="Validate"/>.
    /// <para>
    /// Examples:
    /// <list type="bullet">
    /// <item><c>foo.bar</c> → <c>("foo", "bar")</c></item>
    /// <item><c>foo."MixedCase_Table-1"</c> → <c>("foo", "MixedCase_Table-1")</c></item>
    /// <item><c>"My-KS"."Some.Table"</c> → <c>("My-KS", "Some.Table")</c></item>
    /// </list>
    /// </para>
    /// Throws <see cref="ArgumentException"/> on malformed input
    /// (missing dot, unterminated quote, etc.).
    /// </summary>
    public static (string keyspace, string table) SplitQualifiedName(string fullName)
    {
        if (string.IsNullOrWhiteSpace(fullName))
            throw new ArgumentException("Qualified name cannot be empty",
                nameof(fullName));

        var s = fullName.Trim();
        int pos = 0;
        string keyspace = ReadIdentifier(s, ref pos);
        if (pos >= s.Length || s[pos] != '.')
            throw new ArgumentException(
                $"Qualified name must be 'keyspace.table': '{fullName}'",
                nameof(fullName));
        pos++; // consume the dot
        string table = ReadIdentifier(s, ref pos);
        if (pos < s.Length)
            throw new ArgumentException(
                $"Unexpected trailing characters in qualified name: '{fullName}'",
                nameof(fullName));
        return (Validate(keyspace), Validate(table));
    }

    /// <summary>
    /// Low-level identifier reader: starting at <paramref name="pos"/>,
    /// consumes either a quoted (<c>"..."</c> with <c>""</c> escapes) or
    /// a bare identifier (everything up to the next <c>.</c>) and advances
    /// <paramref name="pos"/> past it. Returns the identifier in bare form.
    /// Exposed for callers that need custom split rules (e.g. allowing
    /// a wildcard <c>*</c> on the table side).
    /// </summary>
    public static string ReadIdentifier(string s, ref int pos)
    {
        if (pos < s.Length && s[pos] == '"')
        {
            // Quoted identifier: read until matching unescaped "
            var sb = new StringBuilder();
            pos++; // consume opening "
            while (pos < s.Length)
            {
                if (s[pos] == '"')
                {
                    // CQL escapes embedded " by doubling it
                    if (pos + 1 < s.Length && s[pos + 1] == '"')
                    {
                        sb.Append('"');
                        pos += 2;
                        continue;
                    }
                    pos++; // consume closing "
                    return sb.ToString();
                }
                sb.Append(s[pos]);
                pos++;
            }
            throw new ArgumentException(
                $"Unterminated quoted identifier in '{s}'");
        }
        else
        {
            // Unquoted identifier: read until '.' or end
            int start = pos;
            while (pos < s.Length && s[pos] != '.') pos++;
            return s.Substring(start, pos - start).Trim();
        }
    }
}
