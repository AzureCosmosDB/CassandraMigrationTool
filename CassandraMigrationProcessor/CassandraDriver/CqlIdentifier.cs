using System.Text;
using System.Text.RegularExpressions;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// CQL identifier (keyspace / table / UDT / column) validator and
/// normaliser. Used before building CQL strings to keep injected /
/// exotic characters out of the wire.
/// <para>
/// Storage convention: identifiers are stored in <i>bare</i> form (no
/// surrounding <c>"</c>, no <c>""</c> escaping). CQL builders
/// unconditionally wrap them in <c>"..."</c>.
/// </para>
/// </summary>
public static class CqlIdentifier
{
    // Fast-path strict CQL grammar for an unquoted identifier. Hyphens,
    // leading digits, and Unicode require quoting and fall through to
    // the permissive path.
    private static readonly Regex UnquotedAllowed =
        new(@"^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

    /// <summary>
    /// If <paramref name="raw"/> is wrapped in CQL double-quotes,
    /// returns the bare identifier (quotes stripped, <c>""</c>
    /// un-escaped). Otherwise returns the trimmed input.
    /// </summary>
    public static string Unquote(string? raw)
    {
        if (raw == null) return string.Empty;
        var s = raw.Trim();
        if (s.Length >= 2 && s[0] == '"' && s[^1] == '"')
        {
            return s.Substring(1, s.Length - 2).Replace("\"\"", "\"");
        }
        return s;
    }

    /// <summary>
    /// Throws <see cref="ArgumentException"/> if
    /// <paramref name="identifier"/> is not a legal CQL identifier in
    /// its bare form. Accepts the strict unquoted form
    /// <c>[A-Za-z_][A-Za-z0-9_]*</c> or any non-empty string without a
    /// raw <c>"</c> (CQL builders wrap output in quotes).
    /// </summary>
    public static string Validate(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("CQL identifier cannot be empty");

        if (UnquotedAllowed.IsMatch(identifier))
            return identifier;

        if (identifier.Contains('"'))
            throw new ArgumentException(
                $"Invalid CQL identifier: contains unescaped double-quote.");

        return identifier;
    }

    /// <summary>
    /// Wraps <paramref name="identifier"/> in CQL double-quotes,
    /// doubling any embedded <c>"</c> as required by the CQL grammar.
    /// </summary>
    public static string Quote(string identifier)
    {
        if (string.IsNullOrEmpty(identifier))
            throw new ArgumentException("CQL identifier cannot be empty",
                nameof(identifier));
        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }

    /// <summary>
    /// Parse a CQL qualified name (<c>keyspace.table</c>) where either
    /// side may be a quoted identifier. Returned components are in
    /// bare form and have been run through <see cref="Validate"/>.
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
        pos++;
        string table = ReadIdentifier(s, ref pos);
        if (pos < s.Length)
            throw new ArgumentException(
                $"Unexpected trailing characters in qualified name: '{fullName}'",
                nameof(fullName));
        return (Validate(keyspace), Validate(table));
    }

    /// <summary>
    /// Low-level identifier reader: starting at <paramref name="pos"/>,
    /// consumes either a quoted (<c>"..."</c> with <c>""</c> escapes)
    /// or a bare identifier and advances <paramref name="pos"/> past
    /// it. Returns bare form.
    /// </summary>
    public static string ReadIdentifier(string s, ref int pos)
    {
        if (pos < s.Length && s[pos] == '"')
        {
            var sb = new StringBuilder();
            pos++;
            while (pos < s.Length)
            {
                if (s[pos] == '"')
                {
                    // CQL escapes embedded " by doubling it.
                    if (pos + 1 < s.Length && s[pos + 1] == '"')
                    {
                        sb.Append('"');
                        pos += 2;
                        continue;
                    }
                    pos++;
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
            int start = pos;
            while (pos < s.Length && s[pos] != '.') pos++;
            return s.Substring(start, pos - start).Trim();
        }
    }
}
