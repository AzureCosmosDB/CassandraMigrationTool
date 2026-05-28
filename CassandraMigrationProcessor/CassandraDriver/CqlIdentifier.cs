using System.Text.RegularExpressions;

namespace CassandraMigrationProcessor.CassandraDriver;

/// <summary>
/// CQL identifier (keyspace / table / UDT / column name) validator. Used
/// before building CQL strings to keep injected/exotic characters out of
/// the wire. Lives next to the rest of the Cassandra driver-facing code.
/// </summary>
internal static class CqlIdentifier
{
    private static readonly Regex Allowed = new(@"^[a-zA-Z0-9_\-]+$", RegexOptions.Compiled);

    /// <summary>
    /// Throws <see cref="ArgumentException"/> if <paramref name="identifier"/>
    /// is null/empty/whitespace or contains characters outside
    /// [a-zA-Z0-9_-]. Returns the identifier unchanged on success so
    /// callers can fluently chain.
    /// </summary>
    public static string Validate(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("CQL identifier cannot be empty");
        if (!Allowed.IsMatch(identifier))
            throw new ArgumentException($"Invalid CQL identifier: {identifier}");
        return identifier;
    }
}
