using Cassandra;

namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Immutable description of a single table copy: source/target identifiers
/// plus the source <see cref="ISession"/> used to read it.
/// </summary>
public record TableCopySpec(
    string KeyspaceName,
    string TableName,
    string TargetKeyspaceName,
    string TargetTableName,
    ISession SourceSession);
