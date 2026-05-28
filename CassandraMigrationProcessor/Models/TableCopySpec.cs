namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Immutable description of a single table copy. Identifies the source
/// and target keyspace/table; runtime sessions are not threaded through
/// here — readers and writers open sessions via the job-wide
/// <c>ISessionFactory</c>.
/// </summary>
public record TableCopySpec(
    string KeyspaceName,
    string TableName,
    string TargetKeyspaceName,
    string TargetTableName);
