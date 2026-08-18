namespace CassandraMigrationProcessor.Models;

/// <summary>
/// Immutable description of a single table copy. Identifies the source
/// and target keyspace/table; runtime sessions are not threaded through
/// here — readers use the job-wide source session and writers open
/// worker-owned sessions through <c>ISessionFactory</c>.
/// </summary>
public record TableCopySpec(
    string KeyspaceName,
    string TableName,
    string TargetKeyspaceName,
    string TargetTableName);
