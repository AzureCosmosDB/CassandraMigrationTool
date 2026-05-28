using Cassandra;

namespace CassandraMigrationProcessor.Models;
public record TableCopySpec(
    string KeyspaceName,
    string TableName,
    string TargetKeyspaceName,
    string TargetTableName,
    ISession SourceSession);
