using Cassandra;

namespace CassandraMigrationProcessor.Models
{
    public record TableContext(
        string KeyspaceName,
        string TableName,
        string TargetKeyspaceName,
        string TargetTableName,
        ISession SourceSession);
}
