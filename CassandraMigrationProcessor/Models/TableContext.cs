using Cassandra;

namespace CassandraMigrationProcessor.Models
{
    public class TableContext
    {
        public required string KeyspaceName { get; set; }
        public required string TableName { get; set; }
        public required string TargetKeyspaceName { get; set; }
        public required string TargetTableName { get; set; }
        public required ISession SourceSession { get; set; }
    }
}
