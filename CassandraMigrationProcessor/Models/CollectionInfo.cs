using System;

namespace CassandraMigrationProcessor.Models
{
    public class CollectionInfo
    {
        public required string TableName { get; set; }
        public required string KeyspaceName { get; set; }
        public string? TargetTableName { get; set; }
        public string? TargetKeyspaceName { get; set; }
    }
}
