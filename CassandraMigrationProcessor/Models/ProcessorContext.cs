using Cassandra;
using System;

namespace CassandraMigrationProcessor.Models
{
    public class ProcessorContext
    {
        public required string MigrationUnitId { get; set; }
        public required string JobId { get; set; }
        public required string KeyspaceName { get; set; }
        public required string TableName { get; set; }
        public required string TargetKeyspaceName { get; set; }
        public required string TargetTableName { get; set; }
        public required ISession SourceSession { get; set; }
        public long DownloadCount { get; set; }
    }
}
