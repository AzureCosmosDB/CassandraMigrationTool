using Newtonsoft.Json;
using System;

namespace CassandraMigrationProcessor.Models
{
    [JsonObject("CollectionInfo")]
    public class TableMapping
    {
        public required string TableName { get; set; }
        public required string KeyspaceName { get; set; }
        public string? TargetTableName { get; set; }
        public string? TargetKeyspaceName { get; set; }
    }
}
