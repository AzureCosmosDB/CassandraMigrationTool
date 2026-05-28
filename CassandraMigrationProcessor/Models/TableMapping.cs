namespace CassandraMigrationProcessor.Models;

/// <summary>DTO: source keyspace/table plus optional target rename for one entry in the migration spec.</summary>
public class TableMapping
{
    public required string TableName { get; set; }
    public required string KeyspaceName { get; set; }
    public string? TargetTableName { get; set; }
    public string? TargetKeyspaceName { get; set; }
}
