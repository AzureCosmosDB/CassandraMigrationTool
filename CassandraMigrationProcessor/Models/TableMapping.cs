namespace CassandraMigrationProcessor.Models;
public class TableMapping
{
    public required string TableName { get; set; }
    public required string KeyspaceName { get; set; }
    public string? TargetTableName { get; set; }
    public string? TargetKeyspaceName { get; set; }
}
