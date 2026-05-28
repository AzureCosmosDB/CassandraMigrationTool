namespace CassandraMigrationProcessor.Models;

/// <summary>DTO: persisted registry of known <see cref="Job"/> IDs (<c>JobRegistry.json</c>).</summary>
public class JobIndex
{
    public List<string> MigrationJobIds { get; set; } = new();
}
