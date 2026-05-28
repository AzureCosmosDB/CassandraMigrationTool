namespace CassandraMigrationProcessor.Models;

/// <summary>DTO: one processed/unprocessed segment inside a <see cref="CopyChunk"/>.</summary>
public class ChunkSegment
{
    public bool? IsProcessed { get; set; }
    public string Id { get; set; } = string.Empty;
}
