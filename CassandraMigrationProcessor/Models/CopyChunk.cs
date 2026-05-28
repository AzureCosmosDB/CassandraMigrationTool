namespace CassandraMigrationProcessor.Models;

/// <summary>DTO: a single bulk-copy chunk for a table — row counts and the segments it covers.</summary>
public class CopyChunk
{
    public bool? IsDownloaded { get; set; }
    public long SourceQueryRowCount { get; set; }
    public long TargetInsertedRowCount { get; set; }
    public long TargetFailedRowCount { get; set; }
    public List<ChunkSegment> Segments { get; set; } = new();
    public string Id { get; set; } = string.Empty;
}
