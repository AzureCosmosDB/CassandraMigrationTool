namespace CassandraMigrationProcessor.Models;

/// <summary>DTO: a single bulk-copy chunk for a table — row counts.</summary>
public class CopyChunk
{
    public bool? IsDownloaded { get; set; }
    public long SourceQueryRowCount { get; set; }
    public long TargetInsertedRowCount { get; set; }
    public long TargetFailedRowCount { get; set; }
    public string Id { get; set; } = string.Empty;
}
