using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models
{
    public class CopyChunk
    {
        public bool? IsDownloaded { get; set; }
        public long SourceQueryRowCount { get; set; }
        public long TargetInsertedRowCount { get; set; }
        public long TargetFailedRowCount { get; set; }
        public List<ChunkSegment> Segments { get; set; } = new();
        public string Id { get; set; } = string.Empty;
    }
}