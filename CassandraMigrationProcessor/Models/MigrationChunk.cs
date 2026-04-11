using System.Collections.Generic;

namespace CassandraMigrationProcessor.Models
{
    public class Segment
    {
        public bool? IsProcessed { get; set; }
        public long QueryRowCount { get; set; }
        public long ResultRowCount { get; set; }
        public string Id { get; set; } = string.Empty;
    }

    public class MigrationChunk
    {
        public bool? IsDownloaded { get; set; }
        public bool? IsUploaded { get; set; }
        public long SourceQueryRowCount { get; set; }
        public long SourceResultRowCount { get; set; }
        public long TargetInsertedRowCount { get; set; }
        public long TargetFailedRowCount { get; set; }
        public long SkippedAsDuplicateCount { get; set; }
        public List<Segment> Segments { get; set; } = new();
        public string Id { get; set; } = string.Empty;
        public int Attempt { get; set; }
    }
}