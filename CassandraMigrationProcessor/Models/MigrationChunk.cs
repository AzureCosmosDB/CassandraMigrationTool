using System.Collections.Generic;

namespace CassandraMigrationProcessor
{
    public class MigrationChunk
    {
        public string? TokenRangeStart { get; set; }
        public string? TokenRangeEnd { get; set; }
        public bool? IsDownloaded { get; set; }
        public bool? IsUploaded { get; set; }
        public long SourceQueryRowCount { get; set; }
        public long SourceResultRowCount { get; set; }
        public long TargetInsertedRowCount { get; set; }
        public long TargetFailedRowCount { get; set; }
        public long RowCountInTarget { get; set; }
        public long SkippedAsDuplicateCount { get; set; }
        public List<Segment> Segments { get; set; } = new();
        public string Id { get; set; } = string.Empty;
        public int Attempt { get; set; }

        public MigrationChunk() { }

        public MigrationChunk(string tokenStart, string tokenEnd, bool? downloaded, bool? uploaded)
        {
            TokenRangeStart = tokenStart;
            TokenRangeEnd = tokenEnd;
            IsDownloaded = downloaded;
            IsUploaded = uploaded;
        }
    }
}