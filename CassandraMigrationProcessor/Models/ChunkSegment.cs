namespace CassandraMigrationProcessor.Models
{
    public class ChunkSegment
    {
        public bool? IsProcessed { get; set; }
        public long QueryRowCount { get; set; }
        public long ResultRowCount { get; set; }
        public string Id { get; set; } = string.Empty;
    }
}
