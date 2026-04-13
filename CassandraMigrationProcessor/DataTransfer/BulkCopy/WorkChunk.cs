namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;
/// <summary>
/// Tracks a pending or completed read-write cycle.
/// Forms a linked list per partition.
/// </summary>
internal class WorkChunk
{
    public byte[]? ContinuationToken { get; set; }
    public bool IsCompleted { get; set; }
    public WorkChunk? Next { get; set; }
}
