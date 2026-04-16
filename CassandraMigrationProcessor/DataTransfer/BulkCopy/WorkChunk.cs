namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;
/// <summary>
/// Tracks a pending or completed read-write cycle.
/// Managed by Partition via LinkedList.
/// </summary>
internal class WorkChunk
{
    public byte[]? ContinuationToken { get; set; }
    public bool IsCompleted { get; set; }
}
