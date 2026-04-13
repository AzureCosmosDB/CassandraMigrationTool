using System.Collections.Generic;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;
/// <summary>
/// Represents a feed range partition with its work chunk list.
/// Uses LinkedList for clean node management.
/// </summary>
internal class Partition
{
    public string FeedRange { get; }
    public bool IsExhausted { get; private set; }
    public byte[]? LastPagingState { get; private set; }

    private readonly LinkedList<WorkChunk> _chunks = new();
    private readonly object _lock = new();

    public Partition(string feedRange, byte[]? initialPagingState)
    {
        FeedRange = feedRange;
        LastPagingState = initialPagingState;

        if (initialPagingState != null)
            _chunks.AddLast(new WorkChunk { ContinuationToken = initialPagingState, IsCompleted = true });
    }

    public void SetPageState(byte[]? pagingState, bool isExhausted)
    {
        lock (_lock)
        {
            LastPagingState = pagingState;
            if (isExhausted) IsExhausted = true;
        }
    }

    public WorkChunk AddChunkAndTrim(byte[]? continuationToken)
    {
        var chunk = new WorkChunk { ContinuationToken = continuationToken };
        lock (_lock)
        {
            // Trim completed chunks from the front
            while (_chunks.First != null && _chunks.First.Value.IsCompleted)
                _chunks.RemoveFirst();

            _chunks.AddLast(chunk);
        }
        return chunk;
    }

    public byte[]? GetResumeToken()
    {
        lock (_lock)
        {
            foreach (var chunk in _chunks)
            {
                if (!chunk.IsCompleted) return chunk.ContinuationToken;
            }
            return _chunks.Last?.Value.ContinuationToken;
        }
    }
}
