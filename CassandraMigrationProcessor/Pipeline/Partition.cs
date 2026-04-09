using System.Threading;

namespace CassandraMigrationProcessor.Pipeline
{
    /// <summary>
    /// Represents a feed range partition with its work
    /// chunk list. Passed through the partition pool channel.
    /// </summary>
    internal class Partition
    {
        public string FeedRange { get; }
        public bool IsExhausted { get; set; }
        public byte[]? LastPagingState { get; set; }

        private WorkChunk? _head;
        private WorkChunk? _tail;
        private readonly object _lock = new();

        public Partition(string feedRange, byte[]? initialPagingState)
        {
            FeedRange = feedRange;
            LastPagingState = initialPagingState;
            if (initialPagingState != null)
                _head = _tail = new WorkChunk { ContinuationToken = initialPagingState, IsCompleted = true };
        }

        public WorkChunk AddChunkAndTrim(byte[]? continuationToken)
        {
            var chunk = new WorkChunk { ContinuationToken = continuationToken };
            lock (_lock)
            {
                while (_head != null && _head.IsCompleted) _head = _head.Next;
                if (_head == null) _tail = null;
                if (_tail == null) _head = _tail = chunk;
                else { _tail.Next = chunk; _tail = chunk; }
            }
            return chunk;
        }

        public byte[]? GetResumeToken()
        {
            lock (_lock)
            {
                var node = _head;
                while (node != null)
                {
                    if (!node.IsCompleted) return node.ContinuationToken;
                    node = node.Next;
                }
                return _tail?.ContinuationToken;
            }
        }
    }
}
