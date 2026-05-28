namespace CassandraMigrationProcessor.DataTransfer;
/// <summary>
/// Thread-safe atomic counters for copy-pipeline progress.
/// Pure data class — no logging, no side effects, easily testable.
/// </summary>
internal class ProgressCounters
{
    private long _totalCopied;
    private long _totalFailed;
    private long _totalBytes;

    // Pipeline diagnostics (accumulated ms)
    private long _readTimeMs;
    private long _writeTimeMs;
    private long _readPages;
    private long _writeOps;

    public ProgressCounters(long initialCopied = 0)
    {
        _totalCopied = initialCopied;
    }

    public long TotalCopied => Volatile.Read(ref _totalCopied);
    public long TotalFailed => Volatile.Read(ref _totalFailed);
    public long TotalBytes => Volatile.Read(ref _totalBytes);
    public long ReadTimeMs => Volatile.Read(ref _readTimeMs);
    public long WriteTimeMs => Volatile.Read(ref _writeTimeMs);
    public long ReadPages => Interlocked.Read(ref _readPages);
    public long WriteOps => Volatile.Read(ref _writeOps);

    public void AddCopied(long count) => Interlocked.Add(ref _totalCopied, count);
    public void AddFailed(long count) => Interlocked.Add(ref _totalFailed, count);
    public void AddBytes(long bytes) => Interlocked.Add(ref _totalBytes, bytes);

    public void AddReadTime(long ms)
    {
        Interlocked.Add(ref _readTimeMs, ms);
        Interlocked.Increment(ref _readPages);
    }

    public void AddWriteTime(long ms, int ops)
    {
        Interlocked.Add(ref _writeTimeMs, ms);
        Interlocked.Add(ref _writeOps, ops);
    }
}
