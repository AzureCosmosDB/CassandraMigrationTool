using CassandraMigrationProcessor.Models;
using System;
using System.Diagnostics;
using System.Threading;

namespace CassandraMigrationProcessor.Workers
{
    /// <summary>
    /// Shared progress tracker for parallel feed-range
    /// workers. Consolidates counts from all workers
    /// and emits a single periodic log line.
    /// </summary>
    public class CopyProgressTracker
    {
        private readonly Log _log;
        private readonly string _keyspace;
        private readonly string _table;
        private readonly int _workerCount;
        private readonly Stopwatch _sw;

        private long _totalCopied;
        private long _totalFailed;
        private long _totalSkipped;
        private int _activeWorkers;
        private int _peakActiveWorkers;
        private int _completedRanges;
        private int _totalRanges;
        private DateTime _lastLogTime = DateTime.MinValue;

        // Sliding window for recent speed
        private long _windowCopied;
        private double _windowTime;
        private double _recentRps;

        // Data volume tracking
        private long _totalBytes;

        // Pipeline diagnostics (accumulated ms)
        private long _readTimeMs;
        private long _writeTimeMs;
        private long _semWaitTimeMs;
        private long _readPages;
        private long _writeOps;
        private int _semCurrent;   // current semaphore usage
        private int _activeRanges; // feed ranges with pages in-flight
        private int _adaptivePageSize; // current adaptive page size

        private const int LogIntervalSeconds = 5;

        public long TotalCopied =>
            Interlocked.Read(ref _totalCopied);
        public long TotalFailed =>
            Interlocked.Read(ref _totalFailed);
        public long TotalSkipped =>
            Interlocked.Read(ref _totalSkipped);
        public int ActiveWorkers => _activeWorkers;

        /// <summary>
        /// Call once when a worker thread starts.
        /// </summary>
        public void WorkerStarted()
        {
            int active = Interlocked.Increment(ref _activeWorkers);
            // Track peak
            int peak = _peakActiveWorkers;
            while (active > peak)
            {
                int old = Interlocked.CompareExchange(
                    ref _peakActiveWorkers, active, peak);
                if (old == peak) break;
                peak = old;
            }
        }

        /// <summary>
        /// Call when a worker finishes a feed range.
        /// Only logs range completion — does NOT affect
        /// active worker count.
        /// </summary>
        public void RangeCompleted(
            string range, TaskResult result)
        {
            Interlocked.Increment(ref _completedRanges);
        }

        /// <summary>
        /// Call once when a worker thread exits.
        /// </summary>
        public void WorkerExited() =>
            Interlocked.Decrement(ref _activeWorkers);
        public double RecentSpeed
        {
            get
            {
                if (_recentRps > 0) return _recentRps;
                double e = _sw.Elapsed.TotalSeconds;
                return e > 0 ? TotalCopied / e : 0;
            }
        }

        public CopyProgressTracker(
            Log log,
            string keyspace,
            string table,
            int workerCount,
            int totalRanges = 0)
        {
            _log = log;
            _keyspace = keyspace;
            _table = table;
            _workerCount = workerCount;
            _totalRanges = totalRanges;
            _sw = Stopwatch.StartNew();
        }

        /// <summary>
        /// Called by each worker after inserting a batch.
        /// Pass the number of NEW rows in this batch
        /// (delta, not cumulative).
        /// </summary>
        public void AddCopied(long count)
        {
            Interlocked.Add(ref _totalCopied, count);
            MaybeLog();
        }

        /// <summary>Track data volume written.</summary>
        public void AddBytes(long bytes)
        {
            Interlocked.Add(ref _totalBytes, bytes);
        }

        /// <summary>Track a source page read duration.</summary>
        public void AddReadTime(long ms)
        {
            Interlocked.Add(ref _readTimeMs, ms);
            Interlocked.Increment(ref _readPages);
        }

        /// <summary>Track total write batch duration.</summary>
        public void AddWriteTime(long ms, int ops)
        {
            Interlocked.Add(ref _writeTimeMs, ms);
            Interlocked.Add(ref _writeOps, ops);
        }

        /// <summary>Track semaphore wait duration.</summary>
        public void AddSemWaitTime(long ms)
        {
            Interlocked.Add(ref _semWaitTimeMs, ms);
        }

        /// <summary>Set current semaphore in-use count.</summary>
        public void SetSemCurrent(int current)
        {
            Volatile.Write(ref _semCurrent, current);
        }

        /// <summary>Set active feed range count and adaptive page size.</summary>
        public void SetPipelineState(int activeRanges, int pageSize)
        {
            Volatile.Write(ref _activeRanges, activeRanges);
            Volatile.Write(ref _adaptivePageSize, pageSize);
        }

        public void AddFailed(long count)
        {
            Interlocked.Add(ref _totalFailed, count);
        }

        public void AddSkipped(long count)
        {
            Interlocked.Add(ref _totalSkipped, count);
        }

        private void MaybeLog()
        {
            var now = DateTime.UtcNow;
            if ((now - _lastLogTime).TotalSeconds
                < LogIntervalSeconds)
                return;
            _lastLogTime = now;

            long copied = TotalCopied;
            long failed = TotalFailed;
            double elapsed = _sw.Elapsed.TotalSeconds;

            // Recent speed (since last log)
            double windowSec = elapsed - _windowTime;
            long windowRows = copied - _windowCopied;
            if (windowSec > 0)
                _recentRps = windowRows / windowSec;
            else if (elapsed > 0)
                _recentRps = copied / elapsed;
            _windowCopied = copied;
            _windowTime = elapsed;

            string speedStr = _recentRps >= 1000
                ? $"{_recentRps / 1000:F1}k/s"
                : $"{_recentRps:F0}/s";

            // Pipeline diagnostics
            long pages = Interlocked.Read(ref _readPages);
            long rMs = Interlocked.Read(ref _readTimeMs);
            long wMs = Interlocked.Read(ref _writeTimeMs);
            long sMs = Interlocked.Read(ref _semWaitTimeMs);
            long wOps = Interlocked.Read(ref _writeOps);
            int sem = Volatile.Read(ref _semCurrent);
            string avgRead = pages > 0
                ? $"{rMs / pages}ms" : "-";
            string avgWrite = wOps > 0
                ? $"{wMs / Math.Max(1, wOps)}ms" : "-";

            long totalB = Interlocked.Read(ref _totalBytes);
            double mbps = elapsed > 0
                ? totalB / 1024.0 / 1024.0 / elapsed : 0;
            string throughput = mbps >= 1
                ? $"{mbps:F1} MB/s" : $"{mbps * 1024:F0} KB/s";

            int ranges = Volatile.Read(ref _activeRanges);
            int pgSz = Volatile.Read(ref _adaptivePageSize);

            string bottleneck =
                sMs > rMs && sMs > wMs ? "TARGET-SATURATED" :
                rMs > wMs * 2          ? "READ-BOUND" :
                wMs > rMs * 2          ? "WRITE-BOUND" :
                                         "BALANCED";

            _log.WriteLine(
                $"Progress: {_keyspace}.{_table} " +
                $"[{_activeWorkers}/{_workerCount} workers, " +
                $"{ranges} ranges, pg={pgSz}] " +
                $"{copied:N0} rows ({speedStr}, {throughput}), " +
                $"{failed:N0} failed " +
                $"({elapsed:F1}s) | " +
                $"avg read={avgRead}/page, " +
                $"avg write={avgWrite}/row, " +
                $"sem wait={sMs:N0}ms, " +
                $"in-flight={sem} | " +
                $"{bottleneck}");
        }

        /// <summary>
        /// Log final summary when all workers complete.
        /// </summary>
        public void LogFinal()
        {
            long copied = TotalCopied;
            long failed = TotalFailed;
            long skipped = TotalSkipped;
            double elapsed = _sw.Elapsed.TotalSeconds;
            double rps = elapsed > 0
                ? copied / elapsed : 0;
            string speedStr = rps >= 1000
                ? $"{rps / 1000:F1}k/s" : $"{rps:F0}/s";
            _log.WriteLine(
                $"Bulk copy done: " +
                $"{_keyspace}.{_table} " +
                $"[{_workerCount} workers] - " +
                $"{copied:N0} copied, " +
                $"{failed:N0} failed, " +
                $"{skipped:N0} skipped " +
                $"({elapsed:F1}s, {speedStr}), " +
                $"peak active: {_peakActiveWorkers}");
        }
    }
}
