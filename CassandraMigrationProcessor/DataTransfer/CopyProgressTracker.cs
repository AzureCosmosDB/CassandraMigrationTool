using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Diagnostics;
using System.Threading;

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Single source of truth for all copy-pipeline progress:
    /// row counts, speed, MigrationUnit field updates, and
    /// periodic checkpoint saves. Workers call AddCopied /
    /// AddFailed / AddRead and UpdateMigrationUnit; no other
    /// class should maintain parallel counters.
    /// </summary>
    public class CopyProgressTracker
    {
        private readonly MigrationLog _log;
        private readonly string _keyspace;
        private readonly string _table;
        private readonly int _workerCount;
        private readonly Stopwatch _stopwatch;

        // --- row counters (single source of truth) ---
        private long _totalCopied;
        private long _totalFailed;
        private long _totalSkipped;
        private long _totalRead;

        private int _activeWorkers;
        private int _peakActiveWorkers;
        private int _completedRanges;
        private int _totalRanges;
        private long _lastLogTicks = 0;

        // Sliding window for recent speed
        private long _windowCopied;
        private double _windowTime;
        private double _recentRowsPerSecond;

        // Data volume tracking
        private long _totalBytes;

        // Pipeline diagnostics (accumulated ms)
        private long _readTimeMs;
        private long _writeTimeMs;
        private long _readPages;
        private long _writeOps;
        private int _activeRanges; // feed ranges with pages in-flight
        private int _adaptivePageSize; // current adaptive page size

        // --- MigrationUnit progress (moved from ProgressState / ProgressConfig) ---
        private readonly MigrationUnit _migrationUnit;
        private readonly int _chunkIndex;
        private readonly double _initialPercent;
        private readonly double _contributionFactor;
        private readonly long _totalRowCount;
        private long _lastCheckpointTicks;

        private const int LogIntervalSeconds = 5;

        public long TotalCopied => Volatile.Read(ref _totalCopied);
        public long TotalFailed => Volatile.Read(ref _totalFailed);
        public long TotalSkipped => Volatile.Read(ref _totalSkipped);
        public long TotalRead => Volatile.Read(ref _totalRead);
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
                int old = Interlocked.CompareExchange(ref _peakActiveWorkers, active, peak);
                if (old == peak) break;
                peak = old;
            }
        }

        /// <summary>
        /// Call when a worker finishes a feed range.
        /// Only logs range completion — does NOT affect
        /// active worker count.
        /// </summary>
        public void RangeCompleted(string range, TaskResult result)
        {
            Interlocked.Increment(ref _completedRanges);
        }

        /// <summary>
        /// Call once when a worker thread exits.
        /// </summary>
        public void WorkerExited() => Interlocked.Decrement(ref _activeWorkers);
        public double RecentSpeed
        {
            get
            {
                if (_recentRowsPerSecond > 0) return _recentRowsPerSecond;
                double e = _stopwatch.Elapsed.TotalSeconds;
                return e > 0 ? TotalCopied / e : 0;
            }
        }

        public CopyProgressTracker(MigrationLog MigrationLog, string keyspace, string table, int workerCount, int totalRanges,
            long initialCopied,
            MigrationUnit migrationUnit, int chunkIndex,
            double initialPercent, double contributionFactor, long totalRowCount)
        {
            _log = MigrationLog;
            _keyspace = keyspace;
            _table = table;
            _workerCount = workerCount;
            _totalRanges = totalRanges;
            _totalCopied = initialCopied;
            _windowCopied = initialCopied;
            _migrationUnit = migrationUnit;
            _chunkIndex = chunkIndex;
            _initialPercent = initialPercent;
            _contributionFactor = contributionFactor;
            _totalRowCount = totalRowCount;
            _lastCheckpointTicks = DateTime.UtcNow.Ticks;
            _stopwatch = Stopwatch.StartNew();
        }

        /// <summary>
        /// Called by each worker after inserting a batch.
        /// Pass the number of NEW rows in this batch
        /// (delta, not cumulative).
        /// </summary>
        public void AddCopied(long count)
        {
            Interlocked.Add(ref _totalCopied, count);
            LogIfDue();
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

        /// <summary>Track source rows read.</summary>
        public void AddRead(long count)
        {
            Interlocked.Add(ref _totalRead, count);
        }

        /// <summary>
        /// Updates MigrationUnit fields (CopyRowsCopied, CopyPercent,
        /// CopyRowsPerSecond, chunk stats) and saves a checkpoint
        /// every <see cref="MigrationDefaults.CheckpointIntervalSeconds"/> seconds.
        /// Called from each worker's finally block after every page.
        /// </summary>
        public void UpdateMigrationUnit()
        {
            long written = TotalCopied;
            long failed = TotalFailed;
            var chunk = _migrationUnit.MigrationChunks[_chunkIndex];
            chunk.SourceResultRowCount = written;
            chunk.TargetInsertedRowCount = written;
            chunk.TargetFailedRowCount = failed;
            _migrationUnit.CopyRowsCopied = written;
            _migrationUnit.CopyRowsPerSecond = RecentSpeed;
            if (_totalRowCount > 0)
            {
                _migrationUnit.CopyPercent = _initialPercent +
                    (Math.Min(MigrationDefaults.ProgressCapPercent,
                        (double)written / _totalRowCount * 100)
                    * _contributionFactor);
            }
            _migrationUnit.UpdateParentJob();

            long prevTicks = Volatile.Read(ref _lastCheckpointTicks);
            long nowTicks = DateTime.UtcNow.Ticks;
            if ((nowTicks - prevTicks) / TimeSpan.TicksPerSecond >= MigrationDefaults.CheckpointIntervalSeconds
                && Interlocked.CompareExchange(ref _lastCheckpointTicks, nowTicks, prevTicks) == prevTicks)
            {
                MigrationJobContext.SaveMigrationUnit(_migrationUnit, true);
            }
        }

        /// <summary>
        /// Emit a periodic progress MigrationLog line if the minimum
        /// interval has elapsed since the last MigrationLog.
        /// </summary>
        private void LogIfDue()
        {
            long nowTicks = DateTime.UtcNow.Ticks;
            long prevTicks = Volatile.Read(ref _lastLogTicks);
            if ((nowTicks - prevTicks) / TimeSpan.TicksPerSecond < LogIntervalSeconds)
                return;
            if (Interlocked.CompareExchange(ref _lastLogTicks, nowTicks, prevTicks) != prevTicks)
                return;

            long copied = TotalCopied;
            long failed = TotalFailed;
            double elapsed = _stopwatch.Elapsed.TotalSeconds;

            // Recent speed (since last MigrationLog)
            double windowSec = elapsed - _windowTime;
            long windowRows = copied - _windowCopied;
            if (windowSec > 0)
                _recentRowsPerSecond = windowRows / windowSec;
            else if (elapsed > 0)
                _recentRowsPerSecond = copied / elapsed;
            _windowCopied = copied;
            _windowTime = elapsed;

            string speedStr = _recentRowsPerSecond >= 1000
                ? $"{_recentRowsPerSecond / 1000:F1}k/s"
                : $"{_recentRowsPerSecond:F0}/s";

            long pages = Interlocked.Read(ref _readPages);
            long readTimeMs = Volatile.Read(ref _readTimeMs);
            long writeTimeMs = Volatile.Read(ref _writeTimeMs);
            long writeOps = Volatile.Read(ref _writeOps);
            long avgReadMs = pages > 0 ? readTimeMs / pages : 0;
            long avgWriteMs = pages > 0 ? writeTimeMs / pages : 0;
            string avgRead = avgReadMs > 0
                ? $"{avgReadMs}ms" : "-";
            string avgWrite = avgWriteMs > 0
                ? $"{avgWriteMs}ms" : "-";

            long totalB = Volatile.Read(ref _totalBytes);
            double mbps = elapsed > 0
                ? totalB / 1024.0 / 1024.0 / elapsed : 0;
            string throughput = mbps >= 1
                ? $"{mbps:F1} MB/s" : $"{mbps * 1024:F0} KB/s";

            int ranges = Volatile.Read(ref _activeRanges);
            int pageSize = Volatile.Read(ref _adaptivePageSize);

            string bottleneck = avgReadMs > avgWriteMs * 2
                    ? "READ-BOUND" :
                avgWriteMs > avgReadMs * 2
                    ? "WRITE-BOUND" :
                      "BALANCED";

            _log.WriteLine($"Progress: {_keyspace}.{_table} [{_activeWorkers}/{_workerCount} workers, {ranges} ranges, pg={pageSize}] {copied:N0} rows ({speedStr}, {throughput}), " + $"{failed:N0} failed ({elapsed:F1}s) | read={avgRead}/page, write={avgWrite}/page | {bottleneck}", LogType.Debug);
        }

        /// <summary>
        /// MigrationLog final summary when all workers complete.
        /// </summary>
        public void LogFinal()
        {
            long copied = TotalCopied;
            long failed = TotalFailed;
            long skipped = TotalSkipped;
            double elapsed = _stopwatch.Elapsed.TotalSeconds;
            double rps = elapsed > 0
                ? copied / elapsed : 0;
            string speedStr = rps >= 1000
                ? $"{rps / 1000:F1}k/s" : $"{rps:F0}/s";
            _log.WriteLine($"Bulk copy done: {_keyspace}.{_table} [{_workerCount} workers] - {copied:N0} copied, " + $"{failed:N0} failed, {skipped:N0} skipped ({elapsed:F1}s, {speedStr}), peak active: {_peakActiveWorkers}", LogType.Info);
        }
    }
}
