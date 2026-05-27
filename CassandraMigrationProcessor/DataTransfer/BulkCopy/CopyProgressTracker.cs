using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System;
using System.Diagnostics;
using System.Threading;

namespace CassandraMigrationProcessor.DataTransfer.BulkCopy;
/// <summary>
/// Orchestrator for copy-pipeline progress: delegates atomic
/// counting to <see cref="ProgressCounters"/> and owns speed
/// calculation, logging, TableMigration updates, and checkpoint
/// saves. Workers call AddCopied / AddFailed / AddRead and
/// UpdateMigrationUnit; no other class should maintain
/// parallel counters.
/// </summary>
public class CopyProgressTracker
{
    private readonly MigrationLog _log;
    private readonly string _keyspace;
    private readonly string _table;
    private readonly int _workerCount;
    private readonly Stopwatch _stopwatch;

    // Atomic counters (delegated)
    private readonly ProgressCounters _counters;

    private int _activeWorkers;
    private int _peakActiveWorkers;
    private long _lastLogTicks = 0;

    // Sliding window for recent speed
    private long _windowCopied;
    private double _windowTime;
    private double _recentRowsPerSecond;

    // Pipeline state (set by writer)
    private int _activeRanges;
    private int _adaptivePageSize;

    // --- TableMigration progress (moved from ProgressState / ProgressConfig) ---
    // Tracker owns progress state updates on this unit (CopyRowsCopied, CopyPercent, chunk stats)
    private readonly TableMigration _migrationUnit;
    private readonly int _chunkIndex;
    private readonly double _initialPercent;
    private readonly double _contributionFactor;
    private readonly long _totalRowCount;
    private long _lastCheckpointTicks;
    private int _forceCheckpointFlush;

    private const int LogIntervalSeconds = 5;

    public long TotalCopied => _counters.TotalCopied;
    public long TotalFailed => _counters.TotalFailed;
    public long TotalSkipped => _counters.TotalSkipped;
    internal TableMigration MigrationUnit => _migrationUnit;

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

    public CopyProgressTracker(MigrationLog log, int workerCount,
        long initialCopied,
        TableMigration migration, ProgressConfig progressConfig)
    {
        _log = log;
        _keyspace = migration.KeyspaceName;
        _table = migration.TableName;
        _workerCount = workerCount;
        _counters = new ProgressCounters(initialCopied);
        _windowCopied = initialCopied;
        _migrationUnit = migration;
        _chunkIndex = progressConfig.ChunkIndex;
        _initialPercent = progressConfig.InitialPercent;
        _contributionFactor = progressConfig.ContributionFactor;
        _totalRowCount = progressConfig.TotalRowCount;
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
        _counters.AddCopied(count);
        LogIfDue();
    }

    /// <summary>
    /// Called by each worker after applying a replay-phase page (post
    /// bulk drain). Bumps the MU's change-feed counters and timestamps.
    /// Always force-flushes the MU on the next UpdateMigrationUnit call
    /// — CF pages are infrequent and each one is irreplaceable progress.
    /// </summary>
    public void AddReplayApplied(long count, long elapsedMs)
    {
        Interlocked.Add(ref _migrationUnit._changeFeedInsertEvents, count);
        Interlocked.Add(ref _migrationUnit._changeFeedRowsInserted, count);
        Interlocked.Add(ref _migrationUnit._changeFeedUpdatesInLastBatch, count);
        _migrationUnit.ChangeFeedLastChecked = DateTime.UtcNow;

        if (elapsedMs > 0 && count > 0)
            _migrationUnit.ChangeFeedAvgWriteLatencyInMS = (double)elapsedMs / count;

        // Force-flush on the next UpdateMigrationUnit call.
        Volatile.Write(ref _forceCheckpointFlush, 1);
    }

    /// <summary>
    /// Called by each worker after a replay-phase page returns errors.
    /// Bumps the MU's change-feed error counter.
    /// </summary>
    public void AddReplayErrors(long count)
    {
        Interlocked.Add(ref _migrationUnit._changeFeedErrors, count);
        Volatile.Write(ref _forceCheckpointFlush, 1);
    }

    /// <summary>Track data volume written.</summary>
    public void AddBytes(long bytes)
    {
        _counters.AddBytes(bytes);
    }

    /// <summary>Track a source page read duration.</summary>
    public void AddReadTime(long ms)
    {
        _counters.AddReadTime(ms);
    }

    /// <summary>Track total write batch duration.</summary>
    public void AddWriteTime(long ms, int ops)
    {
        _counters.AddWriteTime(ms, ops);
    }

    /// <summary>Set active feed range count and adaptive page size.</summary>
    public void SetPipelineState(int activeRanges, int pageSize)
    {
        Volatile.Write(ref _activeRanges, activeRanges);
        Volatile.Write(ref _adaptivePageSize, pageSize);
    }

    public void AddFailed(long count)
    {
        _counters.AddFailed(count);
    }

    /// <summary>Track source rows read.</summary>
    public void AddRead(long count)
    {
        _counters.AddRead(count);
    }

    /// <summary>
    /// Updates TableMigration fields (CopyRowsCopied, CopyPercent,
    /// CopyRowsPerSecond, chunk stats) and saves a checkpoint
    /// every <see cref="MigrationDefaults.CheckpointIntervalSeconds"/> seconds.
    /// Called from each worker's finally block after every page.
    /// </summary>
    public void UpdateMigrationUnit()
    {
        long written = TotalCopied;
        long failed = TotalFailed;
        var chunk = _migrationUnit.CopyChunks[_chunkIndex];
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
        TableMigrationMapper.UpdateParentJob(_migrationUnit);

        long prevTicks = Volatile.Read(ref _lastCheckpointTicks);
        long nowTicks = DateTime.UtcNow.Ticks;
        bool forceFlush = Interlocked.Exchange(ref _forceCheckpointFlush, 0) != 0;
        bool intervalElapsed =
            (nowTicks - prevTicks) / TimeSpan.TicksPerSecond >= MigrationDefaults.CheckpointIntervalSeconds;
        if ((forceFlush || intervalElapsed)
            && Interlocked.CompareExchange(ref _lastCheckpointTicks, nowTicks, prevTicks) == prevTicks)
        {
            MigrationJobContext.Instance.SaveMigrationUnit(_migrationUnit, true);
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

        long pages = _counters.ReadPages;
        long readTimeMs = _counters.ReadTimeMs;
        long writeTimeMs = _counters.WriteTimeMs;
        long writeOps = _counters.WriteOps;
        long avgReadMs = pages > 0 ? readTimeMs / pages : 0;
        long avgWriteMs = pages > 0 ? writeTimeMs / pages : 0;
        string avgRead = avgReadMs > 0
            ? $"{avgReadMs}ms" : "-";
        string avgWrite = avgWriteMs > 0
            ? $"{avgWriteMs}ms" : "-";

        long totalB = _counters.TotalBytes;
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
