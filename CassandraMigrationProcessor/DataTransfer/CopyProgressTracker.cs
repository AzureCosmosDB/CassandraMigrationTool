using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.Models;
using System.Diagnostics;

namespace CassandraMigrationProcessor.DataTransfer;
/// <summary>
/// Orchestrator for copy-pipeline progress: delegates atomic
/// counting to <see cref="ProgressCounters"/> and owns speed
/// calculation, logging, TableMigration updates, and checkpoint
/// saves. Workers call AddCopied / AddFailed and
/// UpdateMigrationUnit; no other class should maintain
/// parallel counters.
/// </summary>
public class CopyProgressTracker
{
    private readonly MigrationLog _log;
    private readonly string _keyspaceName;
    private readonly string _tableName;
    private readonly Stopwatch _stopwatch;

    // Atomic counters (delegated)
    private readonly ProgressCounters _counters;

    private long _lastLogTicks = 0;

    // Sliding window for recent speed
    private long _windowCopied;
    private double _windowTime;
    private double _recentRowsPerSecond;

    // --- TableMigration progress sink ---
    // Tracker owns progress state updates on this unit (CopyRowsCopied,
    // CopyPercent, TargetFailedRowCount).
    private readonly TableMigration _migrationUnit;
    private readonly long _totalRowCount;
    private long _lastCheckpointTicks;
    private int _forceCheckpointFlush;

    private const int LogIntervalSeconds = 5;

    public long TotalCopied => _counters.TotalCopied;
    public long TotalFailed => _counters.TotalFailed;

    private double RecentSpeed
    {
        get
        {
            if (_recentRowsPerSecond > 0) return _recentRowsPerSecond;
            double e = _stopwatch.Elapsed.TotalSeconds;
            return e > 0 ? TotalCopied / e : 0;
        }
    }

    public CopyProgressTracker(MigrationLog log,
        long initialCopied,
        TableMigration migration,
        long totalRowCount)
    {
        _log = log;
        _keyspaceName = migration.KeyspaceName;
        _tableName = migration.TableName;
        _counters = new ProgressCounters(initialCopied);
        _windowCopied = initialCopied;
        _migrationUnit = migration;
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
    public void AddWriteTime(long ms)
    {
        _counters.AddWriteTime(ms);
    }

    public void AddFailed(long count)
    {
        _counters.AddFailed(count);
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
        _migrationUnit.CopyRowsCopied = written;
        _migrationUnit.TargetFailedRowCount = failed;
        _migrationUnit.CopyRowsPerSecond = RecentSpeed;
        if (_totalRowCount > 0)
        {
            _migrationUnit.CopyPercent = Math.Min(
                MigrationDefaults.ProgressCapPercent,
                (double)written / _totalRowCount * 100);
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

        string bottleneck = avgReadMs > avgWriteMs * 2
                ? "READ-BOUND" :
            avgWriteMs > avgReadMs * 2
                ? "WRITE-BOUND" :
                  "BALANCED";

        _log.WriteLine($"Progress: {_keyspaceName}.{_tableName} {copied:N0} rows ({speedStr}, {throughput}), " + $"{failed:N0} failed ({elapsed:F1}s) | read={avgRead}/page, write={avgWrite}/page | {bottleneck}", LogType.Debug);
    }

    /// <summary>
    /// MigrationLog final summary when all workers complete.
    /// </summary>
    public void LogFinal()
    {
        long copied = TotalCopied;
        long failed = TotalFailed;
        double elapsed = _stopwatch.Elapsed.TotalSeconds;
        double rps = elapsed > 0
            ? copied / elapsed : 0;
        string speedStr = rps >= 1000
            ? $"{rps / 1000:F1}k/s" : $"{rps:F0}/s";
        _log.WriteLine($"Bulk copy done: {_keyspaceName}.{_tableName} - {copied:N0} copied, " + $"{failed:N0} failed ({elapsed:F1}s, {speedStr})", LogType.Info);
    }
}
