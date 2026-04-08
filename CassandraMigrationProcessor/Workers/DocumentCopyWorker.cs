using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable CS8618
namespace CassandraMigrationProcessor.Workers
{
    /// <summary>
    /// Reads rows from source Cassandra table page by page
    /// and batch-inserts them into the target Cassandra table.
    /// </summary>
    public class DocumentCopyWorker
    {
        private Log _log;
        private ISession _sourceSession;
        private ISession _targetSession;
        private string _sourceKeyspace;
        private string _sourceTable;
        private string _targetKeyspace;
        private string _targetTable;
        private int _pageSize = 500;
        private long _successCount = 0;
        private long _failureCount = 0;
        private long _skippedCount = 0;

        // Sliding window speed: track recent samples
        private long _speedWindowCount = 0;
        private double _speedWindowTime = 0;

        /// <summary>
        /// Shared progress tracker for consolidated logging
        /// across parallel feed-range workers. When null,
        /// this worker logs its own progress.
        /// </summary>
        private CopyProgressTracker? _tracker;
        private long _lastReportedCopied;
        private long _lastReportedFailed;
        private long _lastReportedSkipped;

        public void Initialize(
            Log log,
            ISession sourceSession,
            ISession targetSession,
            string sourceKeyspace,
            string sourceTable,
            string targetKeyspace,
            string targetTable,
            int pageSize)
        {
            MigrationJobContext.AddVerboseLog(
                $"DocumentCopyWorker.Initialize: " +
                $"{sourceKeyspace}.{sourceTable} -> " +
                $"{targetKeyspace}.{targetTable}");

            _log = log;
            _sourceSession = sourceSession;
            _targetSession = targetSession;
            _sourceKeyspace = sourceKeyspace;
            _sourceTable = sourceTable;
            _targetKeyspace = targetKeyspace;
            _targetTable = targetTable;
            if (pageSize > 0) _pageSize = pageSize;
        }

        /// <summary>
        /// Set shared progress tracker for consolidated
        /// logging across parallel workers.
        /// </summary>
        public void SetProgressTracker(
            CopyProgressTracker tracker)
        {
            _tracker = tracker;
        }

        /// <summary>
        /// Copy all rows from source table to target table.
        /// When feedRange is provided, only copies rows from
        /// that specific physical partition.
        /// </summary>
        public async Task<TaskResult> CopyRowsAsync(
            MigrationUnit mu,
            int chunkIndex,
            double basePercent,
            double contribFactor,
            long totalRowCount,
            CancellationToken ct,
            bool isSimulated,
            string? feedRange = null)
        {
            MigrationJobContext.AddVerboseLog(
                $"DocumentCopyWorker.CopyRowsAsync: " +
                $"{_sourceKeyspace}.{_sourceTable}, " +
                $"total={totalRowCount}");

            _successCount = mu.CopyRowsCopied;
            _failureCount = 0;
            _skippedCount = 0;
            _nonRetriableErrorHit = false;

            Console.WriteLine(
                $"  CopyRowsAsync entered: {_sourceKeyspace}.{_sourceTable}, total={totalRowCount}");

            if (isSimulated)
            {
                _log.WriteLine(
                    $"Simulated: would copy {totalRowCount} rows " +
                    $"from {_sourceKeyspace}.{_sourceTable}");
                return TaskResult.Success;
            }

            try
            {
                // Ensure target table exists
                Console.WriteLine(
                    $"  Checking target table exists...");
                if (!CassandraHelper.TableExists(
                    _targetSession, _targetKeyspace, _targetTable))
                {
                    Console.WriteLine(
                        $"  Creating target table {_targetKeyspace}.{_targetTable}...");
                    CassandraHelper.EnsureKeyspaceExists(
                        _targetSession, _targetKeyspace);
                    CassandraHelper.CreateTableFromSource(
                        _sourceSession, _targetSession,
                        _sourceKeyspace, _sourceTable,
                        _targetKeyspace, _targetTable);
                    _log.WriteLine(
                        $"Created target table " +
                        $"{_targetKeyspace}.{_targetTable}");
                    Console.WriteLine(
                        $"  Target table created OK");
                }
                else
                {
                    // Table exists — sync schema
                    // (adds missing columns via ALTER)
                    Console.WriteLine(
                        $"  Target table exists — syncing schema...");
                    CassandraHelper.CreateTableFromSource(
                        _sourceSession, _targetSession,
                        _sourceKeyspace, _sourceTable,
                        _targetKeyspace, _targetTable);
                }

                // Get column metadata for prepared statement
                Console.WriteLine(
                    $"  Getting columns from source...");
                var columns = CassandraHelper.GetTableColumns(
                    _sourceSession, _sourceKeyspace, _sourceTable);
                Console.WriteLine(
                    $"  Got {columns.Count} columns");

                if (columns.Count == 0)
                {
                    Console.WriteLine(
                        $"  ERROR: No columns found!");
                    _log.WriteLine(
                        $"No columns found for " +
                        $"{_sourceKeyspace}.{_sourceTable}",
                        LogType.Error);
                    return TaskResult.Abort;
                }

                Console.WriteLine(
                    $"  Preparing INSERT statement on target...");
                var (ps, colNames) = CassandraHelper.PrepareInsert(
                    _targetSession, _targetKeyspace, _targetTable,
                    columns);
                Console.WriteLine(
                    $"  INSERT prepared OK with {colNames.Count} cols");

                // Read from source with paging
                Console.WriteLine(
                    $"  Executing SELECT on source...");
                string selectCql;
                if (!string.IsNullOrEmpty(feedRange))
                {
                    // COSMOS_FEEDRANGE requires change feed
                    selectCql =
                        $"SELECT * FROM " +
                        $"\"{_sourceKeyspace}\"" +
                        $".\"{_sourceTable}\"" +
                        $" WHERE COSMOS_CHANGEFEED_FROM_START()" +
                        $" = true" +
                        $" AND COSMOS_FEEDRANGE()" +
                        $" = '{feedRange}'";
                }
                else
                {
                    selectCql =
                        $"SELECT * FROM " +
                        $"\"{_sourceKeyspace}\"" +
                        $".\"{_sourceTable}\"";
                }

                var sw = Stopwatch.StartNew();
                byte[]? pagingState = null;
                int pageCount = 0;

                while (!ct.IsCancellationRequested)
                {
                    var statement =
                        new SimpleStatement(selectCql);
                    statement.SetPageSize(_pageSize);
                    statement.SetAutoPage(false);
                    statement.SetReadTimeoutMillis(60_000);
                    if (pagingState != null)
                        statement.SetPagingState(pagingState);

                    RowSet rs = null;
                    for (int attempt = 1; attempt <= 3; attempt++)
                    {
                        try
                        {
                            rs = await _sourceSession.ExecuteAsync(statement).ConfigureAwait(false);
                            break;
                        }
                        catch (Exception ex) when (
                            attempt < 3 &&
                            (ex is TimeoutException ||
                             ex.GetType().Name.Contains("Timeout") ||
                             ex.GetType().Name.Contains("NoHostAvailable")))
                        {
                            _log.WriteLine(
                                $"Timeout reading {_sourceKeyspace}.{_sourceTable} " +
                                $"(attempt {attempt}/3): {ex.GetType().Name}. Retrying in {attempt * 5}s...",
                                LogType.Warning);
                            await Task.Delay(attempt * 5000, ct);
                        }
                    }
                    if (rs == null)
                        throw new TimeoutException(
                            $"Failed to read from {_sourceKeyspace}.{_sourceTable} after 3 attempts");

                    pagingState = rs.PagingState;

                    var batch = new List<Row>();
                    int available =
                        rs.GetAvailableWithoutFetching();
                    int consumed = 0;
                    foreach (var row in rs)
                    {
                        if (consumed >= available) break;
                        consumed++;
                        batch.Add(row);
                    }

                    if (batch.Count == 0) break;

                    pageCount++;
                    if (pageCount <= 3 || pageCount % 10 == 0)
                    {
                        Console.WriteLine(
                            $"  Page {pageCount}: inserting {batch.Count} rows (total so far: {_successCount})...");
                    }
                    await InsertBatchAsync(
                        batch, ps, colNames, ct);

                    // Abort on non-retriable error
                    if (_nonRetriableErrorHit)
                    {
                        _log.WriteLine(
                            $"Aborting copy of {_sourceKeyspace}" +
                            $".{_sourceTable} due to " +
                            $"non-retriable error",
                            LogType.Error);
                        return TaskResult.Abort;
                    }

                    // Update progress & chunk stats
                    {
                        var chunk = mu.MigrationChunks[chunkIndex];

                        if (_tracker != null)
                        {
                            // Parallel mode: use tracker totals
                            long totalCopied = _tracker.TotalCopied;
                            long totalFailed = _tracker.TotalFailed;
                            long totalSkipped = _tracker.TotalSkipped;
                            chunk.SourceResultRowCount =
                                totalCopied + totalSkipped;
                            chunk.TargetInsertedRowCount =
                                totalCopied;
                            chunk.TargetFailedRowCount =
                                totalFailed;
                            chunk.SkippedAsDuplicateCount =
                                totalSkipped;
                            mu.CopyRowsCopied = totalCopied;
                            mu.CopyRowsPerSecond =
                                _tracker.RecentSpeed;

                            long processed = totalCopied +
                                totalSkipped;
                            if (totalRowCount > 0)
                            {
                                mu.CopyPercent = basePercent +
                                    (Math.Min(99.9,
                                        (double)processed
                                        / totalRowCount * 100)
                                    * contribFactor);
                            }
                            else if (processed > 0)
                            {
                                // No total known — show -1 as
                                // signal to UI to display rows
                                // instead of percentage
                                mu.CopyPercent = -1;
                            }
                        }
                        else
                        {
                            // Single-worker mode
                            chunk.SourceResultRowCount =
                                _successCount + _skippedCount;
                            chunk.TargetInsertedRowCount =
                                _successCount;
                            chunk.TargetFailedRowCount =
                                _failureCount;
                            chunk.SkippedAsDuplicateCount =
                                _skippedCount;
                            mu.CopyRowsCopied = _successCount;

                            // Speed — sliding window (recent 10s)
                            double elapsedSec =
                                sw.Elapsed.TotalSeconds;
                            double windowSec =
                                elapsedSec - _speedWindowTime;
                            long windowRows =
                                _successCount - _speedWindowCount;
                            if (windowSec >= 10)
                            {
                                mu.CopyRowsPerSecond = windowRows
                                    / windowSec;
                                _speedWindowCount = _successCount;
                                _speedWindowTime = elapsedSec;
                            }
                            else if (_speedWindowTime == 0
                                && elapsedSec > 0)
                            {
                                mu.CopyRowsPerSecond =
                                    _successCount / elapsedSec;
                            }

                            long processed = _successCount +
                                _skippedCount;
                            if (totalRowCount > 0)
                            {
                                mu.CopyPercent = basePercent +
                                    (Math.Min(99.9,
                                        (double)processed
                                        / totalRowCount * 100)
                                    * contribFactor);
                            }
                            else if (processed > 0)
                            {
                                mu.CopyPercent = -1;
                            }
                        }
                    }

                    // Save progress for responsive UI updates
                    {
                        if (_tracker != null)
                        {
                            // Report deltas to shared tracker
                            long newCopied = _successCount
                                - _lastReportedCopied;
                            long newFailed = _failureCount
                                - _lastReportedFailed;
                            long newSkipped = _skippedCount
                                - _lastReportedSkipped;
                            if (newCopied > 0)
                                _tracker.AddCopied(newCopied);
                            if (newFailed > 0)
                                _tracker.AddFailed(newFailed);
                            if (newSkipped > 0)
                                _tracker.AddSkipped(newSkipped);
                            _lastReportedCopied = _successCount;
                            _lastReportedFailed = _failureCount;
                            _lastReportedSkipped = _skippedCount;
                        }
                        mu.UpdateParentJob();
                        MigrationJobContext.SaveMigrationUnit(
                            mu, true);
                        // Only log from single-worker mode
                        if (_tracker == null)
                        {
                            _log.WriteLine(
                                $"Progress: {_sourceKeyspace}" +
                                $".{_sourceTable} - " +
                                $"{_successCount} rows copied, " +
                                $"{_failureCount} failed " +
                                $"({sw.Elapsed.TotalSeconds:F1}s)");
                        }
                    }

                    if (pagingState == null) break;
                }

                if (ct.IsCancellationRequested)
                {
                    _log.WriteLine(
                        $"Copy cancelled for " +
                        $"{_sourceKeyspace}.{_sourceTable}");
                    return TaskResult.Canceled;
                }

                // Final update
                var finalChunk = mu.MigrationChunks[chunkIndex];
                finalChunk.SourceResultRowCount =
                    _successCount + _skippedCount;
                finalChunk.TargetInsertedRowCount = _successCount;
                finalChunk.TargetFailedRowCount = _failureCount;

                // Update actual row count from copied data
                mu.ActualRowCount = Math.Max(
                    mu.ActualRowCount,
                    _successCount + _skippedCount);
                mu.CopyRowsCopied = _successCount;

                // Mark segments as processed
                if (finalChunk.Segments.Count == 0)
                {
                    finalChunk.Segments.Add(new Segment
                    {
                        Id = "0",
                        IsProcessed = true,
                        ResultDocCount = _successCount
                    });
                }
                else
                {
                    foreach (var seg in finalChunk.Segments)
                        seg.IsProcessed = true;
                }

                MigrationJobContext.SaveMigrationUnit(mu, true);

                _log.WriteLine(
                    $"Completed copying {_sourceKeyspace}" +
                    $".{_sourceTable}: " +
                    $"{_successCount} inserted, " +
                    $"{_failureCount} failed, " +
                    $"{_skippedCount} skipped " +
                    $"in {sw.Elapsed.TotalSeconds:F1}s");
                Console.WriteLine(
                    $"  COPY DONE: {_sourceKeyspace}.{_sourceTable}: " +
                    $"{_successCount} inserted, {_failureCount} failed " +
                    $"in {sw.Elapsed.TotalSeconds:F1}s");

                return _failureCount > 0
                    ? TaskResult.Retry : TaskResult.Success;
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine(
                    $"  COPY CANCELLED: {_sourceKeyspace}.{_sourceTable}");
                return TaskResult.Canceled;
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"  COPY ERROR: {_sourceKeyspace}.{_sourceTable}: " +
                    $"{ex.GetType().Name}: {ex.Message}");
                _log.WriteLine(
                    $"Error copying {_sourceKeyspace}" +
                    $".{_sourceTable}: {ex}",
                    LogType.Error);
                return IsRetriableError(ex)
                    ? TaskResult.Retry : TaskResult.Abort;
            }
        }

        /// <summary>
        /// Returns true if the error is retriable (429, timeout,
        /// transient network). False means job should abort.
        /// </summary>
        private static bool IsRetriableError(Exception ex)
        {
            var msg = ex.Message ?? string.Empty;
            // 429 / rate limit / overloaded
            if (msg.Contains("429")
                || msg.Contains("TooManyRequests", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("rate", StringComparison.OrdinalIgnoreCase))
                return true;
            // Timeouts
            if (ex is TimeoutException
                || ex is System.IO.IOException
                || msg.Contains("timed out", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("timeout", StringComparison.OrdinalIgnoreCase))
                return true;
            // Transient network
            if (ex is System.Net.Sockets.SocketException
                || msg.Contains("connection", StringComparison.OrdinalIgnoreCase))
                return true;
            // Cassandra overloaded / unavailable
            if (ex is Cassandra.OverloadedException
                || ex is Cassandra.WriteTimeoutException
                || ex is Cassandra.ReadTimeoutException
                || ex is Cassandra.UnavailableException)
                return true;
            return false;
        }

        private bool _nonRetriableErrorHit = false;

        private async Task InsertBatchAsync(
            List<Row> batch,
            PreparedStatement ps,
            List<string> colNames,
            CancellationToken ct)
        {
            foreach (var row in batch)
            {
                if (ct.IsCancellationRequested
                    || _nonRetriableErrorHit) break;

                try
                {
                    var values = new object[colNames.Count];
                    for (int i = 0; i < colNames.Count; i++)
                    {
                        values[i] = row[colNames[i]];
                    }

                    var bound = ps.Bind(values);
                    bound.SetReadTimeoutMillis(60_000);
                    await _targetSession.ExecuteAsync(bound).ConfigureAwait(false);
                    Interlocked.Increment(ref _successCount);
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref _failureCount);
                    _log.WriteLine(
                        $"INSERT failed on " +
                        $"{_sourceKeyspace}.{_sourceTable}: " +
                        $"{ex.GetType().Name}: {ex.Message}",
                        LogType.Error);
                    Console.WriteLine(
                        $"  INSERT FAIL [{_failureCount}]: " +
                        $"{ex.GetType().Name}: {ex.Message}");

                    if (!IsRetriableError(ex))
                    {
                        _log.WriteLine(
                            $"Non-retriable error — stopping " +
                            $"copy for {_sourceKeyspace}" +
                            $".{_sourceTable}",
                            LogType.Error);
                        _nonRetriableErrorHit = true;
                        break;
                    }
                }
            }
        }
    }
}
