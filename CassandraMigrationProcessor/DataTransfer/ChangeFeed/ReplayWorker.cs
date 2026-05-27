using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.DataTransfer;
using CassandraMigrationProcessor.DataTransfer.BulkCopy;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer.ChangeFeed;
/// <summary>
/// Executes the change-feed poll loop for a single table,
/// either in single-range or parallel-range mode.
/// Created by <see cref="ReplayProcessor"/> for each table.
/// </summary>
public class ReplayWorker
{
    private readonly MigrationLog _log;
    private ISession _sourceSession;
    private readonly ISession? _targetSession;
    private readonly PipelineConfig _pipelineConfig;
    private readonly Func<bool> _isCancelled;
    private readonly TokenRefreshManager? _tokenRefreshManager;
    private int _fatalFlag;

    public ReplayWorker(
        MigrationLog log,
        ISession sourceSession,
        ISession? targetSession,
        PipelineConfig pipelineConfig,
        Func<bool> isCancelled,
        TokenRefreshManager? tokenRefreshManager = null)
    {
        _log = log;
        _sourceSession = sourceSession;
        _targetSession = targetSession;
        _pipelineConfig = pipelineConfig;
        _isCancelled = isCancelled;
        _tokenRefreshManager = tokenRefreshManager;
    }

    /// <summary>
    /// Entry point: discovers feed ranges and dispatches to the
    /// partition-pool poll loop.
    /// </summary>
    public async Task ReplayTableAsync(TableMigration mu, CancellationToken ct)
    {
        var feedRanges = await CassandraQueries.GetFeedRangesAsync(
            _sourceSession, mu.KeyspaceName, mu.TableName,
            msg => MigrationJobContext.Instance.AddVerboseLog(msg));

        _log.WriteLine(
            $"Feed ranges discovered: {feedRanges.Count} for {mu.KeyspaceName}.{mu.TableName}",
            LogType.Debug);

        bool parallel = feedRanges.Count > 1
            && mu.FeedRangeContinuationTokens.Count > 0;

        _log.WriteLine(
            $"Change feed {(parallel ? "PARALLEL" : "SINGLE")} mode: " +
            $"{(parallel ? feedRanges.Count : 1)} range(s) for " +
            $"{mu.KeyspaceName}.{mu.TableName}",
            LogType.Debug);

        await RunPoolAsync(
            mu,
            parallel ? feedRanges : new List<string?> { null }!,
            parallel ? _pipelineConfig.MaxFeedRangeParallelism : 1,
            ct);

        long total = Interlocked.Read(ref mu._changeFeedRowsInserted);
        _log.WriteLine(
            $"Change feed stopped for {mu.KeyspaceName}.{mu.TableName}, total applied={total}",
            LogType.Info);
    }

    /// <summary>
    /// A single in-flight feed-range partition: the range token (or
    /// null for single-range mode) and the paging state to resume from
    /// on the next read.
    /// </summary>
    private sealed record CfPartition(string? FeedRange, byte[]? State);

    /// <summary>
    /// Run N worker tasks against a shared partition pool channel.
    /// Mirrors the BulkCopy <see cref="WorkerPool"/> + partition-pool
    /// pattern: workers pull a partition, read one page, write rows,
    /// then re-enqueue the partition — immediately if the page returned
    /// rows (hot range), or after a cooldown if the page was empty
    /// (cold range). Any failure flips <see cref="_fatalFlag"/> and
    /// completes the channel so all workers exit cleanly. Continuation
    /// is never advanced on a failing page (matches BulkCopyWorker
    /// page-atomicity).
    /// </summary>
    private async Task RunPoolAsync(
        TableMigration mu,
        IReadOnlyList<string?> ranges,
        int workerCount,
        CancellationToken ct)
    {
        mu.ChangeFeedStartedOn ??= DateTime.UtcNow;

        var (strategy, userColumns) = await PrepareReplayAsync(mu);

        string startTime = !string.IsNullOrEmpty(mu.ChangeFeedStartToken)
            ? mu.ChangeFeedStartToken
            : DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ",
                System.Globalization.CultureInfo.InvariantCulture);

        _log.WriteLine(
            $"Change feed started for {mu.KeyspaceName}.{mu.TableName} " +
            $"(startToken={startTime}, ranges={ranges.Count}, workers={workerCount})",
            LogType.Info);

        var pool = System.Threading.Channels.Channel
            .CreateUnbounded<CfPartition>(
                new System.Threading.Channels.UnboundedChannelOptions
                {
                    SingleReader = false,
                    SingleWriter = false,
                });

        foreach (var range in ranges)
            await pool.Writer.WriteAsync(
                new CfPartition(range, LoadContinuation(mu, range)), ct);

        int actualWorkers = Math.Min(workerCount, ranges.Count);
        var workers = Enumerable.Range(0, actualWorkers)
            .Select(_ => Task.Run(() =>
                WorkerLoopAsync(mu, strategy, userColumns, pool, startTime, ct), ct))
            .ToArray();

        try { await Task.WhenAll(workers); }
        catch (OperationCanceledException) { /* graceful */ }
        catch (Exception ex)
        {
            // Task.WhenAll only rethrows the first faulted task's
            // exception; log each so we don't lose visibility.
            _log.WriteLine(
                $"CF worker faulted: {ex.GetType().Name}: {ex.Message}",
                LogType.Error);
            foreach (var t in workers.Where(t => t.IsFaulted && t.Exception != null))
                foreach (var inner in t.Exception!.Flatten().InnerExceptions
                             .Where(i => i is not OperationCanceledException))
                    _log.WriteLine(
                        $"CF worker inner: {inner.GetType().Name}: {inner.Message}",
                        LogType.Error);
        }

        // Final persist on exit — every successful page already
        // persisted its own continuation via UpdateStats; this is
        // belt-and-braces for the parent-job/MU rollup.
        TableMigrationMapper.UpdateParentJob(mu);
        MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
    }

    /// <summary>
    /// One worker iteration of the change-feed partition pool. Pulls
    /// the next partition, reads a page, writes rows, then either
    /// re-enqueues immediately (rows found) or schedules a cooldown
    /// re-enqueue (empty page). Errors short-circuit the whole pool.
    /// </summary>
    private async Task WorkerLoopAsync(
        TableMigration mu,
        IRowWriteStrategy strategy,
        List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> userColumns,
        System.Threading.Channels.Channel<CfPartition> pool,
        string startTime,
        CancellationToken ct)
    {
        int intervalMs = _pipelineConfig.ChangeFeedPollIntervalMs;

        try
        {
            while (!ct.IsCancellationRequested
                && !_isCancelled()
                && Volatile.Read(ref _fatalFlag) == 0)
            {
                CfPartition partition;
                try
                {
                    if (!await pool.Reader.WaitToReadAsync(ct)) break;
                    if (!pool.Reader.TryRead(out partition!)) continue;
                }
                catch (OperationCanceledException) { break; }

                string cql = BuildCql(mu, startTime, partition.FeedRange);

                RowSet rs;
                byte[]? newState;
                try
                {
                    (rs, newState) = await ReadPage(cql, partition.State);
                }
                catch (OperationCanceledException)
                {
                    // Re-enqueue so resume can retry this range later.
                    pool.Writer.TryWrite(partition);
                    break;
                }
                catch (Exception ex)
                {
                    _log.WriteLine(
                        $"CF FATAL read error in {mu.KeyspaceName}.{mu.TableName}" +
                        (partition.FeedRange != null ? $" range" : string.Empty) +
                        $": {ex.GetType().Name}: {ex.Message} — failing job",
                        LogType.Error);
                    Interlocked.Increment(ref mu._changeFeedErrors);
                    mu.SourceStatus = TableStatus.Failed;
                    Interlocked.Exchange(ref _fatalFlag, 1);
                    pool.Writer.TryComplete();
                    break;
                }

                var sw = Stopwatch.StartNew();
                var (insertCount, errorCount) =
                    await ReplayRows(rs, strategy, userColumns, mu, ct);
                sw.Stop();

                if (errorCount > 0)
                {
                    // ReplayRows already set _fatalFlag via onFatal.
                    // Don't advance the continuation — resume re-reads
                    // this page (mirrors BulkCopyWorker page-atomicity).
                    _log.WriteLine(
                        $"CF {errorCount}/{insertCount + errorCount} writes failed in " +
                        $"{mu.KeyspaceName}.{mu.TableName} — continuation NOT advanced, failing job",
                        LogType.Error);
                    pool.Writer.TryComplete();
                    break;
                }

                UpdateStats(mu, partition.FeedRange, insertCount, errorCount,
                    sw.ElapsedMilliseconds, newState);

                var next = partition with { State = newState };

                if (insertCount > 0)
                {
                    // Hot range: page returned rows, poll again
                    // immediately. No artificial delay — keeps up with
                    // bursty workloads.
                    if (!pool.Writer.TryWrite(next)) break;
                }
                else
                {
                    // Cold range: empty page. Sit on cooldown so we
                    // don't hammer the source. Fire-and-forget delayed
                    // re-enqueue lets the worker grab another partition
                    // from the pool in the meantime.
                    _ = ScheduleCooldownAsync(pool, next, intervalMs, ct);
                }
            }
        }
        catch (OperationCanceledException) { /* graceful */ }
        catch (Exception ex)
        {
            _log.WriteLine(
                $"CF worker FATAL in {mu.KeyspaceName}.{mu.TableName}: " +
                $"{ex.GetType().Name}: {ex.Message}",
                LogType.Error);
            Interlocked.Exchange(ref _fatalFlag, 1);
            pool.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Fire-and-forget delayed re-enqueue used for cold partitions
    /// (empty pages). Drops the partition silently on cancellation or
    /// fatal shutdown — both cases already persist the last known
    /// continuation, so the next run resumes correctly.
    /// </summary>
    private async Task ScheduleCooldownAsync(
        System.Threading.Channels.Channel<CfPartition> pool,
        CfPartition partition,
        int intervalMs,
        CancellationToken ct)
    {
        try
        {
            await Task.Delay(intervalMs, ct);
        }
        catch (OperationCanceledException) { return; }

        if (ct.IsCancellationRequested
            || _isCancelled()
            || Volatile.Read(ref _fatalFlag) != 0)
            return;

        pool.Writer.TryWrite(partition);
    }

    // ─── shared schema setup ────────────────────────────────

    /// <summary>
    /// Reads table columns from the source, filters out system columns,
    /// and constructs the per-row write strategy on the target via
    /// <see cref="RowWriteStrategyFactory"/>. Counter tables are rejected
    /// at this layer (see comment in the throw).
    /// </summary>
    private async Task<(IRowWriteStrategy Strategy,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> UserColumns)>
        PrepareReplayAsync(TableMigration mu)
    {
        var targetSession = _targetSession ?? throw new InvalidOperationException(
            "Target session is required for replay but was not provided");

        var columns = await SchemaManager.GetTableColumnsAsync(
            _sourceSession, mu.KeyspaceName, mu.TableName);
        var userColumns = columns
            .Where(c => !c.Name.StartsWith("system_", StringComparison.OrdinalIgnoreCase))
            .ToList();

        if (CassandraQueries.IsCounterTable(userColumns))
        {
            // Read-modify-write counter migration is implemented for bulk
            // copy (see CounterRowWriteStrategy) but the change-feed
            // delta semantics on a counter column are ambiguous — a CF
            // event for a counter table reports the post-update value,
            // not the delta — so we'd silently produce wrong totals.
            // Fail loud until that semantic question is resolved.
            throw new NotSupportedException(
                $"Change-feed replay is not supported for counter table " +
                $"{mu.KeyspaceName}.{mu.TableName}.");
        }

        var workerLog = new WorkerLog(_log, 0);
        var strategy = await RowWriteStrategyFactory.CreateAsync(
            workerLog, targetSession, userColumns,
            mu.GetEffectiveTargetKeyspaceName(),
            mu.GetEffectiveTargetTableName(),
            _pipelineConfig.MaxWriteRetries);
        return (strategy, userColumns);
    }

    // ─── page I/O ───────────────────────────────────────────

    /// <summary>
    /// Execute a single paged query against the source session.
    /// </summary>
    private async Task<(RowSet Rows, byte[]? PagingState)> ReadPage(
        string cql, byte[]? continuationState)
    {
        var statement = new SimpleStatement(cql);
        statement.SetPageSize(_pipelineConfig.PageSize);
        statement.SetAutoPage(false);

        if (continuationState != null)
            statement.SetPagingState(continuationState);

        var rs = await _sourceSession.ExecuteAsync(statement);
        return (rs, rs.PagingState);
    }

    /// <summary>
    /// Replay each row from the page to the target cluster via the
    /// shared <see cref="IRowWriteStrategy"/>. Returns (inserted, errors)
    /// counts derived from the strategy's <see cref="WriteCounters"/>.
    /// </summary>
    private async Task<(int InsertCount, int ErrorCount)> ReplayRows(
        RowSet rs,
        IRowWriteStrategy strategy,
        List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> userColumns,
        TableMigration mu,
        CancellationToken ct)
    {
        int available = rs.GetAvailableWithoutFetching();
        int consumed = 0;
        var counters = new WriteCounters();
        var writeTasks = new List<Task>(available);

        Action onFatal = () =>
        {
            Interlocked.Increment(ref mu._changeFeedErrors);
            mu.SourceStatus = TableStatus.Failed;
            Interlocked.Exchange(ref _fatalFlag, 1);
        };

        int rowIndex = 0;
        foreach (var row in rs)
        {
            if (consumed >= available) break;
            consumed++;
            if (ct.IsCancellationRequested || _isCancelled()) break;

            var sourceRow = new object[userColumns.Count];
            for (int i = 0; i < userColumns.Count; i++)
                sourceRow[i] = row[userColumns[i].Name];

            writeTasks.Add(strategy.WriteRowAsync(sourceRow, onFatal, counters, rowIndex++));
        }

        await Task.WhenAll(writeTasks);

        return (counters.Done, counters.Failed);
    }

    /// <summary>
    /// Update stats, persist continuation, and save the MU.
    /// </summary>
    private void UpdateStats(
        TableMigration mu,
        string? feedRange,
        int insertCount,
        int errorCount,
        long elapsedMs,
        byte[]? continuationState)
    {
        Interlocked.Add(ref mu._changeFeedInsertEvents, insertCount);
        Interlocked.Add(ref mu._changeFeedRowsInserted, insertCount);
        Interlocked.Add(ref mu._changeFeedUpdatesInLastBatch, insertCount);
        mu.ChangeFeedLastChecked = DateTime.UtcNow;

        if (elapsedMs > 0 && insertCount > 0)
        {
            mu.ChangeFeedAvgWriteLatencyInMS =
                (double)elapsedMs / insertCount;
        }

        SaveContinuation(mu, feedRange, continuationState);

        TableMigrationMapper.UpdateParentJob(mu);
        MigrationJobContext.Instance.SaveMigrationUnit(
            mu, insertCount > 0 || errorCount > 0);

        if (feedRange == null && (insertCount > 0 || errorCount > 0))
        {
            _log.WriteLine(
                $"CF {mu.KeyspaceName}.{mu.TableName}: ins={insertCount}, err={errorCount}",
                LogType.Debug);
        }
    }

    // ─── helpers ────────────────────────────────────────────

    private static string BuildCql(
        TableMigration mu, string startTime, string? feedRange)
    {
        MigrationUtilities.ValidateCqlIdentifier(mu.KeyspaceName);
        MigrationUtilities.ValidateCqlIdentifier(mu.TableName);
        string cql =
            $"SELECT * FROM \"{mu.KeyspaceName}\".\"{mu.TableName}\" WHERE COSMOS_CHANGEFEED_START_TIME() = '{startTime}'";
        if (feedRange != null)
        {
            // feedRange is a Cosmos DB token (JSON), not a CQL identifier
            cql += $" AND COSMOS_FEEDRANGE() = '{feedRange}'";
        }
        return cql;
    }

    private static byte[]? LoadContinuation(
        TableMigration mu, string? feedRange)
    {
        if (feedRange != null)
        {
            lock (mu.FeedRangeContinuationTokens)
            {
                if (mu.FeedRangeContinuationTokens.TryGetValue(
                        feedRange, out var saved)
                    && !string.IsNullOrEmpty(saved))
                {
                    return Convert.FromBase64String(saved);
                }
            }
            return null;
        }

        return !string.IsNullOrEmpty(mu.ChangeFeedContinuationToken)
            ? Convert.FromBase64String(mu.ChangeFeedContinuationToken)
            : null;
    }

    private static void SaveContinuation(
        TableMigration mu, string? feedRange, byte[]? state)
    {
        if (state == null) return;

        if (feedRange != null)
        {
            lock (mu.FeedRangeContinuationTokens)
            {
                mu.FeedRangeContinuationTokens[feedRange] =
                    Convert.ToBase64String(state);
            }
        }
        else
        {
            mu.ChangeFeedContinuationToken =
                Convert.ToBase64String(state);
        }
    }
}