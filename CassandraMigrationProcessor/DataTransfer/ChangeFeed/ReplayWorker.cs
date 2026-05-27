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
    /// Entry point: discovers feed ranges and dispatches to
    /// parallel or single-range processing.
    /// </summary>
    public async Task ReplayTableAsync(TableMigration mu, CancellationToken ct)
    {
        var feedRanges = await CassandraQueries.GetFeedRangesAsync(
            _sourceSession, mu.KeyspaceName, mu.TableName,
            msg => MigrationJobContext.Instance.AddVerboseLog(msg));

        _log.WriteLine(
            $"Feed ranges discovered: {feedRanges.Count} for {mu.KeyspaceName}.{mu.TableName}",
            LogType.Debug);

        if (feedRanges.Count > 1
            && mu.FeedRangeContinuationTokens.Count > 0)
        {
            _log.WriteLine(
                $"Change feed PARALLEL mode: {feedRanges.Count} ranges for {mu.KeyspaceName}.{mu.TableName}",
                LogType.Debug);

            await RunParallelAsync(mu, feedRanges, ct);
        }
        else
        {
            await RunSingleAsync(mu, ct);
        }
    }

    /// <summary>
    /// Run parallel change feed readers, one per feed range.
    /// Each range reader has its own paging state and poll loop.
    /// </summary>
    private async Task RunParallelAsync(
        TableMigration mu, List<string> feedRanges, CancellationToken ct)
    {
        mu.ChangeFeedStartedOn ??= DateTime.UtcNow;

        var (strategy, userColumns) = await PrepareReplayAsync(mu);

        int maxConcurrent = _pipelineConfig.MaxFeedRangeParallelism;
        using var semaphore = new SemaphoreSlim(maxConcurrent);

        var rangeTasks = feedRanges.Select(async range =>
        {
            await semaphore.WaitAsync(ct);
            try
            {
                await PollLoopAsync(mu, range, strategy, userColumns, ct);
            }
            finally { semaphore.Release(); }
        }).ToArray();

        await Task.WhenAll(rangeTasks);

        _log.WriteLine(
            $"Change feed PARALLEL stopped for {mu.KeyspaceName}.{mu.TableName}, ranges={feedRanges.Count}",
            LogType.Info);
    }

    /// <summary>
    /// Prepare insert statement and run the single-range poll loop.
    /// </summary>
    private async Task RunSingleAsync(TableMigration mu, CancellationToken ct)
    {
        var (strategy, userColumns) = await PrepareReplayAsync(mu);

        mu.ChangeFeedStartedOn ??= DateTime.UtcNow;

        string startTime = !string.IsNullOrEmpty(mu.ChangeFeedStartToken)
            ? mu.ChangeFeedStartToken
            : DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ",
                System.Globalization.CultureInfo.InvariantCulture);

        _log.WriteLine(
            $"Change feed started for {mu.KeyspaceName}.{mu.TableName} (startToken={startTime})",
            LogType.Info);

        await PollLoopAsync(mu, null, strategy, userColumns, ct);

        long total = Interlocked.Read(ref mu._changeFeedRowsInserted);
        _log.WriteLine(
            $"Change feed stopped for {mu.KeyspaceName}.{mu.TableName}, total applied={total}",
            LogType.Info);
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

    // ─── poll loop ──────────────────────────────────────────

    /// <summary>
    /// Unified poll loop. When <paramref name="feedRange"/> is
    /// non-null the query includes a COSMOS_FEEDRANGE() clause
    /// and the continuation token is stored per-range; otherwise
    /// single-range semantics apply.
    /// All stats use Interlocked for thread safety in both modes.
    /// </summary>
    private async Task PollLoopAsync(
        TableMigration mu,
        string? feedRange,
        IRowWriteStrategy strategy,
        List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> userColumns,
        CancellationToken ct)
    {
        try
        {
            int intervalMs = _pipelineConfig.ChangeFeedPollIntervalMs;
            int consecutiveErrors = 0;

            byte[]? continuationState = LoadContinuation(mu, feedRange);

            string startTime = !string.IsNullOrEmpty(mu.ChangeFeedStartToken)
                ? mu.ChangeFeedStartToken
                : DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ",
                    System.Globalization.CultureInfo.InvariantCulture);

            string cql = BuildCql(mu, startTime, feedRange);

            while (!ct.IsCancellationRequested && !_isCancelled())
            {
                try
                {
                    var (rs, newState) = await ReadPage(cql, continuationState);
                    continuationState = newState;

                    var sw = Stopwatch.StartNew();
                    var (insertCount, errorCount) =
                        await ReplayRows(rs, strategy, userColumns, mu, ct);
                    sw.Stop();

                    UpdateStats(mu, feedRange, insertCount, errorCount,
                        sw.ElapsedMilliseconds, continuationState);

                    consecutiveErrors = 0;
                }
                catch (OperationCanceledException) // Expected: graceful cancellation
                {
                    // Save continuation token before exiting so resume
                    // starts from the correct position (no duplicates)
                    SaveContinuation(mu, feedRange, continuationState);
                    TableMigrationMapper.UpdateParentJob(mu);
                    MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
                    break;
                }
                catch (Exception ex)
                {
                    consecutiveErrors++;

                    if (ExceptionClassifier.IsFatal(ex))
                    {
                        _log.WriteLine(
                            $"Change feed FATAL error: {ex.GetType().Name}: {ex.Message} — stopping",
                            LogType.Error);
                        break;
                    }

                    _log.WriteLine(
                        $"Change feed error ({consecutiveErrors}): {ex.GetType().Name}: {ex.Message}",
                        LogType.Warning);

                    if (feedRange == null
                        && consecutiveErrors <= MigrationDefaults.MaxReconnectAttempts
                        && ExceptionClassifier.IsTransient(ex))
                    {
                        var reconnect = await TryReconnectSourceAsync(mu, strategy, userColumns);
                        strategy = reconnect.Strategy;
                        userColumns = reconnect.UserColumns;
                    }

                    if (consecutiveErrors > MigrationDefaults.MaxReconnectAttempts)
                    {
                        _log.WriteLine(
                            $"Change feed giving up after {consecutiveErrors} errors",
                            LogType.Error);
                        break;
                    }

                    int errorDelay = Math.Min(
                        intervalMs * consecutiveErrors, 60_000);
                    if (!await DelayOrBreak(errorDelay, ct)) break;
                    continue;
                }

                if (!await DelayOrBreak(intervalMs, ct)) break;
            }

            // Always persist final continuation state when exiting
            // the loop (pause, cancel, or isCancelled flag)
            SaveContinuation(mu, feedRange, continuationState);
            TableMigrationMapper.UpdateParentJob(mu);
            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
        }
        catch (Exception ex)
        {
            string label = feedRange != null
                ? $"range {mu.KeyspaceName}.{mu.TableName}"
                : $"{mu.KeyspaceName}.{mu.TableName}";
            _log.WriteLine(
                $"CF {label}: {ex.GetType().Name}: {ex.Message}", LogType.Error);
        }
    }

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

    private static async Task<bool> DelayOrBreak(
        int ms, CancellationToken ct)
    {
        try { await Task.Delay(ms, ct); return true; }
        catch (OperationCanceledException) { return false; } // Expected: graceful cancellation
    }

    private async Task<(bool Success, IRowWriteStrategy Strategy,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> UserColumns)>
        TryReconnectSourceAsync(
            TableMigration mu,
            IRowWriteStrategy strategy,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> userColumns)
    {
        try
        {
            var job = MigrationJobContext.Instance.CurrentlyActiveJob;
            ISession newSource;
            if (_tokenRefreshManager != null
                && TokenRefreshManager.IsLikelyAadToken(job.SourcePassword))
                newSource = _tokenRefreshManager
                    .ReconnectSourceWithFreshToken();
            else
                newSource = CassandraClientFactory
                    .CreateSourceSession(_log, job, string.Empty);

            MigrationUtilities.SafeDispose(
                _sourceSession, "CF old source session");
            _sourceSession = newSource;

            var (newStrategy, newUserColumns) = await PrepareReplayAsync(mu);
            return (true, newStrategy, newUserColumns);
        }
        catch (Exception rex)
        {
            _log.WriteLine(
                $"CF reconnect failed: {rex.Message}", LogType.Warning);
            return (false, strategy, userColumns);
        }
    }
}
