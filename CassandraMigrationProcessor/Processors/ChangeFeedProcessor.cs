using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Helpers.JobManagement;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Workers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Processors
{
    /// <summary>
    /// Tails the Cosmos DB Cassandra change feed for one or
    /// more tables and replicates changes to the target OSS
    /// Cassandra cluster.
    ///
    /// FFCF (Full Fidelity Change Feed) mode uses CQL syntax:
    ///   SELECT JSON * FROM ks.tbl
    ///     WHERE COSMOS_CHANGEFEED_FROM_START() = false
    ///       AND COSMOS_FULLFIDELITY_CHANGEFEED() = true
    ///
    /// Returns JSON rows with __sys_metadata.operationType
    /// (create/replace) and tombstone markers for deletes.
    /// SetAutoPage(false) is critical to avoid long-poll hang.
    /// PagingState acts as the continuation token.
    ///
    /// Legacy mode uses custom payload keys and SELECT *.
    /// </summary>
    public class ChangeFeedProcessor
    {
        private readonly Log _log;
        private ISession _sourceSession;
        private ISession? _targetSession;
        private readonly ActiveMigrationUnitsCache _muCache;
        private readonly MigrationSettings _config;
        private readonly bool _singleTable;
        private readonly MigrationWorker? _migrationWorker;

        private readonly ConcurrentQueue<string> _pendingTables =
            new();
        private readonly ConcurrentDictionary<string, Task>
            _activeTasks = new();

        public bool ExecutionCancelled { get; set; }

        public ChangeFeedProcessor(
            Log log,
            ISession sourceSession,
            ISession targetSession,
            ActiveMigrationUnitsCache muCache,
            MigrationSettings config,
            bool singleTable = true,
            MigrationWorker? migrationWorker = null)
        {
            _log = log;
            _sourceSession = sourceSession;
            _targetSession = targetSession;
            _muCache = muCache;
            _config = config;
            _singleTable = singleTable;
            _migrationWorker = migrationWorker;
        }

        /// <summary>
        /// Enqueue a single table for change-feed processing.
        /// </summary>
        public void AddTableToProcess(
            string migrationUnitId,
            CancellationTokenSource cts)
        {
            MigrationJobContext.AddVerboseLog(
                $"ChangeFeedProcessor.AddTableToProcess: " +
                $"mu={migrationUnitId}");
            _pendingTables.Enqueue(migrationUnitId);
            StartPendingTables(cts);
        }

        /// <summary>
        /// Start change-feed polling for all completed tables.
        /// </summary>
        public void RunChangeFeedForAllTables(
            CancellationTokenSource cts)
        {
            MigrationJobContext.AddVerboseLog(
                "ChangeFeedProcessor.RunChangeFeedForAllTables");

            var job = MigrationJobContext.CurrentlyActiveJob;
            if (job?.MigrationUnitBasics == null) return;

            foreach (var mub in job.MigrationUnitBasics)
            {
                if (!Helper.IsMigrationUnitValid(mub)) continue;
                if (!mub.CopyComplete) continue;
                if (!_activeTasks.ContainsKey(mub.Id))
                    _pendingTables.Enqueue(mub.Id);
            }

            StartPendingTables(cts);
        }

        private void StartPendingTables(
            CancellationTokenSource cts)
        {
            while (_pendingTables.TryDequeue(out var muId))
            {
                if (_activeTasks.ContainsKey(muId)) continue;
                if (ExecutionCancelled) break;

                var task = Task.Run(
                    () => PollLoopAsync(muId, cts.Token));
                _activeTasks[muId] = task;
            }
        }

        private async Task PollLoopAsync(
            string muId, CancellationToken ct)
        {
          try
          {
            Console.WriteLine($"  CF PollLoop entering for muId={muId}");
            var mu = _muCache.GetMigrationUnit(muId);
            if (mu == null)
            {
                Console.WriteLine(
                    $"  CF PollLoop: MU {muId} not found in cache!");
                _log.WriteLine(
                    $"ChangeFeed: MU {muId} not found",
                    LogType.Error);
                return;
            }

            Console.WriteLine(
                $"  CF PollLoop: MU found: {mu.KeyspaceName}.{mu.TableName}");

            bool useFullFidelity = _config.ChangeFeedFullFidelity;

            // Discover feed ranges for parallel processing
            var feedRanges = CassandraHelper.GetFeedRanges(
                _sourceSession, mu.KeyspaceName, mu.TableName);

            _log.WriteLine(
                $"Feed ranges discovered: {feedRanges.Count} " +
                $"for {mu.KeyspaceName}.{mu.TableName}");
            Console.WriteLine(
                $"  CF: {feedRanges.Count} feed ranges " +
                $"for {mu.KeyspaceName}.{mu.TableName}");

            if (feedRanges.Count > 1
                && mu.FeedRangeContinuationTokens != null)
            {
                // PARALLEL: multiple feed ranges
                _log.WriteLine(
                    $"Change feed PARALLEL mode: " +
                    $"{feedRanges.Count} ranges for " +
                    $"{mu.KeyspaceName}.{mu.TableName}");

                await PollLoopParallelAsync(
                    mu, feedRanges, ct);
            }
            else
            {
                // SINGLE: one range or no feed ranges
                await PollLoopSingleAsync(mu, null, ct);
            }
          }
          catch (Exception ex)
          {
            Console.WriteLine(
                $"  CF FATAL: muId={muId}: {ex.GetType().Name}: {ex.Message}");
            Console.WriteLine($"  CF FATAL stack: {ex.StackTrace}");
          }

            _activeTasks.TryRemove(muId, out _);
        }

        /// <summary>
        /// Run parallel change feed readers, one per feed range.
        /// Each range reader has its own paging state and poll
        /// loop. Stats are aggregated to the MU using Interlocked.
        /// </summary>
        private async Task PollLoopParallelAsync(
            MigrationUnit mu,
            List<string> feedRanges,
            CancellationToken ct)
        {
            mu.ChangeFeedStartedOn ??= DateTime.UtcNow;

            var columns = CassandraHelper.GetTableColumns(
                _sourceSession, mu.KeyspaceName, mu.TableName);
            var userColumns = columns
                .Where(c => !c.Name.StartsWith(
                    "system_", StringComparison.OrdinalIgnoreCase))
                .ToList();
            var pkColumns = columns
                .Where(c => c.Kind == "partition_key"
                         || c.Kind == "clustering")
                .ToList();

            var (ps, colNames) = CassandraHelper.PrepareInsert(
                _targetSession!,
                mu.GetEffectiveTargetKeyspaceName(),
                mu.GetEffectiveTargetTableName(),
                userColumns);

            bool useFullFidelity = _config.ChangeFeedFullFidelity;
            PreparedStatement? deletePs = null;
            List<string>? deletePkNames = null;
            if (useFullFidelity && pkColumns.Count > 0)
            {
                try
                {
                    var delResult = CassandraHelper.PrepareDelete(
                        _targetSession!,
                        mu.GetEffectiveTargetKeyspaceName(),
                        mu.GetEffectiveTargetTableName(),
                        userColumns);
                    deletePs = delResult.Ps;
                    deletePkNames = delResult.PkColumnNames;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(
                        $"  CF: PrepareDelete failed: {ex.Message}");
                }
            }

            int maxConcurrent = Math.Max(1,
                MigrationJobContext.CurrentlyActiveJob
                    .MaxFeedRangeParallelism);

            Console.WriteLine(
                $"  CF PARALLEL STARTED: " +
                $"{mu.KeyspaceName}.{mu.TableName} " +
                $"{feedRanges.Count} ranges, FFCF={useFullFidelity}" +
                $", maxConcurrent={maxConcurrent}");

            // Throttle concurrent range tasks to avoid
            // thread pool starvation on small App Service plans.
            var semaphore = new SemaphoreSlim(maxConcurrent);
            var rangeTasks = feedRanges.Select(async range =>
            {
                await semaphore.WaitAsync(ct);
                try
                {
                    await PollRangeLoopAsync(
                        mu, range, columns, userColumns, pkColumns,
                        ps, colNames, deletePs, deletePkNames, ct);
                }
                finally { semaphore.Release(); }
            }).ToArray();

            await Task.WhenAll(rangeTasks);

            _log.WriteLine(
                $"Change feed PARALLEL stopped for " +
                $"{mu.KeyspaceName}.{mu.TableName}, " +
                $"ranges={feedRanges.Count}");
        }

        /// <summary>
        /// Poll loop for a single feed range within a table.
        /// Updates MU counters with Interlocked for thread safety.
        /// </summary>
        private async Task PollRangeLoopAsync(
            MigrationUnit mu,
            string feedRange,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> columns,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> userColumns,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> pkColumns,
            PreparedStatement ps,
            List<string> colNames,
            PreparedStatement? deletePs,
            List<string>? deletePkNames,
            CancellationToken ct)
        {
          try
          {
            int intervalMs =
                _config.ChangeFeedPollIntervalMs > 0
                    ? _config.ChangeFeedPollIntervalMs
                    : 5000;
            bool useFullFidelity = _config.ChangeFeedFullFidelity;
            int consecutiveErrors = 0;
            const int MaxReconnectAttempts = 3;

            // Per-range continuation state
            byte[]? continuationState = null;
            if (mu.FeedRangeContinuationTokens != null
                && mu.FeedRangeContinuationTokens.TryGetValue(
                    feedRange, out var saved)
                && !string.IsNullOrEmpty(saved))
            {
                continuationState =
                    Convert.FromBase64String(saved);
            }

            // Build CQL with COSMOS_FEEDRANGE()
            string cql;
            if (useFullFidelity)
            {
                cql =
                    $"SELECT JSON * FROM " +
                    $"\"{mu.KeyspaceName}\".\"{mu.TableName}\"" +
                    $" WHERE COSMOS_CHANGEFEED_FROM_START()" +
                    $" = false" +
                    $" AND COSMOS_FULLFIDELITY_CHANGEFEED()" +
                    $" = true" +
                    $" AND COSMOS_FEEDRANGE() = '{feedRange}'";
            }
            else
            {
                string startTime =
                    !string.IsNullOrEmpty(mu.ChangeFeedStartToken)
                        ? mu.ChangeFeedStartToken
                        : DateTime.UtcNow.ToString(
                            "yyyy-MM-ddTHH:mm:ss.fffZ",
                            System.Globalization.CultureInfo
                                .InvariantCulture);
                cql =
                    $"SELECT * FROM " +
                    $"\"{mu.KeyspaceName}\".\"{mu.TableName}\"" +
                    $" WHERE COSMOS_CHANGEFEED_START_TIME() = " +
                    $"'{startTime}'" +
                    $" AND COSMOS_FEEDRANGE() = '{feedRange}'";
            }

            var rangeLabel = feedRange.Length > 40
                ? feedRange.Substring(0, 40) + "..."
                : feedRange;
            Console.WriteLine(
                $"  CF RANGE: {mu.KeyspaceName}.{mu.TableName} " +
                $"range={rangeLabel}");

            long rangeTotalApplied = 0;

            while (!ct.IsCancellationRequested
                && !ExecutionCancelled)
            {
                try
                {
                    var statement = new SimpleStatement(cql);
                    statement.SetPageSize(
                        _config.CqlCopyPageSize > 0
                            ? _config.CqlCopyPageSize : 500);
                    statement.SetAutoPage(false);

                    if (continuationState != null)
                        statement.SetPagingState(
                            continuationState);

                    var rs = await _sourceSession.ExecuteAsync(statement)
                        .ConfigureAwait(false);

                    continuationState = rs.PagingState;

                    int insertCount = 0;
                    int updateCount = 0;
                    int deleteCount = 0;
                    int errorCount = 0;

                    int available =
                        rs.GetAvailableWithoutFetching();
                    int consumed = 0;
                    foreach (var row in rs)
                    {
                        if (consumed >= available) break;
                        consumed++;
                        if (ct.IsCancellationRequested
                            || ExecutionCancelled) break;

                        try
                        {
                            if (useFullFidelity)
                            {
                                ProcessFfcfRow(
                                    row, mu, ps, colNames,
                                    deletePs, deletePkNames,
                                    userColumns, pkColumns,
                                    ref insertCount,
                                    ref updateCount,
                                    ref deleteCount);
                            }
                            else
                            {
                                var values =
                                    new object[colNames.Count];
                                for (int i = 0;
                                    i < colNames.Count; i++)
                                    values[i] = row[colNames[i]];
                                await _targetSession!.ExecuteAsync(
                                    ps.Bind(values))
                                    .ConfigureAwait(false);
                                insertCount++;
                            }
                            rangeTotalApplied++;
                        }
                        catch (Exception ex)
                        {
                            errorCount++;
                            Interlocked.Increment(
                                ref mu._changeFeedErrors);
                            MigrationJobContext.AddVerboseLog(
                                $"CF apply fail: {ex.Message}");
                        }
                    }

                    // Aggregate to MU with Interlocked
                    Interlocked.Add(
                        ref mu._changeFeedInsertEvents,
                        insertCount);
                    Interlocked.Add(
                        ref mu._changeFeedRowsInserted,
                        insertCount);
                    Interlocked.Add(
                        ref mu._changeFeedUpdateEvents,
                        updateCount);
                    Interlocked.Add(
                        ref mu._changeFeedRowsUpdated,
                        updateCount);
                    Interlocked.Add(
                        ref mu._changeFeedDeleteEvents,
                        deleteCount);
                    Interlocked.Add(
                        ref mu._changeFeedRowsDeleted,
                        deleteCount);

                    int batchTotal =
                        insertCount + updateCount + deleteCount;
                    mu.ChangeFeedUpdatesInLastBatch = batchTotal;
                    mu.ChangeFeedLastChecked = DateTime.UtcNow;

                    // Save continuation AFTER writes so crash
                    // doesn't skip unwritten rows
                    if (continuationState != null
                        && mu.FeedRangeContinuationTokens != null)
                    {
                        lock (mu.FeedRangeContinuationTokens)
                        {
                            mu.FeedRangeContinuationTokens[
                                feedRange] = Convert
                                .ToBase64String(continuationState);
                        }
                    }

                    if (batchTotal > 0 || errorCount > 0)
                    {
                        mu.UpdateParentJob();
                        MigrationJobContext.SaveMigrationUnit(
                            mu, true);
                    }
                    else
                    {
                        MigrationJobContext.SaveMigrationUnit(
                            mu, false);
                    }

                    consecutiveErrors = 0;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    consecutiveErrors++;
                    Console.WriteLine(
                        $"  CF RANGE ERROR [{consecutiveErrors}]: " +
                        $"{mu.KeyspaceName}.{mu.TableName} " +
                        $"range={rangeLabel}: " +
                        $"{ex.GetType().Name}: {ex.Message}");

                    if (consecutiveErrors > MaxReconnectAttempts)
                    {
                        Console.WriteLine(
                            $"  CF RANGE GIVING UP " +
                            $"after {consecutiveErrors} errors");
                        break;
                    }
                }

                try { await Task.Delay(intervalMs, ct); }
                catch (OperationCanceledException) { break; }
            }

            Console.WriteLine(
                $"  CF RANGE DONE: {mu.KeyspaceName}" +
                $".{mu.TableName} range={rangeLabel} " +
                $"applied={rangeTotalApplied}");
          }
          catch (Exception ex)
          {
            Console.WriteLine(
                $"  CF RANGE FATAL: {mu.KeyspaceName}" +
                $".{mu.TableName}: {ex.GetType().Name}: " +
                $"{ex.Message}");
          }
        }

        /// <summary>
        /// Process a single FFCF row (shared by single and
        /// parallel paths). Handles insert/replace/delete.
        /// </summary>
        private void ProcessFfcfRow(
            Row row,
            MigrationUnit mu,
            PreparedStatement ps,
            List<string> colNames,
            PreparedStatement? deletePs,
            List<string>? deletePkNames,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> userColumns,
            List<(string Name, string Type, string Kind, string ClusteringOrder, int Position)> pkColumns,
            ref int insertCount,
            ref int updateCount,
            ref int deleteCount)
        {
            var json = row.GetValue<string>("[json]");
            var doc = System.Text.Json.JsonDocument.Parse(json);
            var root = doc.RootElement;

            string opType;
            if (root.TryGetProperty("__sys_metadata", out var sysMeta)
                && sysMeta.ValueKind != JsonValueKind.Null)
            {
                opType = sysMeta
                    .GetProperty("operationType")
                    .GetString() ?? "create";
            }
            else
            {
                var snippet = json.Length > 200
                    ? json.Substring(0, 200) : json;
                _log.WriteLine(
                    "ChangeFeed: __sys_metadata missing. JSON: "
                    + snippet, LogType.Error);
                throw new InvalidOperationException(
                    "FFCF document missing __sys_metadata.");
            }

            bool isRowDelete = false;
            if (root.TryGetProperty(
                "__sys_rw_tmbstn", out var rwTombstone))
            {
                isRowDelete = rwTombstone.ValueKind
                    != JsonValueKind.Null;
            }
            else
            {
                var snippet = json.Length > 200
                    ? json.Substring(0, 200) : json;
                _log.WriteLine(
                    "ChangeFeed: __sys_rw_tmbstn missing. JSON: "
                    + snippet, LogType.Error);
                throw new InvalidOperationException(
                    "FFCF document missing __sys_rw_tmbstn.");
            }

            if (isRowDelete
                && deletePs != null
                && deletePkNames != null)
            {
                var pkValues =
                    new object[deletePkNames.Count];
                for (int i = 0; i < deletePkNames.Count; i++)
                {
                    pkValues[i] = ExtractJsonValue(
                        root, deletePkNames[i],
                        pkColumns.First(c =>
                            c.Name == deletePkNames[i]).Type);
                }
                _targetSession!.ExecuteAsync(
                    deletePs.Bind(pkValues))
                    .GetAwaiter().GetResult();
                deleteCount++;
            }
            else
            {
                var values = new object[colNames.Count];
                for (int i = 0; i < colNames.Count; i++)
                {
                    values[i] = ExtractJsonValue(
                        root, colNames[i],
                        userColumns.First(c =>
                            c.Name == colNames[i]).Type);
                }
                _targetSession!.ExecuteAsync(ps.Bind(values))
                    .GetAwaiter().GetResult();
                if (opType == "replace")
                    updateCount++;
                else
                    insertCount++;
            }
        }

        /// <summary>
        /// Single-range poll loop (original behavior).
        /// Also used when feed range is explicitly provided.
        /// </summary>
        private async Task PollLoopSingleAsync(
            MigrationUnit mu,
            string? feedRange,
            CancellationToken ct)
        {
          try
          {
            int intervalMs =
                _config.ChangeFeedPollIntervalMs > 0
                    ? _config.ChangeFeedPollIntervalMs
                    : 5000;

            int consecutiveErrors = 0;
            const int MaxReconnectAttempts = 3;
            bool useFullFidelity = _config.ChangeFeedFullFidelity;

            byte[]? continuationState =
                !string.IsNullOrEmpty(
                    mu.ChangeFeedContinuationToken)
                    ? Convert.FromBase64String(
                        mu.ChangeFeedContinuationToken)
                    : null;

            string startTime =
                !string.IsNullOrEmpty(mu.ChangeFeedStartToken)
                    ? mu.ChangeFeedStartToken
                    : DateTime.UtcNow.ToString(
                        "yyyy-MM-ddTHH:mm:ss.fffZ",
                        System.Globalization.CultureInfo
                            .InvariantCulture);

            // Build CQL for change-feed query
            string cql;
            if (useFullFidelity)
            {
                cql =
                    $"SELECT JSON * FROM " +
                    $"\"{mu.KeyspaceName}\".\"{mu.TableName}\"" +
                    $" WHERE COSMOS_CHANGEFEED_FROM_START()" +
                    $" = false" +
                    $" AND COSMOS_FULLFIDELITY_CHANGEFEED()" +
                    $" = true";
            }
            else
            {
                cql =
                    $"SELECT * FROM " +
                    $"\"{mu.KeyspaceName}\".\"{mu.TableName}\"" +
                    $" where COSMOS_CHANGEFEED_START_TIME() = " +
                    $"'{startTime}'";
            }

            Console.WriteLine(
                $"  CF PollLoop: Getting columns from source...");
            var columns = CassandraHelper.GetTableColumns(
                _sourceSession, mu.KeyspaceName, mu.TableName);

            var userColumns = columns
                .Where(c => !c.Name.StartsWith(
                    "system_", StringComparison.OrdinalIgnoreCase))
                .ToList();
            var pkColumns = columns
                .Where(c => c.Kind == "partition_key"
                         || c.Kind == "clustering")
                .ToList();

            var (ps, colNames) = CassandraHelper.PrepareInsert(
                _targetSession!,
                mu.GetEffectiveTargetKeyspaceName(),
                mu.GetEffectiveTargetTableName(),
                userColumns);

            PreparedStatement? deletePs = null;
            List<string>? deletePkNames = null;
            if (useFullFidelity && pkColumns.Count > 0)
            {
                try
                {
                    var delResult = CassandraHelper.PrepareDelete(
                        _targetSession!,
                        mu.GetEffectiveTargetKeyspaceName(),
                        mu.GetEffectiveTargetTableName(),
                        userColumns);
                    deletePs = delResult.Ps;
                    deletePkNames = delResult.PkColumnNames;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(
                        $"  CF: PrepareDelete failed: {ex.Message}");
                    _log.WriteLine(
                        $"CF PrepareDelete failed: {ex.Message}",
                        LogType.Warning);
                }
            }

            mu.ChangeFeedStartedOn ??= DateTime.UtcNow;
            long totalApplied = 0;

            Console.WriteLine(
                $"  CF STARTED: {mu.KeyspaceName}.{mu.TableName} " +
                $"fullFidelity={useFullFidelity} " +
                $"startToken={startTime} " +
                $"pollMs={intervalMs} " +
                $"hasContinuation={continuationState != null}");

            _log.WriteLine(
                $"Change feed started for " +
                $"{mu.KeyspaceName}.{mu.TableName} " +
                $"(FFCF={useFullFidelity}, " +
                $"startToken={startTime})");

            while (!ct.IsCancellationRequested
                && !ExecutionCancelled)
            {
                try
                {
                    var statement = new SimpleStatement(cql);
                    statement.SetPageSize(
                        _config.CqlCopyPageSize > 0
                            ? _config.CqlCopyPageSize : 500);
                    statement.SetAutoPage(false);

                    if (continuationState != null)
                        statement.SetPagingState(
                            continuationState);

                    var rs = await _sourceSession.ExecuteAsync(statement)
                        .ConfigureAwait(false);

                    continuationState = rs.PagingState;
                    if (continuationState != null)
                    {
                        mu.ChangeFeedContinuationToken =
                            Convert.ToBase64String(
                                continuationState);
                    }

                    var sw = Stopwatch.StartNew();
                    int insertCount = 0;
                    int updateCount = 0;
                    int deleteCount = 0;
                    int errorCount = 0;

                    int available =
                        rs.GetAvailableWithoutFetching();
                    int consumed = 0;
                    foreach (var row in rs)
                    {
                        if (consumed >= available) break;
                        consumed++;
                        if (ct.IsCancellationRequested
                            || ExecutionCancelled) break;

                        try
                        {
                            if (useFullFidelity)
                            {
                                ProcessFfcfRow(
                                    row, mu, ps, colNames,
                                    deletePs, deletePkNames,
                                    userColumns, pkColumns,
                                    ref insertCount,
                                    ref updateCount,
                                    ref deleteCount);
                            }
                            else
                            {
                                var values =
                                    new object[colNames.Count];
                                for (int i = 0;
                                    i < colNames.Count; i++)
                                    values[i] = row[colNames[i]];
                                await _targetSession!.ExecuteAsync(
                                    ps.Bind(values))
                                    .ConfigureAwait(false);
                                insertCount++;
                            }
                            totalApplied++;
                        }
                        catch (Exception ex)
                        {
                            errorCount++;
                            mu.ChangeFeedErrors++;
                            MigrationJobContext.AddVerboseLog(
                                $"CF apply fail: {ex.Message}");
                        }
                    }

                    mu.ChangeFeedInsertEvents += insertCount;
                    mu.ChangeFeedRowsInserted += insertCount;
                    mu.ChangeFeedUpdateEvents += updateCount;
                    mu.ChangeFeedRowsUpdated += updateCount;
                    mu.ChangeFeedDeleteEvents += deleteCount;
                    mu.ChangeFeedRowsDeleted += deleteCount;
                    mu.ChangeFeedUpdatesInLastBatch =
                        insertCount + updateCount + deleteCount;
                    mu.ChangeFeedLastChecked = DateTime.UtcNow;

                    int batchTotal =
                        insertCount + updateCount + deleteCount;
                    if (sw.ElapsedMilliseconds > 0
                        && batchTotal > 0)
                    {
                        mu.ChangeFeedAvgWriteLatencyInMS =
                            (double)sw.ElapsedMilliseconds
                            / batchTotal;
                    }

                    if (batchTotal > 0 || errorCount > 0)
                    {
                        mu.UpdateParentJob();
                        MigrationJobContext.SaveMigrationUnit(
                            mu, true);

                        Console.WriteLine(
                            $"  CF {mu.KeyspaceName}.{mu.TableName}: " +
                            $"ins={insertCount}, upd={updateCount}, " +
                            $"del={deleteCount}, err={errorCount}, " +
                            $"total={totalApplied}");
                        _log.WriteLine(
                            $"CF {mu.KeyspaceName}" +
                            $".{mu.TableName}: ins=" +
                            $"{insertCount}, upd={updateCount}," +
                            $" del={deleteCount}," +
                            $" err={errorCount}," +
                            $" total={totalApplied}");
                    }
                    else
                    {
                        MigrationJobContext.SaveMigrationUnit(
                            mu, false);
                    }

                    consecutiveErrors = 0;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    consecutiveErrors++;
                    Console.WriteLine(
                        $"  CF ERROR [{consecutiveErrors}]: " +
                        $"{mu.KeyspaceName}.{mu.TableName}: " +
                        $"{ex.GetType().Name}: {ex.Message}");
                    _log.WriteLine(
                        $"CF error {mu.KeyspaceName}" +
                        $".{mu.TableName}: {ex.Message}",
                        LogType.Error);

                    if (consecutiveErrors <= MaxReconnectAttempts
                        && (ex.GetType().Name.Contains("NoHost")
                            || ex.GetType().Name.Contains("Socket")
                            || ex.GetType().Name.Contains("Auth")
                            || ex.Message.Contains("disposed")
                            || ex.Message.Contains("unauthorized")
                            || ex.Message.Contains("401")))
                    {
                        try
                        {
                            Console.WriteLine(
                                $"  CF RECONNECT: rebuilding...");
                            var job = MigrationJobContext
                                .CurrentlyActiveJob;
                            ISession newSource;
                            if (CassandraClientFactory
                                .IsLikelyAadToken(
                                    job.SourcePassword))
                            {
                                newSource = CassandraClientFactory
                                    .ReconnectSourceWithFreshToken(
                                        _log);
                            }
                            else
                            {
                                newSource = CassandraClientFactory
                                    .CreateSourceSession(
                                        _log, job, string.Empty);
                            }
                            _sourceSession = newSource;
                            columns = CassandraHelper
                                .GetTableColumns(
                                    _sourceSession,
                                    mu.KeyspaceName,
                                    mu.TableName);
                            userColumns = columns
                                .Where(c => !c.Name.StartsWith(
                                    "system_",
                                    StringComparison
                                        .OrdinalIgnoreCase))
                                .ToList();
                            var newInsert = CassandraHelper
                                .PrepareInsert(
                                    _targetSession!,
                                    mu.GetEffectiveTargetKeyspaceName(),
                                    mu.GetEffectiveTargetTableName(),
                                    userColumns);
                            ps = newInsert.Ps;
                            colNames = newInsert.ColumnNames;
                            if (useFullFidelity)
                            {
                                try
                                {
                                    var delResult = CassandraHelper
                                        .PrepareDelete(
                                            _targetSession!,
                                            mu.GetEffectiveTargetKeyspaceName(),
                                            mu.GetEffectiveTargetTableName(),
                                            userColumns);
                                    deletePs = delResult.Ps;
                                    deletePkNames =
                                        delResult.PkColumnNames;
                                }
                                catch { /* best-effort */ }
                            }
                            Console.WriteLine(
                                $"  CF RECONNECT: success");
                        }
                        catch (Exception rex)
                        {
                            Console.WriteLine(
                                $"  CF RECONNECT FAILED: " +
                                $"{rex.Message}");
                        }
                    }
                    else if (consecutiveErrors
                        > MaxReconnectAttempts)
                    {
                        Console.WriteLine(
                            $"  CF GIVING UP after " +
                            $"{consecutiveErrors} errors");
                        break;
                    }
                }

                try { await Task.Delay(intervalMs, ct); }
                catch (OperationCanceledException) { break; }
            }

            _log.WriteLine(
                $"Change feed stopped for " +
                $"{mu.KeyspaceName}.{mu.TableName}, " +
                $"total applied={totalApplied}");

          }
          catch (Exception ex)
          {
            Console.WriteLine(
                $"  CF FATAL: {mu.KeyspaceName}.{mu.TableName}: " +
                $"{ex.GetType().Name}: {ex.Message}");
            Console.WriteLine(
                $"  CF FATAL stack: {ex.StackTrace}");
          }
        }

        /// <summary>
        /// Extract a typed value from a JSON element based on
        /// the Cassandra column type.
        /// </summary>
        private static object ExtractJsonValue(
            JsonElement root,
            string columnName,
            string cassandraType)
        {
            if (!root.TryGetProperty(columnName, out var el)
                || el.ValueKind == JsonValueKind.Null)
                return null!;

            var lowerType = cassandraType.ToLowerInvariant();

            // Scalar types
            if (lowerType == "uuid" || lowerType == "timeuuid")
                return Guid.Parse(el.GetString()!);
            if (lowerType == "int")
                return el.GetInt32();
            if (lowerType == "bigint" || lowerType == "counter")
                return el.GetInt64();
            if (lowerType == "smallint")
                return (short)el.GetInt32();
            if (lowerType == "tinyint")
                return (sbyte)el.GetInt32();
            if (lowerType == "float")
                return el.GetSingle();
            if (lowerType == "double")
                return el.GetDouble();
            if (lowerType == "decimal")
                return el.GetDecimal();
            if (lowerType == "boolean")
                return el.GetBoolean();
            if (lowerType == "timestamp")
                return DateTimeOffset.Parse(el.GetString()!);
            if (lowerType == "date")
                return Cassandra.LocalDate.Parse(
                    el.GetString()!);
            if (lowerType == "time")
                return Cassandra.LocalTime.Parse(
                    el.GetString()!);
            if (lowerType == "text" || lowerType == "varchar"
                || lowerType == "ascii")
                return el.GetString()!;
            if (lowerType == "blob")
            {
                var blobStr = el.GetString() ?? "";
                if (blobStr.StartsWith("0x",
                    StringComparison.OrdinalIgnoreCase))
                {
                    // FFCF returns blob as hex "0x01ab..."
                    var hex = blobStr.Substring(2);
                    var bytes = new byte[hex.Length / 2];
                    for (int i = 0; i < bytes.Length; i++)
                        bytes[i] = Convert.ToByte(
                            hex.Substring(i * 2, 2), 16);
                    return bytes;
                }
                return Convert.FromBase64String(blobStr);
            }
            if (lowerType == "inet")
                return System.Net.IPAddress.Parse(
                    el.GetString()!);
            if (lowerType == "varint")
            {
                if (el.TryGetInt64(out var v))
                    return new System.Numerics.BigInteger(v);
                return System.Numerics.BigInteger.Parse(
                    el.GetRawText());
            }

            // Collection types: set<T>, list<T>, map<K,V>,
            // and frozen<> wrappers.
            if (lowerType.StartsWith("set<")
                || lowerType.StartsWith("frozen<set<"))
            {
                var innerType = ExtractInnerType(lowerType);
                return ParseJsonArray(el, innerType)
                    .ToHashSet();
            }
            if (lowerType.StartsWith("list<")
                || lowerType.StartsWith("frozen<list<"))
            {
                var innerType = ExtractInnerType(lowerType);
                return ParseJsonArray(el, innerType);
            }
            if (lowerType.StartsWith("map<")
                || lowerType.StartsWith("frozen<map<"))
            {
                return ParseJsonMap(el, lowerType);
            }

            // Fallback for unknown/UDT types
            return el.GetRawText();
        }

        /// <summary>
        /// Extract the element type from a collection type
        /// string like "set&lt;text&gt;" or
        /// "frozen&lt;set&lt;int&gt;&gt;".
        /// </summary>
        private static string ExtractInnerType(string cqlType)
        {
            // Strip frozen<...> wrapper if present
            var t = cqlType;
            if (t.StartsWith("frozen<"))
                t = t.Substring(7, t.Length - 8); // remove frozen< and >

            // Now t is e.g. "set<text>" or "list<int>"
            var open = t.IndexOf('<');
            var close = t.LastIndexOf('>');
            if (open >= 0 && close > open)
                return t.Substring(open + 1, close - open - 1)
                    .Trim();
            return "text";
        }

        /// <summary>
        /// Parse a JSON array into a List of typed objects.
        /// </summary>
        private static List<object> ParseJsonArray(
            JsonElement el, string innerType)
        {
            var list = new List<object>();
            if (el.ValueKind != JsonValueKind.Array)
                return list;
            foreach (var item in el.EnumerateArray())
            {
                list.Add(ConvertScalar(item, innerType));
            }
            return list;
        }

        /// <summary>
        /// Parse a JSON object into a Dictionary for map
        /// types. Keys are always strings in JSON; values
        /// are converted based on the map's value type.
        /// </summary>
        private static Dictionary<string, object>
            ParseJsonMap(JsonElement el, string cqlType)
        {
            var dict = new Dictionary<string, object>();
            if (el.ValueKind != JsonValueKind.Object)
                return dict;

            // Extract value type from "map<text, int>"
            var inner = ExtractInnerType(cqlType);
            // inner is "text, int" — split on comma
            var parts = inner.Split(',');
            var valType = parts.Length > 1
                ? parts[1].Trim() : "text";

            foreach (var prop in el.EnumerateObject())
            {
                dict[prop.Name] =
                    ConvertScalar(prop.Value, valType);
            }
            return dict;
        }

        /// <summary>
        /// Convert a single JSON element to a .NET scalar
        /// matching the CQL type.
        /// </summary>
        private static object ConvertScalar(
            JsonElement el, string cqlType)
        {
            if (el.ValueKind == JsonValueKind.Null)
                return null!;
            var t = cqlType.Trim().ToLowerInvariant();
            if (t == "int") return el.GetInt32();
            if (t == "bigint") return el.GetInt64();
            if (t == "smallint") return (short)el.GetInt32();
            if (t == "tinyint") return (sbyte)el.GetInt32();
            if (t == "float") return el.GetSingle();
            if (t == "double") return el.GetDouble();
            if (t == "decimal") return el.GetDecimal();
            if (t == "boolean") return el.GetBoolean();
            if (t == "uuid" || t == "timeuuid")
                return Guid.Parse(el.GetString()!);
            if (t == "timestamp")
                return DateTimeOffset.Parse(el.GetString()!);
            if (t == "blob")
                return Convert.FromBase64String(
                    el.GetString() ?? "");
            if (t == "inet")
                return System.Net.IPAddress.Parse(
                    el.GetString()!);
            // Default: text/varchar/ascii
            return el.GetString() ?? el.GetRawText();
        }
    }
}