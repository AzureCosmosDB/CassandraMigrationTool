using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Processors
{
    public partial class ChangeFeedProcessor
    {
        private async Task PollLoopAsync(string muId, CancellationToken ct)
        {
            try
            {
                var mu = _muCache.GetMigrationUnit(muId, _job?.Id);
                if (mu == null)
                {
                    _log.WriteLine($"ChangeFeed: MU {muId} not found", LogType.Error);
                    return;
                }

                // Discover feed ranges for parallel processing
                var feedRanges = CassandraHelper.GetFeedRanges(_sourceSession, mu.KeyspaceName, mu.TableName);

                _log.WriteLine($"Feed ranges discovered: {feedRanges.Count} for {mu.KeyspaceName}.{mu.TableName}");

                if (feedRanges.Count > 1
                    && mu.FeedRangeContinuationTokens != null)
                {
                    // PARALLEL: multiple feed ranges
                    _log.WriteLine($"Change feed PARALLEL mode: {feedRanges.Count} ranges for {mu.KeyspaceName}.{mu.TableName}");

                    await PollLoopParallelAsync(mu, feedRanges, ct);
                }
                else
                {
                    // SINGLE: one range or no feed ranges
                    await PollLoopSingleAsync(mu, null, ct);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  CF FATAL: muId={muId}: {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine($"  CF FATAL stack: {ex.StackTrace}");
            }

            _activeTasks.TryRemove(muId, out _);
        }

        /// <summary>
        /// Run parallel change feed readers, one per feed range.
        /// Each range reader has its own paging state and poll
        /// loop. Stats are aggregated to the MU using Interlocked.
        /// </summary>
        private async Task PollLoopParallelAsync(MigrationUnit mu, List<string> feedRanges, CancellationToken ct)
        {
            mu.ChangeFeedStartedOn ??= DateTime.UtcNow;

            var columns = CassandraHelper.GetTableColumns(_sourceSession, mu.KeyspaceName, mu.TableName);
            var userColumns = columns
                .Where(c => !c.Name.StartsWith("system_", StringComparison.OrdinalIgnoreCase))
                .ToList();

            var (ps, colNames) = CassandraHelper.PrepareInsert(_targetSession!, mu.GetEffectiveTargetKeyspaceName(),
                mu.GetEffectiveTargetTableName(),
                userColumns);

            int maxConcurrent = Math.Max(1, _job.MaxFeedRangeParallelism);

            // Throttle concurrent range tasks to avoid
            // thread pool starvation on small App Service plans.
            var semaphore = new SemaphoreSlim(maxConcurrent);
            var rangeTasks = feedRanges.Select(async range =>
            {
                await semaphore.WaitAsync(ct);
                try
                {
                    await PollRangeLoopAsync(mu, range,
                        ps, colNames, ct);
                }
                finally { semaphore.Release(); }
            }).ToArray();

            await Task.WhenAll(rangeTasks);

            _log.WriteLine($"Change feed PARALLEL stopped for {mu.KeyspaceName}.{mu.TableName}, ranges={feedRanges.Count}");
        }

        /// <summary>
        /// Poll loop for a single feed range within a table.
        /// Updates MU counters with Interlocked for thread safety.
        /// </summary>
        private async Task PollRangeLoopAsync(MigrationUnit mu, string feedRange,
            PreparedStatement ps,
            List<string> colNames,
            CancellationToken ct)
        {
            try
            {
                int intervalMs = _config.ChangeFeedPollIntervalMs > 0
                        ? _config.ChangeFeedPollIntervalMs
                        : 5000;
                int consecutiveErrors = 0;
                const int MaxReconnectAttempts = 50;
                byte[]? continuationState = null;
                if (mu.FeedRangeContinuationTokens != null)
                {
                    lock (mu.FeedRangeContinuationTokens)
                    {
                        if (mu.FeedRangeContinuationTokens.TryGetValue(feedRange, out var saved)
                            && !string.IsNullOrEmpty(saved))
                        {
                            continuationState = Convert.FromBase64String(saved);
                        }
                    }
                }

                string startTime = !string.IsNullOrEmpty(mu.ChangeFeedStartToken)
                        ? mu.ChangeFeedStartToken
                        : DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ",
                            System.Globalization.CultureInfo.InvariantCulture);
                string cql = $"SELECT * FROM \"{mu.KeyspaceName}\".\"{mu.TableName}\"" + $" WHERE COSMOS_CHANGEFEED_START_TIME() = '{startTime}' AND COSMOS_FEEDRANGE() = '{feedRange}'";

                var rangeLabel = feedRange.Length > 40
                    ? feedRange.Substring(0, 40) + "..."
                    : feedRange;

                long rangeTotalApplied = 0;

                while (!ct.IsCancellationRequested
                    && !ExecutionCancelled)
                {
                    try
                    {
                        var statement = new SimpleStatement(cql);
                        statement.SetPageSize(_config.CqlCopyPageSize > 0
                                ? _config.CqlCopyPageSize : 500);
                        statement.SetAutoPage(false);

                        if (continuationState != null)
                            statement.SetPagingState(continuationState);

                        var rs = await _sourceSession.ExecuteAsync(statement);

                        continuationState = rs.PagingState;

                        int insertCount = 0;
                        int errorCount = 0;

                        int available = rs.GetAvailableWithoutFetching();
                        int consumed = 0;
                        foreach (var row in rs)
                        {
                            if (consumed >= available) break;
                            consumed++;
                            if (ct.IsCancellationRequested
                                || ExecutionCancelled) break;

                            try
                            {
                                var values =
                                    new object[colNames.Count];
                                for (int i = 0;
                                    i < colNames.Count; i++)
                                    values[i] = row[colNames[i]];
                                await _targetSession!.ExecuteAsync(ps.Bind(values));
                                insertCount++;
                                rangeTotalApplied++;
                            }
                            catch (Exception ex)
                            {
                                errorCount++;
                                Interlocked.Increment(ref mu._changeFeedErrors);
                                MigrationJobContext.AddVerboseLog($"CF apply fail: {ex.Message}");
                            }
                        }

                        // Aggregate to MU with Interlocked
                        Interlocked.Add(ref mu._changeFeedInsertEvents, insertCount);
                        Interlocked.Add(ref mu._changeFeedRowsInserted, insertCount);

                        int batchTotal = insertCount;
                        Interlocked.Add(ref mu._changeFeedUpdatesInLastBatch, batchTotal);
                        mu.ChangeFeedLastChecked = DateTime.UtcNow;

                        // Save continuation AFTER writes so crash
                        // doesn't skip unwritten rows
                        if (continuationState != null
                            && mu.FeedRangeContinuationTokens != null)
                        {
                            lock (mu.FeedRangeContinuationTokens)
                            {
                                mu.FeedRangeContinuationTokens[
                                    feedRange] = Convert.ToBase64String(continuationState);
                            }
                        }

                        mu.UpdateParentJob();
                        MigrationJobContext.SaveMigrationUnit(
                            mu, batchTotal > 0 || errorCount > 0);

                        consecutiveErrors = 0;
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        consecutiveErrors++;
                        _log.WriteLine($"Change feed error ({consecutiveErrors}): {ex.GetType().Name}: {ex.Message}",
                            LogType.Warning);

                        if (consecutiveErrors > MaxReconnectAttempts)
                        {
                            _log.WriteLine($"Change feed giving up after {consecutiveErrors} errors",
                                LogType.Error);
                            break;
                        }

                        // Exponential backoff on errors (cap 60s)
                        int errorDelay = Math.Min(intervalMs * consecutiveErrors, 60_000);
                        try { await Task.Delay(errorDelay, ct); }
                        catch (OperationCanceledException) { break; }
                        continue;
                    }

                    try { await Task.Delay(intervalMs, ct); }
                    catch (OperationCanceledException) { break; }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  CF RANGE FATAL: {mu.KeyspaceName}.{mu.TableName}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        /// <summary>
        /// Single-range poll loop (original behavior).
        /// Also used when feed range is explicitly provided.
        /// </summary>
        private async Task PollLoopSingleAsync(MigrationUnit mu, string? feedRange, CancellationToken ct)
        {
            try
            {
                int intervalMs = _config.ChangeFeedPollIntervalMs > 0
                        ? _config.ChangeFeedPollIntervalMs
                        : 5000;

                int consecutiveErrors = 0;
                const int MaxReconnectAttempts = 50;

                byte[]? continuationState = !string.IsNullOrEmpty(mu.ChangeFeedContinuationToken)
                        ? Convert.FromBase64String(mu.ChangeFeedContinuationToken)
                        : null;

                string startTime = !string.IsNullOrEmpty(mu.ChangeFeedStartToken)
                        ? mu.ChangeFeedStartToken
                        : DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ",
                            System.Globalization.CultureInfo.InvariantCulture);

                string cql = $"SELECT * FROM \"{mu.KeyspaceName}\".\"{mu.TableName}\"" + $" where COSMOS_CHANGEFEED_START_TIME() = '{startTime}'";

                var columns = CassandraHelper.GetTableColumns(_sourceSession, mu.KeyspaceName, mu.TableName);

                var userColumns = columns
                    .Where(c => !c.Name.StartsWith("system_", StringComparison.OrdinalIgnoreCase))
                    .ToList();

                var (ps, colNames) = CassandraHelper.PrepareInsert(_targetSession!, mu.GetEffectiveTargetKeyspaceName(),
                    mu.GetEffectiveTargetTableName(),
                    userColumns);

                mu.ChangeFeedStartedOn ??= DateTime.UtcNow;
                long totalApplied = 0;

                _log.WriteLine($"Change feed started for {mu.KeyspaceName}.{mu.TableName} (startToken={startTime})");

                while (!ct.IsCancellationRequested
                    && !ExecutionCancelled)
                {
                    try
                    {
                        var statement = new SimpleStatement(cql);
                        statement.SetPageSize(_config.CqlCopyPageSize > 0
                                ? _config.CqlCopyPageSize : 500);
                        statement.SetAutoPage(false);

                        if (continuationState != null)
                            statement.SetPagingState(continuationState);

                        var rs = await _sourceSession.ExecuteAsync(statement);

                        continuationState = rs.PagingState;
                        if (continuationState != null)
                        {
                            mu.ChangeFeedContinuationToken = Convert.ToBase64String(continuationState);
                        }

                        var sw = Stopwatch.StartNew();
                        int insertCount = 0;
                        int errorCount = 0;

                        int available = rs.GetAvailableWithoutFetching();
                        int consumed = 0;
                        foreach (var row in rs)
                        {
                            if (consumed >= available) break;
                            consumed++;
                            if (ct.IsCancellationRequested
                                || ExecutionCancelled) break;

                            try
                            {
                                var values =
                                    new object[colNames.Count];
                                for (int i = 0;
                                    i < colNames.Count; i++)
                                    values[i] = row[colNames[i]];
                                await _targetSession!.ExecuteAsync(ps.Bind(values));
                                insertCount++;
                                totalApplied++;
                            }
                            catch (Exception ex)
                            {
                                errorCount++;
                                mu.ChangeFeedErrors++;
                                MigrationJobContext.AddVerboseLog($"CF apply fail: {ex.Message}");
                            }
                        }

                        mu.ChangeFeedInsertEvents += insertCount;
                        mu.ChangeFeedRowsInserted += insertCount;
                        Interlocked.Add(ref mu._changeFeedUpdatesInLastBatch, insertCount);
                        mu.ChangeFeedLastChecked = DateTime.UtcNow;

                        int batchTotal = insertCount;
                        if (sw.ElapsedMilliseconds > 0
                            && batchTotal > 0)
                        {
                            mu.ChangeFeedAvgWriteLatencyInMS = (double)sw.ElapsedMilliseconds
                                / batchTotal;
                        }

                        mu.UpdateParentJob();
                        if (batchTotal > 0 || errorCount > 0)
                        {
                            MigrationJobContext.SaveMigrationUnit(mu, true);
                            _log.WriteLine($"CF {mu.KeyspaceName}.{mu.TableName}: ins={insertCount}, err={errorCount}, total={totalApplied}");
                        }
                        else
                        {
                            MigrationJobContext.SaveMigrationUnit(mu, false);
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
                        _log.WriteLine($"CF error {mu.KeyspaceName}.{mu.TableName}: {ex.Message}", LogType.Error);

                        if (consecutiveErrors <= MaxReconnectAttempts
                            && ExceptionClassifier.IsTransient(ex))
                        {
                            try
                            {
                                var job = MigrationJobContext.CurrentlyActiveJob;
                                ISession newSource;
                                if (CassandraClientFactory.IsLikelyAadToken(job.SourcePassword))
                                {
                                    newSource = CassandraClientFactory.ReconnectSourceWithFreshToken(_log);
                                }
                                else
                                {
                                    newSource = CassandraClientFactory.CreateSourceSession(_log, job, string.Empty);
                                }
                                var oldSession = _sourceSession;
                                _sourceSession = newSource;
                                try { oldSession?.Dispose(); }
                                catch (Exception dex)
                                {
                                    Console.WriteLine($"[WARN] CF old source session dispose failed: {dex.Message}");
                                }
                                columns = CassandraHelper.GetTableColumns(_sourceSession, mu.KeyspaceName,
                                        mu.TableName);
                                userColumns = columns
                                    .Where(c => !c.Name.StartsWith("system_", StringComparison.OrdinalIgnoreCase))
                                    .ToList();
                                var newInsert = CassandraHelper.PrepareInsert(_targetSession!,
                                        mu.GetEffectiveTargetKeyspaceName(),
                                        mu.GetEffectiveTargetTableName(),
                                        userColumns);
                                ps = newInsert.Ps;
                                colNames = newInsert.ColumnNames;
                            }
                            catch (Exception rex) { }
                        }
                        else if (consecutiveErrors
                            > MaxReconnectAttempts)
                        {
                            _log.WriteLine($"Change feed giving up after {consecutiveErrors} errors",
                                LogType.Error);
                            break;
                        }

                        // Exponential backoff on errors
                        int errorDelay = Math.Min(intervalMs * consecutiveErrors, 60_000);
                        try { await Task.Delay(errorDelay, ct); }
                        catch (OperationCanceledException) { break; }
                        continue;
                    }

                    try { await Task.Delay(intervalMs, ct); }
                    catch (OperationCanceledException) { break; }
                }

                _log.WriteLine($"Change feed stopped for {mu.KeyspaceName}.{mu.TableName}, total applied={totalApplied}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  CF FATAL: {mu.KeyspaceName}.{mu.TableName}: {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine($"  CF FATAL stack: {ex.StackTrace}");
            }
        }
    }
}
