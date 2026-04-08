using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Helpers;
using CassandraMigrationProcessor.Helpers.Cassandra;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.Processors;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.Workers
{
    /// <summary>
    /// Orchestrates a Cassandra-to-Cassandra migration:
    ///   1. Opens source session via CassandraClientFactory
    ///   2. For each MigrationUnit, creates CopyProcessor
    ///      and runs the copy
    ///   3. Optionally starts ChangeFeedProcessor for
    ///      ongoing replication
    /// </summary>
    public class MigrationWorker
    {
        private readonly Log _log;
        private MigrationProcessor? _activeProcessor;
        private ISession? _sourceSession;
        private int _consecutiveAuthErrors;
        private const int MaxConsecutiveAuthErrors = 3;

        // Parallel table migration: track active processors
        // per migration unit for concurrent table copies
        private readonly ConcurrentDictionary<string, MigrationProcessor>
            _activeProcessors = new();

        public MigrationWorker(Log log)
        {
            _log = log;
        }

        /// <summary>
        /// Start or resume migration for the active job.
        /// </summary>
        public async Task<TaskResult> StartAsync(
            MigrationJob job,
            MigrationSettings config,
            CancellationToken ct)
        {
            Console.WriteLine(
                $"MigrationWorker.StartAsync called for job={job.Id}");
            MigrationJobContext.AddVerboseLog(
                $"MigrationWorker.StartAsync: job={job.Id}");

            try
            {
                var units = Helper.GetMigrationUnitsToMigrate(job);
                Console.WriteLine(
                    $"GetMigrationUnitsToMigrate returned {units?.Count ?? 0} units");

                if (units == null || units.Count == 0)
                {
                    // All tables already copied. If online
                    // mode, restart the change feed processors
                    // so pause/resume works correctly.
                    if (Helper.IsOnline(job)
                        && Helper.IsOfflineJobCompleted(job)
                        && Helper.AnyValidTable(job))
                    {
                        Console.WriteLine(
                            "No copy units left — " +
                            "resuming change feed for " +
                            "online job.");
                        _log.WriteLine(
                            "All tables copied. Resuming " +
                            "change feed processors.",
                            LogType.Info);

                        EnsureSourceSession(job,
                            job.MigrationUnitBasics!
                                .First().KeyspaceName);

                        _activeProcessor = new CopyProcessor(
                            _log, _sourceSession!, config,
                            this);
                        _activeProcessor
                            .RunChangeFeedForAllTables();

                        // Keep worker alive while CF runs
                        while (!ct.IsCancellationRequested
                            && !MigrationJobContext
                                .ControlledPauseRequested)
                        {
                            await Task.Delay(2000, ct);
                        }

                        return ct.IsCancellationRequested
                            ? TaskResult.Canceled
                            : TaskResult.Success;
                    }

                    Console.WriteLine(
                        "No remaining migration units " +
                        "- returning Success");
                    _log.WriteLine(
                        "No remaining migration units.",
                        LogType.Warning);
                    return TaskResult.Success;
                }

                // Determine parallelism: use job setting,
                // capped to a reasonable max for table-level
                // concurrency (not row-level).
                int maxParallel = Math.Max(1,
                    Math.Min(job.ParallelThreads,
                        units.Count));

                Console.WriteLine(
                    $"Starting migration of {units.Count}" +
                    $" units with parallelism={maxParallel}." +
                    $" First: {units[0].KeyspaceName}" +
                    $".{units[0].TableName}");
                _log.WriteLine(
                    $"Migrating {units.Count} tables with" +
                    $" max parallelism={maxParallel}");

                var abortRequested = false;

                await Parallel.ForEachAsync(
                    units,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = maxParallel,
                        CancellationToken = ct
                    },
                    async (mu, token) =>
                    {
                        if (MigrationJobContext
                            .ControlledPauseRequested)
                            return;

                        if (_consecutiveAuthErrors
                            >= MaxConsecutiveAuthErrors)
                        {
                            abortRequested = true;
                            return;
                        }

                        Console.WriteLine(
                            $"About to process: " +
                            $"{mu.KeyspaceName}.{mu.TableName}");

                        // Retry on transient 429/overload errors
                        const int MaxTableRetries = 3;
                        for (int attempt = 1;
                            attempt <= MaxTableRetries;
                            attempt++)
                        {
                            try
                            {
                                await ProcessMigrationUnitAsync(
                                    job, config, mu, token);
                                break; // success
                            }
                            catch (Exception ex) when (
                                CassandraClientFactory
                                    .IsRetryableException(ex)
                                && attempt < MaxTableRetries)
                            {
                                int delayMs =
                                    CassandraClientFactory
                                        .GetRetryDelayMs(
                                            ex, attempt);
                                Console.WriteLine(
                                    $"  Table retry " +
                                    $"{attempt}/{MaxTableRetries}" +
                                    $" for {mu.KeyspaceName}" +
                                    $".{mu.TableName}: " +
                                    $"{ex.GetType().Name}, " +
                                    $"waiting {delayMs}ms");
                                _log.WriteLine(
                                    $"Table retry {attempt} " +
                                    $"for {mu.KeyspaceName}" +
                                    $".{mu.TableName}: " +
                                    $"{ex.Message}",
                                    LogType.Warning);
                                await Task.Delay(
                                    delayMs, token);
                            }
                        }

                        Console.WriteLine(
                            $"Finished processing: " +
                            $"{mu.KeyspaceName}.{mu.TableName}");
                    });

                if (abortRequested)
                {
                    _log.WriteLine(
                        $"Aborting: {_consecutiveAuthErrors}" +
                        " consecutive auth failures.",
                        LogType.Error);
                    return TaskResult.Abort;
                }

                // All tables processed — check job completion
                // (must be after Parallel.ForEachAsync, not per-table)
                if (_activeProcessor != null)
                    _activeProcessor.StopOfflineOrInvokeChangeFeed();

                return TaskResult.Success;
            }
            catch (OperationCanceledException)
            {
                _log.WriteLine("Migration was cancelled.");
                return TaskResult.Canceled;
            }
            catch (Exception ex)
            {
                _log.WriteLine(
                    $"Migration failed: {ex}", LogType.Error);
                return TaskResult.Abort;
            }
            finally
            {
                CleanupSession();
            }
        }

        private async Task ProcessMigrationUnitAsync(
            MigrationJob job,
            MigrationSettings config,
            MigrationUnit mu,
            CancellationToken ct)
        {
            Console.WriteLine(
                $"ProcessMigrationUnitAsync: {mu.KeyspaceName}.{mu.TableName}");
            _log.WriteLine(
                $"Processing {mu.KeyspaceName}.{mu.TableName}");

            // Reset Failed status on retry/resume so the table
            // gets a fresh chance (Bug 3 fix)
            if (mu.SourceStatus == CollectionStatus.Failed)
            {
                Console.WriteLine(
                    $"  Resetting SourceStatus from Failed " +
                    $"to OK for {mu.KeyspaceName}.{mu.TableName}");
                mu.SourceStatus = CollectionStatus.OK;
                MigrationJobContext.SaveMigrationUnit(mu, true);
            }

            // Each parallel table gets its own source session
            // so concurrent copies don't interfere.
            ISession? localSourceSession = null;
            try
            {
                Console.WriteLine("  Creating source session...");
                localSourceSession = CassandraClientFactory
                    .CreateSourceSession(
                        _log, job, mu.KeyspaceName);
                Console.WriteLine("  Source session OK");

                // Validate table exists on source
                Console.WriteLine("  Checking table exists on source...");
                if (!await CassandraHelper.TableExistsAsync(
                    localSourceSession!,
                    mu.KeyspaceName, mu.TableName)
                    .ConfigureAwait(false))
                {
                    Console.WriteLine($"  Table NOT FOUND on source");
                    _log.WriteLine(
                        $"Source table {mu.KeyspaceName}" +
                        $".{mu.TableName} not found.",
                        LogType.Error);
                    mu.SourceStatus = CollectionStatus.NotFound;
                    MigrationJobContext.SaveMigrationUnit(
                        mu, true);
                    return;
                }
                Console.WriteLine("  Table exists on source");

                // Ensure target keyspace + table
                if (!job.IsSimulatedRun)
                {
                    Console.WriteLine("  Creating target session...");
                    using (var targetSession =
                        CassandraClientFactory.CreateTargetSession(
                            _log, job, string.Empty))
                    {
                        Console.WriteLine("  Target session created, ensuring keyspace...");
                        await CassandraHelper.EnsureKeyspaceExistsAsync(
                            targetSession, mu.KeyspaceName)
                            .ConfigureAwait(false);

                        if (job.DropTargetTableBeforeStart
                            && await CassandraHelper.TableExistsAsync(
                                targetSession,
                                mu.KeyspaceName, mu.TableName)
                                .ConfigureAwait(false))
                        {
                            _log.WriteLine(
                                $"Dropping target table " +
                                $"{mu.KeyspaceName}" +
                                $".{mu.TableName} " +
                                $"(DropTargetTableBeforeStart)");
                            await targetSession.ExecuteAsync(
                                new SimpleStatement(
                                    $"DROP TABLE " +
                                    $"\"{mu.KeyspaceName}\"" +
                                    $".\"{mu.TableName}\""))
                                .ConfigureAwait(false);
                        }

                        if (!await CassandraHelper.TableExistsAsync(
                            targetSession,
                            mu.KeyspaceName, mu.TableName)
                            .ConfigureAwait(false))
                        {
                            Console.WriteLine($"  Creating target table {mu.KeyspaceName}.{mu.TableName}...");
                            await CassandraHelper.CreateTableFromSourceAsync(
                                localSourceSession!, targetSession,
                                mu.KeyspaceName, mu.TableName,
                                mu.KeyspaceName, mu.TableName)
                                .ConfigureAwait(false);
                            _log.WriteLine(
                                $"Created target table " +
                                $"{mu.KeyspaceName}" +
                                $".{mu.TableName}");
                        }
                        else
                        {
                            // Table exists — sync schema
                            // (adds missing columns via ALTER)
                            await CassandraHelper.CreateTableFromSourceAsync(
                                localSourceSession!, targetSession,
                                mu.KeyspaceName, mu.TableName,
                                mu.KeyspaceName, mu.TableName)
                                .ConfigureAwait(false);
                        }
                        Console.WriteLine("  Target table ready");
                    }
                }

                mu.BulkCopyStartedOn ??= DateTime.UtcNow;

                // Log feed range count for this table
                if (!job.IsSimulatedRun)
                {
                    try
                    {
                        var rangeCount = (await CassandraHelper
                            .GetFeedRangesAsync(
                                localSourceSession!,
                                mu.KeyspaceName,
                                mu.TableName)
                                .ConfigureAwait(false)).Count;
                        _log.WriteLine(
                            $"Feed ranges: {rangeCount} " +
                            $"for {mu.KeyspaceName}" +
                            $".{mu.TableName}");
                    }
                    catch { }
                }

                // Capture FFCF continuation token BEFORE
                // bulk copy.  We do a "dry" poll with
                // FROM_START=false which returns the current
                // position in the change feed.  After bulk
                // copy, the change feed resumes from this
                // position, ensuring changes made during
                // copy are not lost.
                //
                // Also capture per-feed-range tokens for
                // parallel change feed processing.
                if (Helper.IsOnline(job)
                    && string.IsNullOrEmpty(
                        mu.ChangeFeedContinuationToken))
                {
                    try
                    {
                        // Get feed ranges for parallel CF
                        var feedRanges = CassandraHelper
                            .GetFeedRanges(
                                localSourceSession!,
                                mu.KeyspaceName,
                                mu.TableName);

                        _log.WriteLine(
                            $"Feed ranges discovered: " +
                            $"{feedRanges.Count} for " +
                            $"{mu.KeyspaceName}" +
                            $".{mu.TableName}");
                        Console.WriteLine(
                            $"  Feed ranges: " +
                            $"{feedRanges.Count} for " +
                            $"{mu.KeyspaceName}" +
                            $".{mu.TableName}");

                        if (feedRanges.Count > 1)
                        {
                            // Capture per-range tokens
                            mu.FeedRangeStartTokens =
                                new Dictionary<string, string>();
                            mu.FeedRangeContinuationTokens =
                                new Dictionary<string, string>();

                            foreach (var range in feedRanges)
                            {
                                try
                                {
                                    string rangeCql =
                                        $"SELECT JSON * FROM " +
                                        $"\"{mu.KeyspaceName}\"" +
                                        $".\"{mu.TableName}\"" +
                                        $" WHERE COSMOS_CHANGEFEED" +
                                        $"_FROM_START() = false" +
                                        $" AND COSMOS_FULLFIDELITY" +
                                        $"_CHANGEFEED() = true" +
                                        $" AND COSMOS_FEEDRANGE()" +
                                        $" = '{range}'";
                                    var rangeStmt = new Cassandra
                                        .SimpleStatement(rangeCql);
                                    rangeStmt.SetPageSize(1);
                                    rangeStmt.SetAutoPage(false);
                                    var rangeRs = await localSourceSession!
                                        .ExecuteAsync(rangeStmt)
                                        .ConfigureAwait(false);
                                    if (rangeRs.PagingState != null)
                                    {
                                        mu.FeedRangeContinuationTokens[
                                            range] = Convert
                                            .ToBase64String(
                                                rangeRs.PagingState);
                                    }
                                }
                                catch (Exception rex)
                                {
                                    Console.WriteLine(
                                        $"  FFCF: Range token " +
                                        $"capture failed for " +
                                        $"range: {rex.Message}");
                                }
                            }
                            Console.WriteLine(
                                $"  FFCF: Captured {feedRanges.Count}" +
                                $" feed range tokens for " +
                                $"{mu.KeyspaceName}" +
                                $".{mu.TableName}");
                            _log.WriteLine(
                                $"FFCF: {feedRanges.Count} feed " +
                                $"range tokens captured for " +
                                $"{mu.KeyspaceName}" +
                                $".{mu.TableName}");
                        }
                        else
                        {
                            // Single range or no ranges:
                            // use legacy single-token path
                            string ffcfCql =
                                $"SELECT JSON * FROM " +
                                $"\"{mu.KeyspaceName}\"" +
                                $".\"{mu.TableName}\"" +
                                $" WHERE COSMOS_CHANGEFEED" +
                                $"_FROM_START() = false" +
                                $" AND COSMOS_FULLFIDELITY" +
                                $"_CHANGEFEED() = true";
                            var stmt = new Cassandra
                                .SimpleStatement(ffcfCql);
                            stmt.SetPageSize(1);
                            stmt.SetAutoPage(false);
                            var rs = await localSourceSession!
                                .ExecuteAsync(stmt)
                                .ConfigureAwait(false);
                            if (rs.PagingState != null)
                            {
                                mu.ChangeFeedContinuationToken =
                                    Convert.ToBase64String(
                                        rs.PagingState);
                            }
                            Console.WriteLine(
                                $"  FFCF: Captured continuation " +
                                $"token for {mu.KeyspaceName}" +
                                $".{mu.TableName} " +
                                $"(has token: " +
                                $"{rs.PagingState != null})");
                        }
                        _log.WriteLine(
                            $"FFCF start token captured for " +
                            $"{mu.KeyspaceName}" +
                            $".{mu.TableName}: " +
                            $"{DateTime.UtcNow:o}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(
                            $"  FFCF: Failed to capture " +
                            $"start token for " +
                            $"{mu.KeyspaceName}" +
                            $".{mu.TableName}: " +
                            $"{ex.Message}");
                        _log.WriteLine(
                            $"FFCF start token capture " +
                            $"failed for " +
                            $"{mu.KeyspaceName}" +
                            $".{mu.TableName}: " +
                            $"{ex.Message}",
                            LogType.Warning);
                    }
                    // Also record timestamp for display
                    mu.ChangeFeedStartToken =
                        DateTime.UtcNow.ToString(
                            "yyyy-MM-ddTHH:mm:ss.fffZ",
                            System.Globalization.CultureInfo
                                .InvariantCulture);
                }

                MigrationJobContext.SaveMigrationUnit(mu, true);

                var processor = new CopyProcessor(
                    _log, localSourceSession!, config, this);
                _activeProcessors[mu.Id] = processor;

                ct.ThrowIfCancellationRequested();

                TaskResult result =
                    await processor.StartProcessAsync(
                        mu.Id);

                switch (result)
                {
                    case TaskResult.Success:
                        _log.WriteLine(
                            $"Copy succeeded for " +
                            $"{mu.KeyspaceName}.{mu.TableName}");
                        break;

                    case TaskResult.Canceled:
                        _log.WriteLine(
                            $"Copy paused for " +
                            $"{mu.KeyspaceName}.{mu.TableName}");
                        break;

                    default:
                        _log.WriteLine(
                            $"Copy failed for " +
                            $"{mu.KeyspaceName}.{mu.TableName}",
                            LogType.Error);
                        break;
                }

                MigrationJobContext.SaveMigrationUnit(mu, true);
            }
            catch (OperationCanceledException)
            {
                _log.WriteLine(
                    $"Cancelled {mu.KeyspaceName}" +
                    $".{mu.TableName}");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"  ERROR processing {mu.KeyspaceName}.{mu.TableName}: {ex.Message}");
                _log.WriteLine(
                    $"Error processing " +
                    $"{mu.KeyspaceName}.{mu.TableName}: {ex}",
                    LogType.Error);

                // BB-3 fix: Mark MU as failed so the UI shows
                // "Failed" instead of "0.0%"
                mu.SourceStatus = CollectionStatus.Failed;

                // Detect auth errors (expired token)
                if (IsAuthError(ex))
                {
                    Interlocked.Increment(
                        ref _consecutiveAuthErrors);
                    Console.WriteLine(
                        $"  AUTH ERROR #{_consecutiveAuthErrors}" +
                        " for {mu.KeyspaceName}.{mu.TableName}");
                    _log.WriteLine(
                        $"Auth failure #{_consecutiveAuthErrors}" +
                        $" on {mu.KeyspaceName}.{mu.TableName}",
                        LogType.Warning);
                }
                else
                {
                    Interlocked.Exchange(
                        ref _consecutiveAuthErrors, 0);
                }

                MigrationJobContext.SaveMigrationUnit(mu, true);
            }
            finally
            {
                // Cleanup per-table resources
                _activeProcessors.TryRemove(mu.Id, out _);
                try { localSourceSession?.Dispose(); }
                catch { /* best-effort */ }
            }
        }

        private void EnsureSourceSession(
            MigrationJob job, string keyspace)
        {
            if (_sourceSession != null
                && !_sourceSession.IsDisposed)
            {
                return;
            }

            _sourceSession = CassandraClientFactory
                .CreateSourceSession(_log, job, keyspace);

            _log.WriteLine(
                $"Source session created for " +
                $"{job.SourceContactPoint}");
        }

        private void CleanupSession()
        {
            try
            {
                _sourceSession?.Dispose();
                _sourceSession = null;
            }
            catch (Exception ex)
            {
                MigrationJobContext.AddVerboseLog(
                    $"Session cleanup error: {ex.Message}");
            }
        }

        /// <summary>
        /// Force-invalidate the source session so
        /// EnsureSourceSession creates a new one.
        /// Used after auth failures (expired tokens).
        /// </summary>
        private void ForceInvalidateSession()
        {
            try { _sourceSession?.Dispose(); }
            catch { /* best-effort */ }
            _sourceSession = null;
        }

        /// <summary>
        /// Check if an exception is an auth-related
        /// failure (expired AAD token, bad credentials).
        /// </summary>
        private static bool IsAuthError(Exception ex)
        {
            if (ex is Cassandra.AuthenticationException)
                return true;
            if (ex.InnerException
                is Cassandra.AuthenticationException)
                return true;
            // NoHostAvailableException can wrap auth errors
            if (ex is Cassandra.NoHostAvailableException nhae)
            {
                return nhae.Errors?.Values?.Any(
                    e => e is Cassandra.AuthenticationException)
                    ?? false;
            }
            return false;
        }

        /// <summary>
        /// Request the active processor to stop gracefully.
        /// </summary>
        public void Stop()
        {
            // Stop all active parallel processors
            foreach (var kvp in _activeProcessors)
            {
                kvp.Value?.StopProcessing();
            }
            _activeProcessors.Clear();

            // Legacy single-processor stop
            _activeProcessor?.StopProcessing();
            CleanupSession();
        }

        /// <summary>
        /// Populate the migration-unit list for a new job
        /// by discovering keyspaces and tables from source.
        /// </summary>
        public static List<MigrationUnit> DiscoverTables(
            Log log, MigrationJob job)
        {
            MigrationJobContext.AddVerboseLog(
                "MigrationWorker.DiscoverTables");

            var result = new List<MigrationUnit>();

            using (var session = CassandraClientFactory
                .CreateSourceSession(log, job, "system"))
            {
                var keyspaces = CassandraHelper
                    .ListKeyspaces(session);

                foreach (var ks in keyspaces)
                {
                    var tables = CassandraHelper
                        .ListTables(session, ks);

                    foreach (var tbl in tables)
                    {
                        result.Add(new MigrationUnit(
                            job, ks, tbl,
                            new List<MigrationChunk>()));
                    }
                }
            }

            int ksCount = result
                .Select(r => r.KeyspaceName)
                .Distinct().Count();
            log.WriteLine(
                $"Discovered {result.Count} tables across " +
                $"{ksCount} keyspaces");

            return result;
        }
    }
}
