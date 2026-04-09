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
        private readonly MigrationLog _log;
        private MigrationProcessor? _activeProcessor;
        private ISession? _sourceSession;
        private int _consecutiveAuthErrors;

        // Parallel table migration: track active processors
        // per migration unit for concurrent table copies
        private readonly ConcurrentDictionary<string, MigrationProcessor>
            _activeProcessors = new();

        public MigrationWorker(MigrationLog MigrationLog)
        {
            _log = MigrationLog;
        }

        /// <summary>
        /// Start or resume migration for the active job.
        /// Processes each <see cref="MigrationUnit"/> with
        /// table-level parallelism and optional change-feed
        /// replication.
        /// </summary>
        public async Task<TaskResult> StartAsync(MigrationJob job, MigrationSettings config,
            CancellationToken cancellationToken)
        {
            MigrationJobContext.AddVerboseLog($"MigrationWorker.StartAsync: job={job.Id}");

            try
            {
                var units = MigrationHelper.GetMigrationUnitsToMigrate(job);

                if (units == null || units.Count == 0)
                {
                    // All tables already copied. If online
                    // mode, restart the change feed processors
                    // so pause/resume works correctly.
                    if (MigrationHelper.IsOnline(job)
                        && MigrationHelper.IsOfflineJobCompleted(job)
                        && MigrationHelper.AnyValidTable(job))
                    {
                        _log.WriteLine("All tables copied. Resuming " + "change feed processors.", LogType.Info);

                        EnsureSourceSession(job, job.Tables!.First().KeyspaceName);

                        _activeProcessor = new CopyProcessor(_log, _sourceSession!, config, job, this);

                        foreach (var mub in job.Tables)
                        {
                            if (!MigrationHelper.IsMigrationUnitValid(mub) || !mub.CopyComplete)
                                continue;
                            var mu = MigrationJobContext.GetMigrationUnit(mub.Id);
                            if (mu != null)
                                _activeProcessor.AddTableToChangeFeedQueue(mu);
                        }

                        // Keep worker alive while CF runs
                        while (!cancellationToken.IsCancellationRequested
                            && !MigrationJobContext.ControlledPauseRequested)
                        {
                            await Task.Delay(2000, cancellationToken);
                        }

                        return cancellationToken.IsCancellationRequested
                            ? TaskResult.Canceled
                            : TaskResult.Success;
                    }

                    _log.WriteLine("No remaining migration units.", LogType.Warning);
                    return TaskResult.Success;
                }

                // Determine parallelism: use job setting,
                // capped to a reasonable max for table-level
                // concurrency (not row-level).
                int maxParallel = Math.Max(1, Math.Min(job.ParallelThreads, units.Count));

                _log.WriteLine($"Migrating {units.Count} tables with max parallelism={maxParallel}");

                var abortRequested = false;

                await Parallel.ForEachAsync(units, new ParallelOptions
                    {
                        MaxDegreeOfParallelism = maxParallel,
                        CancellationToken = cancellationToken
                    },
                    async (migrationUnit, token) =>
                    {
                        if (MigrationJobContext.ControlledPauseRequested)
                            return;

                        if (Volatile.Read(ref _consecutiveAuthErrors)
                            >= MigrationDefaults.MaxConsecutiveAuthErrors)
                        {
                            abortRequested = true;
                            return;
                        }

                        // Retry on transient 429/overload errors
                        for (int attempt = 1;
                            attempt <= MigrationDefaults.MaxTableRetries;
                            attempt++)
                        {
                            try
                            {
                                await ProcessMigrationUnitAsync(job, config, migrationUnit, token);
                                break; // success
                            }
                            catch (Exception ex) when (CassandraClientFactory.IsRetryableException(ex)
                                && attempt < MigrationDefaults.MaxTableRetries)
                            {
                                int delayMs = CassandraClientFactory.GetRetryDelayMs(ex, attempt);
                                _log.WriteLine($"Table retry {attempt} for {migrationUnit.KeyspaceName}.{migrationUnit.TableName}: {ex.Message}",
                                    LogType.Warning);
                                await Task.Delay(delayMs, token);
                            }
                        }

                    });

                if (abortRequested)
                {
                    _log.WriteLine($"Aborting: {Volatile.Read(ref _consecutiveAuthErrors)}" + " consecutive auth failures.",
                        LogType.Error);
                    return TaskResult.Abort;
                }

                // All tables processed — handle completion by mode
                if (MigrationHelper.IsOnline(job))
                {
                    // Online: change feed already started per-table
                    // as each completed. Keep worker alive until
                    // pause/cancel.
                    _log.WriteLine("All tables copied. Change feed replaying.");
                    while (!cancellationToken.IsCancellationRequested
                        && !MigrationJobContext.ControlledPauseRequested)
                    {
                        await Task.Delay(2000, cancellationToken);
                    }
                }
                else
                {
                    // Offline mode — mark completed
                    if (MigrationHelper.IsOfflineJobCompleted(job)
                        && !MigrationJobContext.ControlledPauseRequested
                        && job.Status != JobStatus.Cancelled
                        && job.Status != JobStatus.Paused)
                    {
                        _log.WriteLine($"Job {job.Id} Completed");
                        job.Status = JobStatus.Completed;
                        MigrationJobContext.SaveMigrationJob(job);
                    }
                }

                return TaskResult.Success;
            }
            catch (OperationCanceledException)
            {
                _log.WriteLine("Migration was cancelled.");
                return TaskResult.Canceled;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Migration failed: {ex}", LogType.Error);
                return TaskResult.Abort;
            }
            finally
            {
                CleanupSession();
            }
        }

        /// <summary>
        /// Process a single migration unit: validate source/target
        /// tables, create schema if needed, capture change-feed
        /// tokens, and run the bulk copy.
        /// </summary>
        private async Task ProcessMigrationUnitAsync(MigrationJob job, MigrationSettings config,
            MigrationUnit migrationUnit,
            CancellationToken cancellationToken)
        {
            _log.WriteLine($"Processing {migrationUnit.KeyspaceName}.{migrationUnit.TableName}");

            // Reset Failed status on retry/resume so the table
            // gets a fresh chance (Bug 3 fix)
            if (migrationUnit.SourceStatus == TableStatus.Failed)
            {
                migrationUnit.SourceStatus = TableStatus.OK;
                MigrationJobContext.SaveMigrationUnit(migrationUnit, true);
            }

            // Each parallel table gets its own source session
            // so concurrent copies don't interfere.
            ISession? localSourceSession = null;
            try
            {
                localSourceSession = CassandraClientFactory.CreateSourceSession(_log, job, migrationUnit.KeyspaceName);

                // Validate table exists on source
                if (!await CassandraHelper.TableExistsAsync(localSourceSession!,
                    migrationUnit.KeyspaceName, migrationUnit.TableName))
                {
                    _log.WriteLine($"Source table {migrationUnit.KeyspaceName}.{migrationUnit.TableName} not found.",
                        LogType.Error);
                    migrationUnit.SourceStatus = TableStatus.NotFound;
                    MigrationJobContext.SaveMigrationUnit(migrationUnit, true);
                    return;
                }

                // Ensure target keyspace + table
                if (!job.IsSimulatedRun)
                {
                    using (var targetSession = CassandraClientFactory.CreateTargetSession(_log, job, string.Empty))
                    {
                        await CassandraHelper.EnsureKeyspaceExistsAsync(targetSession, migrationUnit.KeyspaceName);

                        if (job.DropTargetTableBeforeStart
                            && await CassandraHelper.TableExistsAsync(targetSession,
                                migrationUnit.KeyspaceName, migrationUnit.TableName))
                        {
                            _log.WriteLine($"Dropping target table {migrationUnit.KeyspaceName}.{migrationUnit.TableName} (DropTargetTableBeforeStart)");
                            await targetSession.ExecuteAsync(new SimpleStatement(
                                    $"DROP TABLE \"{migrationUnit.KeyspaceName}\"" +
                                    $".\"{migrationUnit.TableName}\""));
                        }

                        if (!await CassandraHelper.TableExistsAsync(targetSession,
                            migrationUnit.KeyspaceName, migrationUnit.TableName))
                        {
                            await CassandraHelper.CreateTableFromSourceAsync(localSourceSession!, targetSession,
                                migrationUnit.KeyspaceName, migrationUnit.TableName,
                                migrationUnit.KeyspaceName, migrationUnit.TableName);
                            _log.WriteLine($"Created target table {migrationUnit.KeyspaceName}.{migrationUnit.TableName}");
                        }
                        else
                        {
                            // Table exists — sync schema
                            // (adds missing columns via ALTER)
                            await CassandraHelper.CreateTableFromSourceAsync(localSourceSession!, targetSession,
                                migrationUnit.KeyspaceName, migrationUnit.TableName,
                                migrationUnit.KeyspaceName, migrationUnit.TableName);
                        }
                    }
                }

                migrationUnit.BulkCopyStartedOn ??= DateTime.UtcNow;

                // MigrationLog feed range count for this table
                if (!job.IsSimulatedRun)
                {
                    try
                    {
                        var rangeCount = (await CassandraHelper.GetFeedRangesAsync(localSourceSession!,
                                migrationUnit.KeyspaceName,
                                migrationUnit.TableName)
                                ).Count;
                        _log.WriteLine($"Feed ranges: {rangeCount} for {migrationUnit.KeyspaceName}.{migrationUnit.TableName}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[WARN] MigrationWorker GetFeedRanges failed: {ex.Message}");
                    }
                }

                // Record change feed start time before bulk copy
                if (MigrationHelper.IsOnline(job))
                {
                    migrationUnit.ChangeFeedStartToken ??= DateTime.UtcNow.ToString(
                        "yyyy-MM-ddTHH:mm:ss.fffZ",
                        System.Globalization.CultureInfo.InvariantCulture);
                }

                MigrationJobContext.SaveMigrationUnit(migrationUnit, true);

                var processor = new CopyProcessor(_log, localSourceSession!, config, job, this);
                _activeProcessors[migrationUnit.Id] = processor;

                cancellationToken.ThrowIfCancellationRequested();

                TaskResult result = await processor.StartProcessAsync(migrationUnit.Id);

                switch (result)
                {
                    case TaskResult.Success:
                        _log.WriteLine($"Copy succeeded for {migrationUnit.KeyspaceName}.{migrationUnit.TableName}");
                        break;

                    case TaskResult.Canceled:
                        _log.WriteLine($"Copy paused for {migrationUnit.KeyspaceName}.{migrationUnit.TableName}");
                        break;

                    default:
                        _log.WriteLine($"Copy failed for {migrationUnit.KeyspaceName}.{migrationUnit.TableName}",
                            LogType.Error);
                        break;
                }

                MigrationJobContext.SaveMigrationUnit(migrationUnit, true);
            }
            catch (OperationCanceledException)
            {
                _log.WriteLine($"Cancelled {migrationUnit.KeyspaceName}.{migrationUnit.TableName}");
                throw;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Error processing {migrationUnit.KeyspaceName}.{migrationUnit.TableName}: {ex}",
                    LogType.Error);

                // BB-3 fix: Mark MU as failed so the UI shows
                // "Failed" instead of "0.0%"
                migrationUnit.SourceStatus = TableStatus.Failed;

                // Detect auth errors (expired token)
                if (IsAuthError(ex))
                {
                    Interlocked.Increment(ref _consecutiveAuthErrors);
                    _log.WriteLine($"Auth failure #{Volatile.Read(ref _consecutiveAuthErrors)} on {migrationUnit.KeyspaceName}.{migrationUnit.TableName}",
                        LogType.Warning);
                }
                else
                {
                    Interlocked.Exchange(ref _consecutiveAuthErrors, 0);
                }

                MigrationJobContext.SaveMigrationUnit(migrationUnit, true);
            }
            finally
            {
                _activeProcessors.TryRemove(migrationUnit.Id, out var removedProcessor);
                MigrationHelper.SafeDispose(removedProcessor as IDisposable, "MigrationWorker processor");
                MigrationHelper.SafeDispose(localSourceSession, "MigrationWorker localSourceSession");
            }
        }

        private void EnsureSourceSession(MigrationJob job, string keyspace)
        {
            if (_sourceSession != null
                && !_sourceSession.IsDisposed)
            {
                return;
            }

            _sourceSession = CassandraClientFactory.CreateSourceSession(_log, job, keyspace);

            _log.WriteLine($"Source session created for {job.SourceContactPoint}");
        }

        private void CleanupSession()
        {
            MigrationHelper.SafeDispose(_sourceSession, "MigrationWorker source session");
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
                return nhae.Errors?.Values?.Any(e => e is Cassandra.AuthenticationException)
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
        public static List<MigrationUnit> DiscoverTables(MigrationLog MigrationLog, MigrationJob job)
        {
            MigrationJobContext.AddVerboseLog("MigrationWorker.DiscoverTables");

            var result = new List<MigrationUnit>();

            using (var session = CassandraClientFactory.CreateSourceSession(MigrationLog, job, "system"))
            {
                var keyspaces = CassandraHelper.ListKeyspaces(session);

                foreach (var ks in keyspaces)
                {
                    var tables = CassandraHelper.ListTables(session, ks);

                    foreach (var tbl in tables)
                    {
                        result.Add(new MigrationUnit(job, ks, tbl, new List<MigrationChunk>()));
                    }
                }
            }

            int ksCount = result
                .Select(r => r.KeyspaceName).Distinct().Count();
            MigrationLog.WriteLine($"Discovered {result.Count} tables across {ksCount} keyspaces");

            return result;
        }
    }
}
