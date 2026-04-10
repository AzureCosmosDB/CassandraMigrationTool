using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using CassandraMigrationProcessor.DataTransfer;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CassandraMigrationProcessor.DataTransfer
{
    /// <summary>
    /// Orchestrates a Cassandra-to-Cassandra migration with table-level
    /// parallelism and optional change-feed replication.
    /// </summary>
    public class MigrationWorker
    {
        private readonly MigrationLog _log;
        private BulkCopyEngine? _activeProcessor;
        private ISession? _sourceSession;
        private int _consecutiveAuthErrors;
        private readonly ConcurrentDictionary<string, BulkCopyEngine> _activeProcessors = new();

        public MigrationWorker(MigrationLog migrationLog) => _log = migrationLog;

        public async Task<TaskResult> StartAsync(MigrationJob job, MigrationSettings config,
            CancellationToken cancellationToken)
        {
            try
            {
                var units = MigrationUtilities.GetMigrationUnitsToMigrate(job);

                if (units == null || units.Count == 0)
                {
                    if (MigrationUtilities.IsOnline(job)
                        && MigrationUtilities.IsOfflineJobCompleted(job)
                        && MigrationUtilities.AnyValidTable(job))
                        return await ResumeChangeFeedAsync(job, config, cancellationToken);

                    _log.WriteLine("No remaining migration units.", LogType.Warning);
                    return TaskResult.Success;
                }

                int maxParallel = Math.Max(1, Math.Min(job.ParallelThreads, units.Count));
                _log.WriteLine($"Migrating {units.Count} tables with max parallelism={maxParallel}", LogType.Info);

                var abortRequested = false;

                await Parallel.ForEachAsync(units, new ParallelOptions
                    {
                        MaxDegreeOfParallelism = maxParallel,
                        CancellationToken = cancellationToken
                    },
                    async (mu, token) =>
                    {
                        if (MigrationJobContext.ControlledPauseRequested)
                            return;
                        if (Volatile.Read(ref _consecutiveAuthErrors) >= MigrationDefaults.MaxConsecutiveAuthErrors)
                        {
                            abortRequested = true;
                            return;
                        }
                        await ProcessWithRetryAsync(job, config, mu, token);
                    });

                if (abortRequested)
                {
                    _log.WriteLine($"Aborting: {Volatile.Read(ref _consecutiveAuthErrors)} consecutive auth failures.", LogType.Error);
                    return TaskResult.Abort;
                }

                await HandleCompletionAsync(job, cancellationToken);
                return TaskResult.Success;
            }
            catch (OperationCanceledException)
            {
                _log.WriteLine("Migration was cancelled.", LogType.Info);
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

        private async Task<TaskResult> ResumeChangeFeedAsync(MigrationJob job, MigrationSettings config,
            CancellationToken cancellationToken)
        {
            _log.WriteLine("All tables copied. Resuming change feed processors.", LogType.Info);
            EnsureSourceSession(job, job.Tables!.First().KeyspaceName);
            _activeProcessor = new BulkCopyEngine(_log, _sourceSession!, config, job, this);

            foreach (var mub in job.Tables)
            {
                if (!MigrationUtilities.IsMigrationUnitValid(mub) || !mub.CopyComplete)
                    continue;
                var mu = MigrationJobContext.GetMigrationUnit(mub.Id);
                if (mu != null)
                    _activeProcessor.ChangeFeed.AddTable(mu, CancellationToken.None);
            }

            while (!cancellationToken.IsCancellationRequested && !MigrationJobContext.ControlledPauseRequested)
                await Task.Delay(2000, cancellationToken);

            return cancellationToken.IsCancellationRequested ? TaskResult.Canceled : TaskResult.Success;
        }

        private async Task ProcessWithRetryAsync(MigrationJob job, MigrationSettings config,
            MigrationUnit mu, CancellationToken token)
        {
            for (int attempt = 1; attempt <= MigrationDefaults.MaxTableRetries; attempt++)
            {
                try
                {
                    await ProcessMigrationUnitAsync(job, config, mu, token);
                    return;
                }
                catch (Exception ex) when (CassandraClientFactory.IsRetryableException(ex)
                    && attempt < MigrationDefaults.MaxTableRetries)
                {
                    int delayMs = CassandraClientFactory.GetRetryDelayMs(ex, attempt);
                    _log.WriteLine($"Table retry {attempt} for {mu.KeyspaceName}.{mu.TableName}: {ex.Message}", LogType.Warning);
                    await Task.Delay(delayMs, token);
                }
            }
        }

        private async Task HandleCompletionAsync(MigrationJob job, CancellationToken cancellationToken)
        {
            if (MigrationUtilities.IsOnline(job))
            {
                _log.WriteLine("All tables copied. Change feed replaying.", LogType.Info);
                while (!cancellationToken.IsCancellationRequested && !MigrationJobContext.ControlledPauseRequested)
                    await Task.Delay(2000, cancellationToken);
            }
            else if (MigrationUtilities.IsOfflineJobCompleted(job)
                && !MigrationJobContext.ControlledPauseRequested
                && job.Status != JobStatus.Cancelled
                && job.Status != JobStatus.Paused)
            {
                _log.WriteLine($"Job {job.Id} Completed", LogType.Info);
                job.Status = JobStatus.Completed;
                MigrationJobContext.SaveMigrationJob(job);
            }
        }

        private async Task ProcessMigrationUnitAsync(MigrationJob job, MigrationSettings config,
            MigrationUnit mu, CancellationToken cancellationToken)
        {
            if (mu.SourceStatus == TableStatus.Failed)
            {
                mu.SourceStatus = TableStatus.OK;
                MigrationJobContext.SaveMigrationUnit(mu, true);
            }

            ISession? localSourceSession = null;
            try
            {
                localSourceSession = CassandraClientFactory.CreateSourceSession(_log, job, mu.KeyspaceName);

                if (!await SchemaManager.TableExistsAsync(localSourceSession!, mu.KeyspaceName, mu.TableName))
                {
                    _log.WriteLine($"Source table {mu.KeyspaceName}.{mu.TableName} not found.", LogType.Error);
                    mu.SourceStatus = TableStatus.NotFound;
                    MigrationJobContext.SaveMigrationUnit(mu, true);
                    return;
                }

                if (!job.IsSimulatedRun)
                    await SetupTargetSchemaAsync(job, localSourceSession!, mu);

                mu.BulkCopyStartedOn ??= DateTime.UtcNow;

                if (!job.IsSimulatedRun)
                    await LogFeedRangesAsync(localSourceSession!, mu);

                if (MigrationUtilities.IsOnline(job))
                {
                    mu.ChangeFeedStartToken ??= DateTime.UtcNow.ToString(
                        "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
                }

                MigrationJobContext.SaveMigrationUnit(mu, true);
                await RunCopyForUnitAsync(job, config, localSourceSession!, mu, cancellationToken);
                MigrationJobContext.SaveMigrationUnit(mu, true);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex)
            {
                HandleMigrationUnitError(mu, ex);
            }
            finally
            {
                _activeProcessors.TryRemove(mu.Id, out var removed);
                MigrationUtilities.SafeDispose(removed as IDisposable, "MigrationWorker processor");
                MigrationUtilities.SafeDispose(localSourceSession, "MigrationWorker localSourceSession");
            }
        }

        private async Task SetupTargetSchemaAsync(MigrationJob job, ISession sourceSession, MigrationUnit mu)
        {
            using var targetSession = CassandraClientFactory.CreateTargetSession(_log, job, string.Empty);

            if (job.DropTargetTableBeforeStart
                && await SchemaManager.TableExistsAsync(targetSession, mu.KeyspaceName, mu.TableName))
            {
                _log.WriteLine($"Dropping target table {mu.KeyspaceName}.{mu.TableName} (DropTargetTableBeforeStart)", LogType.Info);
                await targetSession.ExecuteAsync(new SimpleStatement(
                    $"DROP TABLE \"{mu.KeyspaceName}\".\"{mu.TableName}\""));
            }

            bool existed = await SchemaManager.TableExistsAsync(targetSession, mu.KeyspaceName, mu.TableName);

            await SchemaManager.SyncSchemaAsync(sourceSession, targetSession,
                mu.KeyspaceName, mu.TableName, mu.KeyspaceName, mu.TableName);

            if (!existed)
                _log.WriteLine($"Created target table {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
        }

        private async Task RunCopyForUnitAsync(MigrationJob job, MigrationSettings config,
            ISession sourceSession, MigrationUnit mu, CancellationToken ct)
        {
            var processor = new BulkCopyEngine(_log, sourceSession, config, job, this);
            _activeProcessors[mu.Id] = processor;
            ct.ThrowIfCancellationRequested();

            TaskResult result = await processor.StartProcessAsync(mu.Id);

            if (result == TaskResult.Success)
                _log.WriteLine($"Copy succeeded for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
            else if (result == TaskResult.Canceled)
                _log.WriteLine($"Copy paused for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
            else
                _log.WriteLine($"Copy failed for {mu.KeyspaceName}.{mu.TableName}", LogType.Error);
        }

        private async Task LogFeedRangesAsync(ISession session, MigrationUnit mu)
        {
            try
            {
                var rangeCount = (await CassandraQueries.GetFeedRangesAsync(session,
                    mu.KeyspaceName, mu.TableName)).Count;
                _log.WriteLine($"Feed ranges: {rangeCount} for {mu.KeyspaceName}.{mu.TableName}", LogType.Debug);
            }
            catch (Exception ex)
            {
                _log.WriteLine($"GetFeedRanges failed: {ex.Message}", LogType.Warning);
            }
        }

        private void HandleMigrationUnitError(MigrationUnit mu, Exception ex)
        {
            _log.WriteLine($"Error processing {mu.KeyspaceName}.{mu.TableName}: {ex}", LogType.Error);
            mu.SourceStatus = TableStatus.Failed;

            if (IsAuthError(ex))
            {
                Interlocked.Increment(ref _consecutiveAuthErrors);
                _log.WriteLine($"Auth failure #{Volatile.Read(ref _consecutiveAuthErrors)} on {mu.KeyspaceName}.{mu.TableName}", LogType.Warning);
            }
            else
            {
                Interlocked.Exchange(ref _consecutiveAuthErrors, 0);
            }

            MigrationJobContext.SaveMigrationUnit(mu, true);
        }

        private void EnsureSourceSession(MigrationJob job, string keyspace)
        {
            if (_sourceSession != null && !_sourceSession.IsDisposed)
                return;
            _sourceSession = CassandraClientFactory.CreateSourceSession(_log, job, keyspace);
        }

        private void CleanupSession()
        {
            MigrationUtilities.SafeDispose(_sourceSession, "MigrationWorker source session");
            _sourceSession = null;
        }

        private static bool IsAuthError(Exception ex)
        {
            if (ex is Cassandra.AuthenticationException)
                return true;
            if (ex.InnerException is Cassandra.AuthenticationException)
                return true;
            if (ex is Cassandra.NoHostAvailableException nhae)
                return nhae.Errors?.Values?.Any(e => e is Cassandra.AuthenticationException) ?? false;
            return false;
        }

        public void Stop()
        {
            foreach (var kvp in _activeProcessors)
                kvp.Value?.StopProcessing();
            _activeProcessors.Clear();
            _activeProcessor?.StopProcessing();
            CleanupSession();
        }

        public static List<MigrationUnit> DiscoverTables(MigrationLog migrationLog, MigrationJob job)
        {
            var result = new List<MigrationUnit>();

            using (var session = CassandraClientFactory.CreateSourceSession(migrationLog, job, "system"))
            {
                foreach (var ks in CassandraQueries.ListKeyspaces(session))
                    foreach (var tbl in CassandraQueries.ListTables(session, ks))
                        result.Add(new MigrationUnit(job, ks, tbl, new List<MigrationChunk>()));
            }

            int ksCount = result.Select(r => r.KeyspaceName).Distinct().Count();
            migrationLog.WriteLine($"Discovered {result.Count} tables across {ksCount} keyspaces", LogType.Info);
            return result;
        }
    }
}
