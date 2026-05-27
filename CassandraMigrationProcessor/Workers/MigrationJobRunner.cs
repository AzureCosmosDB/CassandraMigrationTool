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

namespace CassandraMigrationProcessor.DataTransfer;
/// <summary>
/// Orchestrates a Cassandra-to-Cassandra migration with table-level
/// parallelism and optional change-feed replication.
/// </summary>
public class MigrationJobRunner
{
    private readonly MigrationLog _log;
    private int _consecutiveAuthErrors;
    private readonly ConcurrentDictionary<string, TableMigrationEngine> _activeProcessors = new();
    private readonly TokenRefreshManager _tokenRefreshManager;

    public MigrationJobRunner(MigrationLog migrationLog)
    {
        _log = migrationLog;
        _tokenRefreshManager = new TokenRefreshManager(migrationLog);
    }

    public async Task<TaskResult> StartAsync(Job job, AppSettings config,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(config);

        try
        {
            var units = UnitStore.GetMigrationUnitsToMigrate(job);

            if (units == null || units.Count == 0)
            {
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
                    if (MigrationJobContext.Instance.ControlledPauseRequested)
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
            _tokenRefreshManager.StopTokenRefreshTimer();
        }
    }

    private async Task ProcessWithRetryAsync(Job job, AppSettings config,
        TableMigration mu, CancellationToken token)
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

    private async Task HandleCompletionAsync(Job job, CancellationToken cancellationToken)
    {
        if (MigrationUtilities.IsOnline(job))
        {
            _log.WriteLine("All tables copied. Change feed replaying.", LogType.Info);
            while (!cancellationToken.IsCancellationRequested && !MigrationJobContext.Instance.ControlledPauseRequested)
                await Task.Delay(2000, cancellationToken);
        }
        else if (MigrationUtilities.IsOfflineJobCompleted(job)
            && !MigrationJobContext.Instance.ControlledPauseRequested
            && job.Status != JobStatus.Cancelled
            && job.Status != JobStatus.Paused)
        {
            _log.WriteLine($"Job {job.Id} Completed", LogType.Info);
            job.Status = JobStatus.Completed;
            MigrationJobContext.Instance.SaveMigrationJob(job);
        }
    }

    private async Task ProcessMigrationUnitAsync(Job job, AppSettings config,
        TableMigration mu, CancellationToken cancellationToken)
    {
        if (mu.SourceStatus == TableStatus.Failed)
        {
            mu.SourceStatus = TableStatus.OK;
            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
        }

        try
        {
            await SetupTargetSchemaAsync(job, mu);

            if (mu.BulkCopyPhase < BulkCopyPhase.Copying)
                mu.BulkCopyPhase = BulkCopyPhase.Copying;

            mu.BulkCopyStartedOn ??= DateTime.UtcNow;

            if (MigrationUtilities.IsOnline(job))
            {
                mu.ChangeFeedStartToken ??= DateTime.UtcNow.ToString(
                    "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
            }

            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
            await RunCopyForUnitAsync(job, config, mu, cancellationToken);
            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            HandleMigrationUnitError(mu, ex);
        }
        finally
        {
            // Online: leave the engine in _activeProcessors so its
            // long-lived DataCopyWorker pool keeps tailing the change
            // feed. Stop() will dispose it on shutdown. Offline:
            // dispose immediately — the table is done.
            if (!MigrationUtilities.IsOnline(job))
            {
                _activeProcessors.TryRemove(mu.Id, out var removed);
                MigrationUtilities.SafeDispose(removed as IDisposable, "MigrationJobRunner processor");
            }
        }
    }

    private async Task SetupTargetSchemaAsync(Job job, TableMigration mu)
    {
        if (job.SkipSchemaSync)
        {
            _log.WriteLine(
                $"Skipping schema sync for {mu.KeyspaceName}.{mu.TableName} (job.SkipSchemaSync is enabled — target schema is assumed to already exist).",
                LogType.Info);
            return;
        }

        if (mu.BulkCopyPhase >= BulkCopyPhase.Copying)
        {
            if (job.DropTargetTableBeforeStart)
            {
                _log.WriteLine(
                    $"Skipping DropTargetTableBeforeStart for {mu.KeyspaceName}.{mu.TableName} on resume (phase={mu.BulkCopyPhase})",
                    LogType.Debug);
            }
            return;
        }

        bool shouldDrop = mu.BulkCopyPhase == BulkCopyPhase.NotStarted
                       && job.DropTargetTableBeforeStart;

        if (mu.BulkCopyPhase == BulkCopyPhase.NotStarted)
        {
            mu.BulkCopyPhase = BulkCopyPhase.InitializingDestination;
            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
        }

        using var targetSession = await CassandraClientFactory.CreateTargetSessionAsync(_log, job, string.Empty);
        var sourceSession = CassandraClientFactory.CreateSourceSession(_log, job, mu.KeyspaceName, _tokenRefreshManager);
        try
        {
            if (shouldDrop
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
        finally
        {
            MigrationUtilities.SafeDispose(sourceSession, "SetupTargetSchemaAsync source session");
        }
    }

    private async Task RunCopyForUnitAsync(Job job, AppSettings config,
        TableMigration mu, CancellationToken ct)
    {
        var processor = await TableMigrationEngine.CreateAsync(_log, config, job, _tokenRefreshManager, ct);
        _activeProcessors[mu.Id] = processor;
        ct.ThrowIfCancellationRequested();

        TaskResult result = await processor.MigrateTableAsync(mu.Id);

        if (result == TaskResult.Success)
            _log.WriteLine($"Copy succeeded for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
        else if (result == TaskResult.Canceled)
            _log.WriteLine($"Copy paused for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
        else
            _log.WriteLine($"Copy failed for {mu.KeyspaceName}.{mu.TableName}", LogType.Error);
    }

    private void HandleMigrationUnitError(TableMigration mu, Exception ex)
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

        MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
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
        foreach (var kvp in _activeProcessors)
            MigrationUtilities.SafeDispose(kvp.Value, "MigrationJobRunner processor (Stop)");
        _activeProcessors.Clear();
        _tokenRefreshManager.StopTokenRefreshTimer();
    }

}
