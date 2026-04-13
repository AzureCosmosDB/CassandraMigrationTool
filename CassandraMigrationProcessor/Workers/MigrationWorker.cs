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
public class MigrationWorker
{
    private readonly MigrationLog _log;
    private BulkCopyEngine? _activeProcessor;
    private ISession? _sourceSession;
    private int _consecutiveAuthErrors;
    private readonly ConcurrentDictionary<string, BulkCopyEngine> _activeProcessors = new();
    private readonly TokenRefreshManager _tokenRefreshManager;

    public MigrationWorker(MigrationLog migrationLog)
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

    private async Task<TaskResult> ResumeChangeFeedAsync(Job job, AppSettings config,
        CancellationToken cancellationToken)
    {
        _log.WriteLine("All tables copied. Resuming change feed processors.", LogType.Info);
        EnsureSourceSession(job, job.Tables.First().KeyspaceName);
        var sourceSession = _sourceSession ?? throw new InvalidOperationException(
            "Source session not initialized after EnsureSourceSession");
        _activeProcessor = new BulkCopyEngine(_log, sourceSession, config, job, _tokenRefreshManager);

        foreach (var mub in job.Tables)
        {
            if (!MigrationUtilities.IsMigrationUnitValid(mub) || !mub.CopyComplete)
                continue;
            var mu = MigrationJobContext.GetMigrationUnit(mub.Id);
            if (mu != null)
                await _activeProcessor.ChangeFeed.AddTable(mu, CancellationToken.None);
        }

        while (!cancellationToken.IsCancellationRequested && !MigrationJobContext.ControlledPauseRequested)
            await Task.Delay(2000, cancellationToken);

        return cancellationToken.IsCancellationRequested ? TaskResult.Canceled : TaskResult.Success;
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

    private async Task ProcessMigrationUnitAsync(Job job, AppSettings config,
        TableMigration mu, CancellationToken cancellationToken)
    {
        if (mu.SourceStatus == TableStatus.Failed)
        {
            mu.SourceStatus = TableStatus.OK;
            MigrationJobContext.SaveMigrationUnit(mu, true);
        }

        ISession? localSourceSession = null;
        try
        {
            localSourceSession = CassandraClientFactory.CreateSourceSession(_log, job, mu.KeyspaceName, _tokenRefreshManager);
            var session = localSourceSession ?? throw new InvalidOperationException(
                "CreateSourceSession returned null");

            if (!await SchemaManager.TableExistsAsync(session, mu.KeyspaceName, mu.TableName))
            {
                _log.WriteLine($"Source table {mu.KeyspaceName}.{mu.TableName} not found.", LogType.Error);
                mu.SourceStatus = TableStatus.NotFound;
                MigrationJobContext.SaveMigrationUnit(mu, true);
                return;
            }

            if (!job.IsSimulatedRun)
                await SetupTargetSchemaAsync(job, session, mu);

            mu.BulkCopyStartedOn ??= DateTime.UtcNow;

            if (!job.IsSimulatedRun)
                await LogFeedRangesAsync(session, mu);

            if (MigrationUtilities.IsOnline(job))
            {
                mu.ChangeFeedStartToken ??= DateTime.UtcNow.ToString(
                    "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
            }

            MigrationJobContext.SaveMigrationUnit(mu, true);
            await RunCopyForUnitAsync(job, config, session, mu, cancellationToken);
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

    private async Task SetupTargetSchemaAsync(Job job, ISession sourceSession, TableMigration mu)
    {
        using var targetSession = await CassandraClientFactory.CreateTargetSessionAsync(_log, job, string.Empty);

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

    private async Task RunCopyForUnitAsync(Job job, AppSettings config,
        ISession sourceSession, TableMigration mu, CancellationToken ct)
    {
        var processor = new BulkCopyEngine(_log, sourceSession, config, job, _tokenRefreshManager);
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

    private async Task LogFeedRangesAsync(ISession session, TableMigration mu)
    {
        try
        {
            var rangeCount = (await CassandraQueries.GetFeedRangesAsync(session,
                mu.KeyspaceName, mu.TableName,
                msg => MigrationJobContext.AddVerboseLog(msg))).Count;
            _log.WriteLine($"Feed ranges: {rangeCount} for {mu.KeyspaceName}.{mu.TableName}", LogType.Debug);
        }
        catch (Exception ex)
        {
            _log.WriteLine($"GetFeedRanges failed: {ex.Message}", LogType.Warning);
        }
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

        MigrationJobContext.SaveMigrationUnit(mu, true);
    }

    private void EnsureSourceSession(Job job, string keyspace)
    {
        if (_sourceSession != null && !_sourceSession.IsDisposed)
            return;
        _sourceSession = CassandraClientFactory.CreateSourceSession(_log, job, keyspace, _tokenRefreshManager);
    }

    private void CleanupSession()
    {
        _tokenRefreshManager.StopTokenRefreshTimer();
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

}
