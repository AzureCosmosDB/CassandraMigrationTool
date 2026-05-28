using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System.Collections.Concurrent;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Orchestrates a Cassandra-to-Cassandra migration with a single
/// job-wide worker pool. All tables share the same pool of
/// <see cref="PipelineConfig.WorkerCount"/> workers; there is no
/// table-level worker parallelism. Destination schema provisioning
/// is done serially up front (Phase 1); <see cref="Job.ParallelThreads"/>
/// only caps how many tables can be in copy orchestration
/// concurrently in Phase 2 (seeding partitions / awaiting drain),
/// not steady-state row throughput.
/// </summary>
public class MigrationJobRunner
{
    private readonly MigrationLog _log;
    private int _consecutiveAuthErrors;
    private readonly ConcurrentDictionary<string, TableCopyCoordinator> _activeProcessors = new();
    private readonly TokenRefreshManager _tokenRefreshManager;
    private JobPipeline? _pipeline;

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

            var pipelineConfig = PipelineConfig.Resolve(job, config);
            int copyParallelism = Math.Max(1, Math.Min(job.ParallelThreads, units.Count));
            _log.WriteLine(
                $"Migrating {units.Count} tables with {pipelineConfig.WorkerCount} shared workers " +
                $"(copy orchestration parallelism={copyParallelism})", LogType.Info);

            _pipeline = new JobPipeline(_log, job, pipelineConfig, _tokenRefreshManager, cancellationToken);
            _pipeline.Start();

            // Phase 1: provision destination schema for every table serially.
            // Schema sync issues DDL (CREATE KEYSPACE / CREATE TYPE / CREATE TABLE)
            // and is cheap per table; running it serially avoids concurrent
            // schema-change traffic on the target cluster and ensures we fail
            // fast on bad schema before any row data starts flowing.
            var schemaFailed = await ProvisionAllSchemasAsync(job, units, cancellationToken);

            var abortRequested = false;
            // Phase 2: parallel copy orchestration. The shared worker pool
            // owned by JobPipeline does the actual row-level parallelism;
            // copyParallelism only caps how many tables can be seeding
            // partitions / awaiting drain at the same time.
            await Parallel.ForEachAsync(units, new ParallelOptions
                {
                    MaxDegreeOfParallelism = copyParallelism,
                    CancellationToken = cancellationToken
                },
                async (mu, token) =>
                {
                    if (MigrationJobContext.Instance.ControlledPauseRequested)
                        return;
                    if (schemaFailed.Contains(mu.Id))
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

            // All tables have either drained or completed. Online jobs
            // keep the shared pool alive for change-feed tailing; offline
            // jobs close the channel so workers exit.
            if (job.IsOnline)
            {
                _log.WriteLine("All tables drained. Change feed replaying on shared worker pool.", LogType.Info);
                while (!cancellationToken.IsCancellationRequested
                    && !MigrationJobContext.Instance.ControlledPauseRequested)
                {
                    // Online change-feed mode keeps the shared worker pool alive
                    // indefinitely. If every worker has died (faults or fatal trip)
                    // the loop would otherwise wait forever while no rows are being
                    // copied — silent data loss. Probe the pool each tick.
                    if (_pipeline!.AllWorkersExited)
                    {
                        int faulted = _pipeline.FaultedWorkerCount;
                        _log.WriteLine(
                            $"Online worker pool has stopped (faulted={faulted}). Aborting job.",
                            LogType.Error);
                        return TaskResult.Abort;
                    }
                    if (Volatile.Read(ref _pipeline.Context.Flags.FatalErrorFlag) == 1)
                    {
                        _log.WriteLine("Fatal error tripped during online replay. Aborting job.", LogType.Error);
                        return TaskResult.Abort;
                    }
                    await Task.Delay(2000, cancellationToken);
                }
            }
            else
            {
                _pipeline.CompletePartitionChannel();
                await _pipeline.WaitForCompletionAsync();
                await HandleOfflineCompletionAsync(job);
            }

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
            MigrationUtilities.SafeDispose(_pipeline, "JobPipeline");
            _pipeline = null;
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
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // Non-retryable, or retryable but final attempt. Record the
                // unit-level failure and stop retrying this table. Other
                // tables in the Parallel.ForEachAsync continue running.
                HandleMigrationUnitError(mu, ex);
                return;
            }
        }
    }

    private async Task HandleOfflineCompletionAsync(Job job)
    {
        if (job.IsOfflineCompleted
            && !MigrationJobContext.Instance.ControlledPauseRequested
            && job.Status != JobStatus.Cancelled
            && job.Status != JobStatus.Paused)
        {
            _log.WriteLine($"Job {job.Id} Completed", LogType.Info);
            job.Status = JobStatus.Completed;
            MigrationJobContext.Instance.SaveMigrationJob(job);
        }
        await Task.CompletedTask;
    }

    /// <summary>
    /// Phase 1 of <see cref="StartAsync"/>: provision destination schema
    /// for every table serially. Failures are recorded on the unit
    /// (<see cref="HandleMigrationUnitError"/>) and the unit ID is
    /// returned so Phase 2 can skip it without re-running provision
    /// against a broken destination.
    /// </summary>
    private async Task<HashSet<string>> ProvisionAllSchemasAsync(
        Job job, IReadOnlyList<TableMigration> units, CancellationToken ct)
    {
        var failed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var mu in units)
        {
            ct.ThrowIfCancellationRequested();
            if (MigrationJobContext.Instance.ControlledPauseRequested)
                break;
            if (Volatile.Read(ref _consecutiveAuthErrors) >= MigrationDefaults.MaxConsecutiveAuthErrors)
                break;

            try
            {
                await ProvisionTargetSchemaAsync(job, mu);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                HandleMigrationUnitError(mu, ex);
                failed.Add(mu.Id);
            }
        }
        return failed;
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
            // Destination schema was provisioned up front in Phase 1.
            if (mu.BulkCopyPhase < BulkCopyPhase.Copying)
                mu.BulkCopyPhase = BulkCopyPhase.Copying;

            mu.BulkCopyStartedOn ??= DateTime.UtcNow;
            if (job.IsOnline)
            {
                mu.ChangeFeedStartToken ??= DateTime.UtcNow.ToString(
                    "yyyy-MM-ddTHH:mm:ss.fffZ", System.Globalization.CultureInfo.InvariantCulture);
            }

            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
            await RunCopyForUnitAsync(job, config, mu, cancellationToken);
            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
        }
        finally
        {
            // All tables can release their coordinators once MigrateTableAsync
            // returns — the worker pool is owned by the JobPipeline now,
            // not by the coordinator.
            _activeProcessors.TryRemove(mu.Id, out var removed);
            MigrationUtilities.SafeDispose(removed as IDisposable, "MigrationJobRunner processor");
        }
    }

    /// <summary>
    /// Owns destination schema provisioning for a single table:
    /// optional drop, then keyspace + UDTs + table creation via
    /// <see cref="SchemaManager.SyncSchemaAsync"/>. Runs exactly once
    /// per table, before <see cref="TableCopyCoordinator"/> is invoked.
    /// The coordinator fetches the source column list it needs to build
    /// writers via the cheap <see cref="SchemaManager.GetTableColumnsAsync"/>
    /// path and does no DDL of its own.
    /// </summary>
    private async Task ProvisionTargetSchemaAsync(Job job, TableMigration mu)
    {
        if (job.SkipSchemaSync)
        {
            _log.WriteLine(
                $"Skipping schema sync for {mu.KeyspaceName}.{mu.TableName} (job.SkipSchemaSync is enabled).",
                LogType.Info);
            return;
        }

        if (mu.BulkCopyPhase >= BulkCopyPhase.Copying)
            return;

        bool shouldDrop = mu.BulkCopyPhase == BulkCopyPhase.NotStarted
                       && job.DropTargetTableBeforeStart;

        if (mu.BulkCopyPhase == BulkCopyPhase.NotStarted)
        {
            mu.BulkCopyPhase = BulkCopyPhase.InitializingDestination;
            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
        }

        var targetSession = await CassandraClientFactory.CreateTargetSessionAsync(_log, job);
        // Keyspace-agnostic source session: SchemaManager queries hit system_schema with
        // parameterized keyspace_name and all data CQL is fully qualified, so we avoid the
        // extra USE keyspace round trip and per-keyspace metadata refresh.
        var sourceSession = CassandraClientFactory.CreateSourceSession(_log, job, _tokenRefreshManager);
        try
        {
            if (shouldDrop
                && await SchemaManager.TableExistsAsync(targetSession, mu.KeyspaceName, mu.TableName))
            {
                _log.WriteLine($"Dropping target table {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
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
            MigrationUtilities.SafeDisposeSession(sourceSession, "ProvisionTargetSchemaAsync source session");
            MigrationUtilities.SafeDisposeSession(targetSession, "ProvisionTargetSchemaAsync target session");
        }
    }

    private async Task RunCopyForUnitAsync(Job job, AppSettings config,
        TableMigration mu, CancellationToken ct)
    {
        var processor = await TableCopyCoordinator.CreateAsync(_log, config, job, _pipeline!, _tokenRefreshManager, ct);
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
            kvp.Value?.Cancel();
        foreach (var kvp in _activeProcessors)
            MigrationUtilities.SafeDispose(kvp.Value, "MigrationJobRunner processor (Stop)");
        _activeProcessors.Clear();
        _pipeline?.Stop();
        MigrationUtilities.SafeDispose(_pipeline, "JobPipeline (Stop)");
        _pipeline = null;
        _tokenRefreshManager.StopTokenRefreshTimer();
    }
}
