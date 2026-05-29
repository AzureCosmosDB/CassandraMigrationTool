using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Orchestrates a Cassandra-to-Cassandra migration with a single
/// job-wide worker pool. All tables share the same pool of
/// <see cref="PipelineConfig.WorkerCount"/> workers; there is no
/// table-level worker parallelism. Destination schema provisioning
/// (Phase 1) and copy orchestration (Phase 3) both fan out across
/// every table concurrently — the shared pool gates steady-state
/// row throughput.
/// </summary>
public class MigrationJobRunner
{
    private readonly MigrationLog _log;
    private int _consecutiveAuthErrors;
    private readonly TokenRefreshManager _tokenRefreshManager;
    private JobPipeline? _pipeline;

    /// <summary>
    /// Per-run in-memory cache of <see cref="TableMigration"/> documents
    /// keyed by <c>jobId::unitId</c>. Lives for the duration of the
    /// run — created with the runner, dropped when the parent
    /// (<c>JobManager</c>) releases the runner reference. Replaces the
    /// previous process-wide singleton on <c>MigrationJobContext</c>,
    /// which leaked entries for every job ever run until the process
    /// recycled.
    /// </summary>
    public TableMigrationCache MigrationUnitsCache { get; }
        = new TableMigrationCache();

    public MigrationJobRunner(MigrationLog migrationLog)
    {
        _log = migrationLog;
        _tokenRefreshManager = new TokenRefreshManager(migrationLog);
    }

    public async Task StartAsync(Job job, AppSettings config,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(config);

        try
        {
            var units = UnitStore.GetMigrationUnitsToMigrate(job);
            if (units.Count == 0)
            {
                _log.WriteLine("No remaining migration units.", LogType.Warning);
                return;
            }

            var pipelineConfig = PipelineConfig.Resolve(job, config);
            _log.WriteLine(
                $"Migrating {units.Count} tables with {pipelineConfig.WorkerCount} shared workers");

            // Phase 1: provision destination schema for every table serially.
            var schemaFailed = await RunSchemaPhaseAsync(job, units, cancellationToken);

            // Phase 2: discover partitioning. All source-side I/O
            // (columns / feed ranges / row counts) happens here so the
            // worker pool is constructed with the full partition list.
            var partitioning = await RunDiscoveryPhaseAsync(
                job, units, schemaFailed, cancellationToken);

            _pipeline = new JobPipeline(_log, job, pipelineConfig, partitioning, _tokenRefreshManager, cancellationToken);
            _pipeline.Start();

            // Phase 3: parallel copy orchestration. The shared worker
            // pool owned by JobPipeline does the actual row-level
            // parallelism; here every table's orchestration loop runs
            // concurrently and competes for those shared workers.
            await RunCopyPhaseAsync(job, units, partitioning, schemaFailed, cancellationToken);

            if (Volatile.Read(ref _consecutiveAuthErrors) >= MigrationDefaults.MaxConsecutiveAuthErrors)
            {
                _log.WriteLine($"Aborting: {Volatile.Read(ref _consecutiveAuthErrors)} consecutive auth failures.", LogType.Error);
                return;
            }

            // All tables have either drained or completed. Online jobs
            // keep the shared pool alive for change-feed tailing; offline
            // jobs close the channel so workers exit.
            if (job.IsOnline)
                await RunOnlineTailLoopAsync(cancellationToken);
            else
                await RunOfflineFinalizeAsync(job);
        }
        catch (OperationCanceledException)
        {
            _log.WriteLine("Migration was cancelled.", LogType.Info);
        }
        catch (Exception ex)
        {
            _log.WriteLine($"Migration failed: {ex}", LogType.Error);
        }
        finally
        {
            _tokenRefreshManager.StopTokenRefreshTimer();
            MigrationUtilities.SafeDispose(_pipeline, "JobPipeline");
            _pipeline = null;
        }
    }

    /// <summary>
    /// Phase 1 of <see cref="StartAsync"/>: provision destination
    /// schema for every table in parallel (bounded by the job's
    /// copy parallelism). Failures are recorded on the unit
    /// (<see cref="HandleMigrationUnitError"/>) and the unit ID
    /// is returned so later phases skip it.
    /// </summary>
    private async Task<HashSet<string>> RunSchemaPhaseAsync(
        Job job, IReadOnlyList<TableMigration> units, CancellationToken ct)
    {
        var failed = new HashSet<string>(StringComparer.Ordinal);
        if (job.IsSimulatedRun)
        {
            _log.WriteLine("Simulated run: skipping target schema provisioning.", LogType.Info);
            return failed;
        }

        _log.WriteLine($"=== Phase 1: Schema — {units.Count} table(s) ===", LogType.Info);
        // Schema provisioning is per-table independent (each table
        // opens its own source + target sessions for one CREATE
        // KEYSPACE / CREATE TYPE / CREATE TABLE round-trip). Running
        // it serially blocked the entire copy phase for ~25s per
        // table on Cosmos source clusters with high DDL latency
        // (e.g. 11 tables took ~4.5 minutes before any data flowed).
        var failedLock = new object();

        await Task.WhenAll(units.Select(async mu =>
        {
            if (ct.IsCancellationRequested)
                return;
            if (Volatile.Read(ref _consecutiveAuthErrors) >= MigrationDefaults.MaxConsecutiveAuthErrors)
                return;
            ct.ThrowIfCancellationRequested();

            _log.WriteLine($"[Schema] Provisioning target for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
            try
            {
                await ProvisionTargetSchemaAsync(job, mu);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                lock (failedLock)
                {
                    HandleMigrationUnitError(mu, ex);
                    failed.Add(mu.Id);
                }
            }
        }));

        return failed;
    }

    /// <summary>
    /// Phase 2: walk every still-eligible migration unit and gather
    /// all source-side state we need to build the worker pool —
    /// columns, feed-range list, per-chunk row count — and call
    /// <see cref="Partitioner.Partition"/> to produce a
    /// <see cref="TablePartitioning"/> for each chunk. All work uses a
    /// single transient source session so the JobPipeline can be
    /// constructed with the full partition list and no runtime
    /// "seeding" path is required.
    /// </summary>
    private async Task<JobPartitioning> RunDiscoveryPhaseAsync(
        Job job,
        IReadOnlyList<TableMigration> units,
        HashSet<string> schemaFailed,
        CancellationToken ct)
    {
        var chunks = new List<TablePartitioning>();

        _log.WriteLine($"=== Phase 2: Partition discovery ===", LogType.Info);
        var partitioner = new Partitioner(_log);
        var sourceSession = CassandraClientFactory.CreateSourceSession(_log, job, _tokenRefreshManager);
        try
        {
            foreach (var mu in units)
            {
                ct.ThrowIfCancellationRequested();
                if (schemaFailed.Contains(mu.Id))
                    continue;
                if (mu.CopyComplete && !job.IsOnline)
                    continue;

                _log.WriteLine($"[Discovery] Discovering partitions for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
                try
                {
                    await DiscoverUnitPartitioningAsync(job, mu, sourceSession, partitioner, chunks);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    HandleMigrationUnitError(mu, ex);
                    schemaFailed.Add(mu.Id);
                }
            }
        }
        finally
        {
            MigrationUtilities.SafeDisposeSession(sourceSession, "RunDiscoveryPhaseAsync source session");
        }
        return new JobPartitioning(chunks);
    }

    /// <summary>
    /// Phase 3: orchestrate per-table copy concurrently. All tables
    /// share the worker pool owned by JobPipeline; this loop just
    /// kicks off each table's drain so the pool stays saturated.
    /// </summary>
    private Task RunCopyPhaseAsync(
        Job job,
        IReadOnlyList<TableMigration> units,
        JobPartitioning partitioning,
        HashSet<string> schemaFailed,
        CancellationToken cancellationToken)
    {
        _log.WriteLine($"=== Phase 3: Copy — {units.Count} table(s) concurrent ===", LogType.Info);
        // Catch each table's fault inside the loop body so every
        // table either completes, is observably faulted, or is
        // observably cancelled — never silently elided by a sibling.
        return Task.WhenAll(units.Select(async mu =>
        {
            if (cancellationToken.IsCancellationRequested)
                return;
            if (schemaFailed.Contains(mu.Id))
                return;
            if (Volatile.Read(ref _consecutiveAuthErrors) >= MigrationDefaults.MaxConsecutiveAuthErrors)
            {
                _pipeline?.Stop();
                return;
            }
            _log.WriteLine($"[Copy] Copying data for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
            try
            {
                await ProcessWithRetryAsync(job, mu, partitioning, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                // Pause-driven cancel propagated from the coordinator;
                // not a fault, just a graceful early exit.
            }
            catch (Exception ex)
            {
                _log.WriteLine(
                    $"Table {mu.KeyspaceName}.{mu.TableName} faulted: {ex.GetType().FullName}: {ex.Message}",
                    LogType.Error);
                if (ex.StackTrace != null)
                    _log.WriteLine($"  at {ex.StackTrace}", LogType.Error);
            }
        }));
    }

    /// <summary>
    /// Online change-feed mode keeps the shared worker pool alive
    /// indefinitely. If every worker has died (faults or fatal trip)
    /// the loop would otherwise wait forever while no rows are being
    /// copied — silent data loss. Probe the pool each tick.
    /// </summary>
    private async Task RunOnlineTailLoopAsync(CancellationToken cancellationToken)
    {
        _log.WriteLine("All tables drained. Change feed replaying on shared worker pool.", LogType.Info);
        while (!cancellationToken.IsCancellationRequested)
        {
            if (_pipeline!.AllWorkersExited)
            {
                int faulted = _pipeline.FaultedWorkerCount;
                _log.WriteLine(
                    $"Online worker pool has stopped (faulted={faulted}). Aborting job.",
                    LogType.Error);
                return;
            }
            if (Volatile.Read(ref _pipeline.Context.Flags.FatalErrorFlag) == 1)
            {
                _log.WriteLine("Fatal error tripped during online replay. Aborting job.", LogType.Error);
                return;
            }
            await Task.Delay(2000, cancellationToken);
        }
    }

    /// <summary>
    /// Offline finalize: complete the partition channel so workers
    /// drain and exit, await pool completion, then mark the job
    /// completed if the run reached the terminal state cleanly.
    /// </summary>
    private async Task RunOfflineFinalizeAsync(Job job)
    {
        _pipeline!.CompletePartitionChannel();
        await _pipeline.WaitForCompletionAsync();

        if (job.IsOfflineCompleted
            && job.Status != JobStatus.Cancelled
            && job.Status != JobStatus.Paused)
        {
            _log.WriteLine($"Job {job.Id} Completed", LogType.Info);
            job.Status = JobStatus.Completed;
            MigrationJobContext.Instance.SaveMigrationJob(job);
        }
    }

    private async Task ProcessWithRetryAsync(Job job, TableMigration mu, JobPartitioning partitioning, CancellationToken token)
    {
        for (int attempt = 1; attempt <= MigrationDefaults.MaxTableRetries; attempt++)
        {
            try
            {
                await ProcessMigrationUnitAsync(job, mu, partitioning, token);
                return;
            }
            catch (Exception ex) when (ExceptionClassifier.IsTransient(ex)
                && attempt < MigrationDefaults.MaxTableRetries)
            {
                int delayMs = ExceptionClassifier.GetRetryDelayMs(ex, attempt);
                _log.WriteLine($"Table retry {attempt} for {mu.KeyspaceName}.{mu.TableName}: {ex.Message}", LogType.Warning);
                await Task.Delay(delayMs, token);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                HandleMigrationUnitError(mu, ex);
                return;
            }
        }
    }

    private async Task DiscoverUnitPartitioningAsync(
        Job job,
        TableMigration mu,
        ISession sourceSession,
        Partitioner partitioner,
        List<TablePartitioning> chunks)
    {
        if (!await SchemaManager.TableExistsAsync(sourceSession, mu.KeyspaceName, mu.TableName))
        {
            _log.WriteLine($"Source table {mu.KeyspaceName}.{mu.TableName} not found.", LogType.Error);
            mu.SourceStatus = TableStatus.NotFound;
            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
            return;
        }

        var columns = await SchemaManager.GetTableColumnsAsync(sourceSession, mu.KeyspaceName, mu.TableName);
        if (columns.Count == 0)
        {
            _log.WriteLine($"No columns for {mu.KeyspaceName}.{mu.TableName}", LogType.Error);
            mu.SourceStatus = TableStatus.Failed;
            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
            return;
        }

        if (mu.CopyChunks == null || mu.CopyChunks.Count == 0)
            mu.CopyChunks = new List<CopyChunk> { new CopyChunk() };

        bool isOnline = job.IsOnline;
        var spec = new TableCopySpec(
            mu.KeyspaceName, mu.TableName,
            mu.GetEffectiveTargetKeyspaceName(), mu.GetEffectiveTargetTableName());

        for (int chunkIndex = 0; chunkIndex < mu.CopyChunks.Count; chunkIndex++)
        {
            var chunk = mu.CopyChunks[chunkIndex];
            if (chunk.IsDownloaded == true && !isOnline)
                continue;

            long rowCount = await CassandraQueries.GetRowCountAsync(sourceSession, mu.KeyspaceName, mu.TableName);
            if (rowCount > 0)
            {
                mu.EstimatedRowCount = rowCount;
                TableMigrationMapper.UpdateParentJob(mu);
            }
            chunk.SourceQueryRowCount = rowCount;

            double initialPercent = (100.0 / mu.CopyChunks.Count) * chunkIndex;
            double contributionFactor = 1.0 / mu.CopyChunks.Count;

            var tracker = new CopyProgressTracker(_log,
                mu.CopyRowsCopied, mu,
                new ProgressConfig(chunkIndex, initialPercent, contributionFactor, rowCount));

            // Partitioner owns feed-range discovery and reconciliation
            // (stored snapshots vs. source's current ranges) and hands
            // back blueprints for still-pending ranges. TableResources
            // and Partition materialization stay here so the partitioner
            // does not need to know about per-table resource wiring.
            var result = await partitioner.PartitionAsync(sourceSession, mu, enableReplay: isOnline);

            var resources = new TableResources(spec, columns, tracker, result.TotalFeedRanges);
            // Restore the bulk-completed counter on resume so PageWriter's
            // ETA reads a correct "remaining ranges" count immediately, and
            // the table-wide BulkDrainSignal trips automatically if every
            // range was already finished in a prior run.
            for (int i = 0; i < result.AlreadyCompletedCount; i++)
                resources.OnPartitionBulkCompleted();

            var partitions = new List<Partition>(result.PendingPartitions.Count);
            foreach (var bp in result.PendingPartitions)
                partitions.Add(new Partition(bp.Snapshot, bp.InitialPagingState, resources, bp.Phase));

            chunks.Add(new TablePartitioning(mu, chunkIndex, resources, partitions, result.AllRangesAlreadyComplete));
        }

        MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
    }

    private async Task ProcessMigrationUnitAsync(Job job, TableMigration mu, JobPartitioning partitioning, CancellationToken cancellationToken)
    {
        if (mu.SourceStatus == TableStatus.Failed)
        {
            mu.SourceStatus = TableStatus.OK;
            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
        }

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
        await RunCopyForUnitAsync(job, mu, partitioning, cancellationToken);
        // Note: TableCopyCoordinator already flushes the unit when the
        // copy completes successfully; cancel/fault paths propagate
        // before we'd save here. No post-save needed.
    }

    /// <summary>
    /// Owns destination schema provisioning for a single table:
    /// optional drop, then keyspace + UDTs + table creation via
    /// <see cref="SchemaManager.SyncSchemaAsync"/>. Runs exactly once
    /// per table, before <see cref="TableCopyCoordinator"/> is invoked.
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

        // Open BOTH sessions inside the try, source first.
        //
        // Order rationale:
        //   - Source is the read end and the only part needed for the
        //     SchemaManager metadata queries (system_schema.*). Opening
        //     it first lets a source-side failure short-circuit before
        //     we spend an async round-trip building the target Cluster.
        //   - Target is async (CreateTargetSessionAsync awaits cluster
        //     init); source is sync. If source were second and target
        //     first (the previous arrangement) any source failure would
        //     leak the already-built target Cluster — its socket pool,
        //     metadata listener, and IO threads — and Phase 1 fans out
        //     under Task.WhenAll so the leak scales with table count.
        //
        // Declaring both nullable outside the try lets the finally
        // dispose whichever ones got assigned, regardless of which
        // construction throws.
        Cassandra.ISession? sourceSession = null;
        Cassandra.ISession? targetSession = null;
        try
        {
            // Keyspace-agnostic source session: SchemaManager queries hit system_schema with
            // parameterized keyspace_name and all data CQL is fully qualified, so we avoid the
            // extra USE keyspace round trip and per-keyspace metadata refresh.
            sourceSession = CassandraClientFactory.CreateSourceSession(_log, job, _tokenRefreshManager);
            targetSession = await CassandraClientFactory.CreateTargetSessionAsync(_log, job);

            if (shouldDrop
                && await SchemaManager.TableExistsAsync(targetSession, mu.KeyspaceName, mu.TableName))
            {
                _log.WriteLine($"Dropping target table {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
                await targetSession.ExecuteAsync(new SimpleStatement(
                    $"DROP TABLE \"{mu.KeyspaceName}\".\"{mu.TableName}\""));
            }

            bool existed = await SchemaManager.TableExistsAsync(targetSession, mu.KeyspaceName, mu.TableName);
            await SchemaManager.SyncSchemaAsync(sourceSession, targetSession,
                mu.KeyspaceName, mu.TableName, mu.KeyspaceName, mu.TableName, _log);
            if (!existed)
                _log.WriteLine($"Created target table {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
        }
        finally
        {
            MigrationUtilities.SafeDisposeSession(sourceSession, "ProvisionTargetSchemaAsync source session");
            MigrationUtilities.SafeDisposeSession(targetSession, "ProvisionTargetSchemaAsync target session");
        }
    }

    private async Task RunCopyForUnitAsync(Job job, TableMigration mu, JobPartitioning partitioning, CancellationToken ct)
    {
        var chunks = partitioning.ForUnit(mu.Id);
        // Use the pipeline's token (not the raw external token) so a
        // worker's TripFatal — which cancels JobPipeline._cts via the
        // wired shutdown callback — also wakes this coordinator's
        // BulkDrainSignal.Task.WaitAsync. Linking the coordinator's
        // CTS only to the external token (a sibling, not a child of
        // the pipeline CTS) left fatal-driven shutdowns hanging the
        // coordinator until the operator manually stopped the job.
        using var coordinator = new TableCopyCoordinator(_log, job, _pipeline!, chunks, _pipeline!.Token);
        ct.ThrowIfCancellationRequested();

        TaskResult result = await coordinator.MigrateTableAsync(mu);
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

        if (ExceptionClassifier.IsAuth(ex))
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

    public void Stop()
    {
        // Pipeline CTS cascades cancellation to every running
        // coordinator and every worker, including any sibling tables
        // still being scheduled by Parallel.ForEachAsync (the
        // ParallelOptions.CancellationToken is the same external token
        // that the pipeline links from).
        _pipeline?.Stop();
        MigrationUtilities.SafeDispose(_pipeline, "JobPipeline (Stop)");
        _pipeline = null;
        _tokenRefreshManager.StopTokenRefreshTimer();
    }
}
