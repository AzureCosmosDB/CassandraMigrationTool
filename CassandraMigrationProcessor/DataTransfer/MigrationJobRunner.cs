using Cassandra;
using CassandraMigrationProcessor.Context;
using CassandraMigrationProcessor.Infrastructure;
using CassandraMigrationProcessor.CassandraDriver;
using CassandraMigrationProcessor.Models;
using System.Runtime.ExceptionServices;

namespace CassandraMigrationProcessor.DataTransfer;

/// <summary>
/// Orchestrates a Cassandra-to-Cassandra migration with a single
/// job-wide worker pool of <see cref="PipelineConfig.WorkerCount"/>
/// shared across all tables. Schema provisioning and copy orchestration
/// fan out across every table concurrently.
/// </summary>
public class MigrationJobRunner : IAsyncDisposable
{
    private readonly MigrationLog _log;
    private readonly Job _job;
    private readonly PipelineConfig _pipelineConfig;
    private readonly JobControl _control;
    private readonly TokenRefreshManager _tokenRefreshManager;
    private readonly RotatingSessionProvider _sourceSessions;
    private int _consecutiveAuthErrors;
    // Last auth exception observed by HandleMigrationUnitError;
    // attached as inner when the consecutive-auth threshold trips so
    // operators see the actual driver failure (host, message, stack)
    // not just the sentinel reason. Read/written under the
    // Interlocked.Exchange of _consecutiveAuthErrors so writers race
    // safely with the threshold-check reader in RunCopyPhaseAsync.
    private Exception? _lastAuthException;
    private JobPipeline? _pipeline;

    /// <summary>
    /// Runner-wide source / target sessions opened once in
    /// <see cref="CreateAsync"/> and reused across wildcard expansion,
    /// schema provisioning, and partition discovery. Disposed in
    /// <see cref="DisposeAsync"/>. Copy workers reuse the thread-safe source
    /// session to avoid multiplying driver metadata topology/schema handshakes,
    /// while retaining independent target sessions for write throughput.
    /// For simulated runs the target session is a <see cref="NullSession"/>.
    /// </summary>
    private readonly ISession _sourceSession;
    private readonly ISession _targetSession;

    /// <summary>
    /// Per-run in-memory cache of <see cref="TableMigration"/> documents
    /// keyed by <c>jobId::unitId</c>. Lives for the duration of the run.
    /// </summary>
    public TableMigrationCache MigrationUnitsCache { get; }
        = new TableMigrationCache();

    private MigrationJobRunner(
        MigrationLog log,
        Job job,
        PipelineConfig pipelineConfig,
        JobControl control,
        TokenRefreshManager tokenRefreshManager,
        RotatingSessionProvider sourceSessions,
        ISession sourceSession,
        ISession targetSession)
    {
        _log = log;
        _job = job;
        _pipelineConfig = pipelineConfig;
        _control = control;
        _tokenRefreshManager = tokenRefreshManager;
        _sourceSessions = sourceSessions;
        _sourceSession = sourceSession;
        _targetSession = targetSession;
    }

    /// <summary>
    /// Asynchronous factory: resolves the pipeline configuration, opens
    /// source and target sessions, and only then materialises the runner.
    /// On any failure during session acquisition all partially-opened
    /// resources are torn down before the exception propagates.
    /// </summary>
    public static async Task<MigrationJobRunner> CreateAsync(
        MigrationLog log, Job job, AppSettings config, JobControl control)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(control);

        var pipelineConfig = PipelineConfig.Resolve(job, config);
        var sourceSessions = new RotatingSessionProvider(
            credential => CassandraClientFactory.CreateSourceSessionWithCredential(
                log, job, credential));
        var tokenRefreshManager = new TokenRefreshManager(log, sourceSessions);
        ISession? source = null;
        ISession? target = null;
        try
        {
            string sourceCredential = CassandraClientFactory.ResolveSourceCredential(
                job, tokenRefreshManager);
            source = sourceSessions.Initialize(sourceCredential);
            if (TokenRefreshManager.IsLikelyAadToken(sourceCredential))
                tokenRefreshManager.StartTokenRefreshTimer(sourceCredential);
            target = await CassandraClientFactory.CreateTargetSessionAsync(log, job);
            return new MigrationJobRunner(
                log, job, pipelineConfig, control, tokenRefreshManager,
                sourceSessions, source, target);
        }
        catch
        {
            MigrationUtilities.SafeDisposeSession(target, "MigrationJobRunner target (CreateAsync rollback)");
            tokenRefreshManager.Dispose();
            sourceSessions.Dispose();
            throw;
        }
    }

    public async Task StartAsync()
    {
        var job = _job;
        var cancellationToken = _control.Token;
        Exception? failure = null;

        try
        {
            if (job.Tables.Count == 0
                || job.Tables.Any(m => m.TableName == "*"))
            {
                await ExpandWildcardTablesAsync(job, cancellationToken);
            }

            var units = UnitStore.GetMigrationUnitsToMigrate(job);
            if (units.Count == 0)
            {
                // Distinguish two distinct zero-unit outcomes from
                // UnitStore.GetMigrationUnitsToMigrate:
                //   (a) operator-supplied input didn't resolve to any
                //       table on the source — a typo. Fault loudly so
                //       the homepage badge tells the truth.
                //   (b) every job.Tables entry was filtered as terminal
                //       (CopyComplete on an offline job, or
                //       SkippedDueToMaxRetries). That's the
                //       resume-after-completion or re-run path
                //       JobManager.StartMigration explicitly permits;
                //       faulting here would flip Completed/Pending
                //       jobs to Faulted on re-run.
                bool operatorRequestedTables =
                    !string.IsNullOrWhiteSpace(job.Namespaces)
                    || (job.Tables != null && job.Tables.Count > 0);
                bool anyUnitWasFilteredAsTerminal =
                    job.Tables != null
                    && job.Tables.Any(t => t.IsValid
                        && ((t.CopyComplete && !job.IsOnline)
                            || t.SkippedDueToMaxRetries));
                if (operatorRequestedTables && !anyUnitWasFilteredAsTerminal)
                {
                    string requested = !string.IsNullOrWhiteSpace(job.Namespaces)
                        ? job.Namespaces!
                        : string.Join(", ", (job.Tables ?? new List<TableMigrationSummary>())
                            .Select(t => $"{t.KeyspaceName}.{t.TableName}"));
                    string msg =
                        $"Tables To Migrate '{requested}' did not match any source tables. " +
                        "Check that the keyspace and table names exist on the source cluster.";
                    _log.WriteLine(msg, LogType.Error);
                    job.Status = JobStatus.Faulted;
                    StampEndedOnIfTerminal(job);
                    MigrationJobContext.Instance.SaveMigrationJob(job);
                }
                else
                {
                    _log.WriteLine("No remaining migration units.", LogType.Warning);
                }
                return;
            }

            if (job.IsSimulatedRun)
            {
                _log.WriteLine(
                    $"[Simulation] Target writes will be skipped for {units.Count} table(s). " +
                    "Row counts shown reflect rows that WOULD have been written.",
                    LogType.Info);
            }

            _log.WriteLine(
                $"Migrating {units.Count} tables with {_pipelineConfig.WorkerCount} shared workers");

            await RunSchemaPhaseAsync(job, units, cancellationToken);

            var partitioning = await RunPartitioningPhaseAsync(
                job, units, cancellationToken);

            _pipeline = new JobPipeline(
                _log, job, _pipelineConfig, partitioning,
                _sourceSessions,
                new GatedSessionFactory(
                    new JobSessionFactory(_log, job)),
                _control);
            _pipeline.Start();

            await RunCopyPhaseAsync(job, units, partitioning, cancellationToken);

            // All tables have either drained or completed. Online jobs
            // keep the shared pool alive for change-feed tailing; offline
            // jobs close the channel so workers exit.
            //
            // Auth-error handling: HandleMigrationUnitError already
            // marked each affected table as Failed, so WriteFinalJobStatus
            // (called from the finally block) will mark the whole job
            // Faulted via its tableFailed check. The per-iteration
            // _consecutiveAuthErrors guard in RunCopyPhaseAsync stops
            // new work from spinning up against broken credentials;
            // no additional throw is needed here.
            if (job.IsOnline)
                await RunOnlineTailLoopAsync(cancellationToken);
            else
                await RunOfflineFinalizeAsync(job);
        }
        catch (OperationCanceledException)
        {
            // Differentiate the operator-facing message based on the
            // intent captured in JobControl: pause keeps the job
            // resumable; stop is terminal; bare token cancel (e.g.
            // process shutdown) falls back to "cancelled".
            string message = _control.Requested switch
            {
                JobCommand.PauseRequested   => "Migration was paused (resumable).",
                JobCommand.StopRequested    => "Migration was stopped.",
                JobCommand.CutoverRequested => "Migration was cutover (completed).",
                _                           => "Migration was cancelled.",
            };
            _log.WriteLine(message, LogType.Info);
        }
        catch (Exception ex)
        {
            failure = ex;
            // Make the fault observable through JobControl.FirstFault so
            // any cooperating coordinator (PartitionManager, change-feed
            // tail) that polls IsFatal also stops promptly — without
            // this, the catch swallowed the signal and only the final
            // status update surfaced it.
            _control.ReportFault(ex);
            _log.WriteLine($"Migration failed: {ex}", LogType.Error);
        }
        finally
        {
            // Sessions and the token-refresh timer are owned by the
            // runner itself and torn down in DisposeAsync; here we
            // only release per-run resources scoped to this call.
            MigrationUtilities.SafeDispose(_pipeline, "JobPipeline");
            _pipeline = null;

            WriteFinalJobStatus(job, _control, failure);
        }
    }

    /// <summary>
    /// Releases the source / target sessions and stops the AAD
    /// token-refresh timer. Idempotent and safe to call even if
    /// <see cref="StartAsync"/> was never invoked (e.g. the run was
    /// cancelled between <see cref="CreateAsync"/> and the start
    /// trigger).
    /// </summary>
    public ValueTask DisposeAsync()
    {
        _tokenRefreshManager.StopTokenRefreshTimer();
        MigrationUtilities.SafeDispose(_pipeline, "JobPipeline (Dispose)");
        _pipeline = null;
        MigrationUtilities.SafeDisposeSession(_targetSession, "MigrationJobRunner target session");
        _tokenRefreshManager.Dispose();
        _sourceSessions.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Single writer of <see cref="Job.Status"/> for a run that
    /// reached <see cref="StartAsync"/>. Faults outrank user intent
    /// (a Faulted run is visible even if the operator clicked Pause
    /// after the failure had already happened); user intent then
    /// distinguishes Paused from Cancelled; otherwise the run is
    /// either left at its already-terminal status (e.g. Completed
    /// written by <see cref="RunOfflineFinalizeAsync"/>) or
    /// normalised from Running to Pending.
    ///
    /// Fault sources, in priority order: (1) the exception that
    /// escaped <see cref="StartAsync"/>'s try/catch, (2) any per-table
    /// <see cref="TableStatus.Failed"/> stamp set by the schema /
    /// online-replay paths, (3) the first fault captured on
    /// <see cref="JobControl.FirstFault"/> — worker self-aborts route
    /// here without throwing out of <see cref="StartAsync"/> or
    /// marking the unit Failed, so without consulting FirstFault a
    /// fatal worker fault would collapse to Pending.
    /// </summary>
    private void WriteFinalJobStatus(Job job, JobControl control, Exception? failure)
    {
        failure ??= control.FirstFault;

        bool tableFailed = job.Tables?.Any(
            t => t.SourceStatus == TableStatus.Failed) ?? false;

        if (failure != null || tableFailed || control.IsFatal)
        {
            if (failure != null)
                _log.WriteLine($"Job finalised as Faulted: {failure.GetType().Name}: {failure.Message}", LogType.Error);
            job.Status = JobStatus.Faulted;
        }
        else if (control.Requested == JobCommand.CutoverRequested)
        {
            // Operator chose to finalise an online job — terminal,
            // not resumable. Distinct from Stop so the log and UI
            // can show "Completed (cutover)" rather than "Cancelled".
            job.Status = JobStatus.Completed;
        }
        else if (control.Requested == JobCommand.StopRequested)
        {
            job.Status = JobStatus.Cancelled;
        }
        else if (control.Requested == JobCommand.PauseRequested)
        {
            job.Status = JobStatus.Paused;
        }
        else if (job.Status == JobStatus.Running)
        {
            job.Status = JobStatus.Pending;
        }

        StampEndedOnIfTerminal(job);

        MigrationJobContext.Instance.SaveMigrationJob(job);
    }

    /// <summary>
    /// Stamp <see cref="Job.EndedOn"/> the first time a job reaches a
    /// terminal state. Re-resume cycles intentionally do not overwrite
    /// an existing EndedOn so the homepage Duration reflects "ran for X
    /// before terminating" rather than "X since the last terminal write".
    /// </summary>
    private static void StampEndedOnIfTerminal(Job job)
    {
        if (job.EndedOn.HasValue) return;
        if (job.Status.IsTerminal())
        {
            job.EndedOn = DateTime.UtcNow;
        }
    }

    /// <summary>
    /// schema for every table in parallel. Schema provisioning is
    /// fail-fast: the first per-table failure marks that unit Failed,
    /// cancels every sibling provisioning task, and rethrows so the
    /// whole job aborts before any partitioning or copy work starts.
    /// Continuing past a schema failure would leave the target in a
    /// partially provisioned state and waste source/target I/O on the
    /// remaining tables; the operator should fix the offending table
    /// (e.g. unsupported UDT, missing keyspace privilege) and resume.
    /// </summary>
    private async Task RunSchemaPhaseAsync(
        Job job, IReadOnlyList<TableMigration> units, CancellationToken ct)
    {
        if (job.IsSimulatedRun)
        {
            _log.WriteLine("Simulated run: skipping target schema provisioning.", LogType.Info);
            return;
        }

        _log.WriteLine($"=== Phase 1: Schema — {units.Count} table(s) ===", LogType.Info);

        // One-shot discovery: warn the operator about source-side
        // schema objects this tool does not migrate (secondary
        // indexes, materialized views, UDFs/UDAs, triggers). Runs
        // once per job, before per-table provisioning. Failures here
        // are best-effort and must not block schema phase.
        try
        {
            var inScopeKeyspaces = units
                .Select(u => u.KeyspaceName)
                .Where(k => !string.IsNullOrEmpty(k))
                .Distinct(StringComparer.Ordinal)
                .ToList();
            await SchemaManager.WarnAboutUnreplicatedSchemaAsync(
                _sourceSession, inScopeKeyspaces, _log);
        }
        catch (Exception ex)
        {
            _log.WriteLine(
                $"[Schema] Unreplicated-schema discovery scan failed " +
                $"({ex.GetType().Name}: {ex.Message}); continuing.",
                LogType.Warning);
        }

        // Schema provisioning is per-table independent (each table
        // opens its own source + target sessions for one CREATE
        // KEYSPACE / CREATE TYPE / CREATE TABLE round-trip). Running
        // it serially blocked the entire copy phase for ~25s per
        // table on Cosmos source clusters with high DDL latency
        // (e.g. 11 tables took ~4.5 minutes before any data flowed).
        //
        // The linked CTS is the fail-fast trigger: as soon as one
        // unit's catch records the failure, we cancel it so siblings
        // that haven't started yet observe IsCancellationRequested
        // and return silently. ProvisionTargetSchemaAsync isn't
        // token-aware, so in-flight DDL still runs to completion —
        // but the captured "first failure" rethrows after WhenAll.
        using var phaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var failureLock = new object();
        Exception? firstFailure = null;
        TableMigration? firstFailureUnit = null;

        await Task.WhenAll(units.Select(async mu =>
        {
            try
            {
                // External cancellation (Stop / job control) propagates as
                // OCE so StartAsync's "Migration was cancelled" path runs.
                // The fail-fast trip and auth threshold both silently
                // skip remaining units — the captured failure that tripped
                // phaseCts is what gets rethrown after Task.WhenAll.
                ct.ThrowIfCancellationRequested();
                if (phaseCts.IsCancellationRequested)
                    return;
                if (Volatile.Read(ref _consecutiveAuthErrors) >= MigrationDefaults.MaxConsecutiveAuthErrors)
                    return;

                if (job.SkipSchemaSync)
                {
                    // Emit a single accurate line per table. The
                    // previous code logged "[Schema] Provisioning target
                    // for ..." here unconditionally, then
                    // ProvisionTargetSchemaAsync logged "Skipping schema
                    // sync ..." right after — two contradictory lines per
                    // table at the same timestamp. The skip log lives
                    // inside ProvisionTargetSchemaAsync so resume / replay
                    // paths reach it too; suppressing the outer line keeps
                    // the operator log consistent with what actually ran.
                }
                else
                {
                    _log.WriteLine($"[Schema] Provisioning target for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
                }
                await ProvisionTargetSchemaAsync(job, mu);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                lock (failureLock)
                {
                    if (firstFailure == null)
                    {
                        firstFailure = ex;
                        firstFailureUnit = mu;
                    }
                    HandleMigrationUnitError(mu, ex);
                }
                phaseCts.Cancel();
            }
        }));

        if (firstFailure != null)
        {
            _log.WriteLine(
                $"[Schema] Aborting job: provisioning failed for {firstFailureUnit!.KeyspaceName}.{firstFailureUnit.TableName}",
                LogType.Error);
            ExceptionDispatchInfo.Capture(firstFailure).Throw();
        }
    }

    /// <summary>
    /// Build all source-side state needed for the worker pool — columns,
    /// feed-range list, per-chunk row count — and call
    /// <see cref="Partitioner.PartitionAsync"/> to produce a
    /// <see cref="TablePartitioning"/> for each chunk. All work uses the
    /// runner's source session so the <see cref="JobPipeline"/> can be
    /// constructed with the full partition list and no runtime "seeding"
    /// path is required.
    ///
    /// Discovery runs at bounded parallelism across every still-eligible
    /// migration unit because each unit's source-side probe is independent
    /// but <c>COUNT(*)</c> is heavy on big tables — we avoid 10-table fan-out
    /// causing source-coordinator overload. On resume, each unit's
    /// <c>COUNT(*)</c> is short-circuited by the persisted
    /// <c>EstimatedRowCount</c> (see <c>DiscoverUnitPartitioningAsync</c>),
    /// so a resumed job pays no rescan cost regardless of table count.
    /// </summary>
    private async Task<JobPartitioning> RunPartitioningPhaseAsync(
        Job job,
        IReadOnlyList<TableMigration> units,
        CancellationToken ct)
    {
        var chunks = new List<TablePartitioning>();
        var failedUnitIds = new HashSet<string>(StringComparer.Ordinal);
        var collectLock = new object();

        _log.WriteLine($"=== Phase 2: Partitioning ===", LogType.Info);
        var partitioner = new Partitioner(_log);

        int parallelism = Math.Min(MigrationDefaults.PartitionDiscoveryParallelism, units.Count);
        if (parallelism < 1) parallelism = 1;

        var options = new ParallelOptions
        {
            CancellationToken = ct,
            MaxDegreeOfParallelism = parallelism,
        };

        await Parallel.ForEachAsync(units, options, async (mu, token) =>
        {
            token.ThrowIfCancellationRequested();

            _log.WriteLine($"[Partitioning] Discovering partitions for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
            try
            {
                await DiscoverUnitPartitioningAsync(job, mu, _sourceSession, partitioner, chunks, collectLock);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                lock (collectLock)
                {
                    HandleMigrationUnitError(mu, ex);
                    failedUnitIds.Add(mu.Id);
                }
            }
        });

        return new JobPartitioning(chunks, failedUnitIds);
    }

    /// <summary>
    /// share the worker pool owned by JobPipeline; this loop just
    /// kicks off each table's drain so the pool stays saturated.
    /// </summary>
    private Task RunCopyPhaseAsync(
        Job job,
        IReadOnlyList<TableMigration> units,
        JobPartitioning partitioning,
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
            if (partitioning.FailedUnitIds.Contains(mu.Id))
                return;
            if (Volatile.Read(ref _consecutiveAuthErrors) >= MigrationDefaults.MaxConsecutiveAuthErrors)
            {
                var lastAuth = Interlocked.CompareExchange(ref _lastAuthException, null, null);
                var msg = $"Aborting: {MigrationDefaults.MaxConsecutiveAuthErrors} consecutive auth errors";
                _control.ReportFault(lastAuth != null
                    ? new MigrationFatalException(msg, lastAuth)
                    : new MigrationFatalException(msg));
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
                HandleMigrationUnitError(mu, ex);
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
    /// copied — silent data loss. Probe the pool each tick and throw
    /// so the run is recorded as Faulted.
    /// </summary>
    private async Task RunOnlineTailLoopAsync(CancellationToken cancellationToken)
    {
        _log.WriteLine("All tables drained. Change feed replaying on shared worker pool.", LogType.Info);
        while (!cancellationToken.IsCancellationRequested)
        {
            if (_pipeline!.AllWorkersExited)
            {
                int faulted = _pipeline.FaultedWorkerCount;
                throw new InvalidOperationException(
                    $"Online worker pool has stopped (faulted={faulted}).");
            }
            if (_control.IsFatal)
            {
                throw new InvalidOperationException(
                    "Fatal error tripped during online replay.");
            }
            await Task.Delay(2000, cancellationToken);
        }
    }

    /// <summary>
    /// Offline finalize: complete the partition channel so workers
    /// drain and exit, await pool completion, then mark the job
    /// completed if every unit reached the terminal state.
    /// </summary>
    private async Task RunOfflineFinalizeAsync(Job job)
    {
        _pipeline!.CompletePartitionChannel();
        await _pipeline.WaitForCompletionAsync();

        if (job.IsOfflineCompleted)
        {
            _log.WriteLine($"Job {job.Id} Completed", LogType.Info);
            job.Status = JobStatus.Completed;
            StampEndedOnIfTerminal(job);
            MigrationJobContext.Instance.SaveMigrationJob(job);
        }
    }

    private Task ProcessWithRetryAsync(Job job, TableMigration mu, JobPartitioning partitioning, CancellationToken token)
    {
        return RetryExecutor.ExecuteAsync<int>(
            operation: async _ =>
            {
                await ProcessMigrationUnitAsync(job, mu, partitioning, token);
                return 0;
            },
            maxAttempts: MigrationDefaults.MaxTableRetries,
            shouldRetry: ExceptionClassifier.IsTransient,
            delayFor: (ex, attempt) => RetryPolicy.FromException(ex, attempt),
            onRetry: (ex, attempt) => _log.WriteLine(
                $"Table retry {attempt} for {mu.KeyspaceName}.{mu.TableName}: {ex.Message}",
                LogType.Warning),
            cancellationToken: token);
    }

    private async Task DiscoverUnitPartitioningAsync(
        Job job,
        TableMigration mu,
        ISession sourceSession,
        Partitioner partitioner,
        List<TablePartitioning> chunks,
        object chunksLock)
    {
        if (!await SchemaManager.TableExistsAsync(sourceSession, mu.KeyspaceName, mu.TableName))
        {
            MarkUnitFailedAndSave(mu, $"Source table {mu.KeyspaceName}.{mu.TableName} not found.", TableStatus.NotFound);
            return;
        }

        var columns = await SchemaManager.GetTableColumnsAsync(sourceSession, mu.KeyspaceName, mu.TableName);
        if (columns.Count == 0)
        {
            MarkUnitFailedAndSave(mu, $"No columns for {mu.KeyspaceName}.{mu.TableName}", TableStatus.Failed);
            return;
        }

        bool isOnline = job.IsOnline;
        if (mu.BulkDownloaded == true && !isOnline)
            return;

        var spec = new TableCopySpec(
            mu.KeyspaceName, mu.TableName,
            mu.GetEffectiveTargetKeyspaceName(), mu.GetEffectiveTargetTableName());

        long rowCount;
        if (mu.EstimatedRowCount > 0)
        {
            rowCount = mu.EstimatedRowCount;
            _log.WriteLine(
                $"{mu.KeyspaceName}.{mu.TableName}: reusing persisted EstimatedRowCount={rowCount:N0} (skip live COUNT(*) on resume)",
                LogType.Info);
        }
        else if (job.SkipSourceRowCount)
        {
            rowCount = 0;
            _log.WriteLine(
                $"{mu.KeyspaceName}.{mu.TableName}: skipping live COUNT(*) (Skip source row count is enabled on the job). Progress percent will display as '?' for this table.",
                LogType.Info);
        }
        else
        {
            rowCount = await CassandraQueries.GetRowCountAsync(sourceSession, mu.KeyspaceName, mu.TableName);
            if (rowCount > 0)
            {
                mu.EstimatedRowCount = rowCount;
                TableMigrationMapper.UpdateParentJob(mu);
            }
        }
        mu.SourceCountDuringCopy = rowCount;

        var tracker = new CopyProgressTracker(_log, mu.CopyRowsCopied, mu, rowCount);

        // Partitioner reconciles feed ranges (source vs. stored) and
        // hands back descriptors for still-pending ranges.
        // TableResources and Partition materialization stay here so the
        // partitioner does not need to know about per-table resource wiring.
        var plan = await partitioner.PartitionAsync(sourceSession, mu, enableReplay: isOnline);

        var resources = new TableResources(spec, columns, tracker, plan.TotalFeedRanges);
        // Restore the bulk-completed counter on resume so PageWriter's
        // ETA reads a correct "remaining ranges" count immediately, and
        // the table-wide BulkDrainSignal trips automatically if every
        // range was already finished in a prior run.
        for (int i = 0; i < plan.AlreadyCompletedCount; i++)
            resources.OnPartitionBulkCompleted();

        var partitions = new List<Partition>(plan.PendingPartitions.Count);
        foreach (var pending in plan.PendingPartitions)
            partitions.Add(new Partition(pending.Snapshot, pending.InitialPagingState, resources, pending.Phase));

        lock (chunksLock)
        {
            chunks.Add(new TablePartitioning(mu, resources, partitions, plan.AllRangesAlreadyComplete));
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

        if (mu.BulkCopyPhase < BulkCopyPhase.Copying)
            mu.AdvanceBulkCopyPhase(BulkCopyPhase.Copying);

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
            mu.AdvanceBulkCopyPhase(BulkCopyPhase.InitializingDestination);
            MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
        }

        if (shouldDrop
            && await SchemaManager.TableExistsAsync(_targetSession, mu.KeyspaceName, mu.TableName))
        {
            _log.WriteLine($"Dropping target table {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
            await _targetSession.ExecuteAsync(new SimpleStatement(
                $"DROP TABLE \"{mu.KeyspaceName}\".\"{mu.TableName}\""));
        }

        bool existed = await SchemaManager.TableExistsAsync(_targetSession, mu.KeyspaceName, mu.TableName);
        await SchemaManager.SyncSchemaAsync(_sourceSession, _targetSession,
            mu.KeyspaceName, mu.TableName, mu.KeyspaceName, mu.TableName, _log);
        if (!existed)
            _log.WriteLine($"Created target table {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
    }

    private async Task RunCopyForUnitAsync(Job job, TableMigration mu, JobPartitioning partitioning, CancellationToken ct)
    {
        var chunks = partitioning.ForUnit(mu.Id);
        // Every coordinator observes the same shared JobControl: a
        // worker's ReportFault cancels the single CTS so this
        // coordinator's BulkDrainSignal.Task.WaitAsync unblocks
        // immediately (no linked-CTS hop required).
        var coordinator = new TableCopyCoordinator(_log, job, _control, chunks);
        ct.ThrowIfCancellationRequested();

        TaskResult result = await coordinator.MigrateTableAsync(mu);
        if (result == TaskResult.Success)
            _log.WriteLine($"{(job.IsSimulatedRun ? "[Simulation] " : string.Empty)}Copy succeeded for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
        else if (result == TaskResult.Canceled)
            _log.WriteLine($"Copy paused for {mu.KeyspaceName}.{mu.TableName}", LogType.Info);
        else if (result == TaskResult.Abort)
        {
            // No coordinator-level log: an abort is always the
            // downstream consequence of a worker-reported fault, which
            // is already logged at the source via JobControl. A second
            // line here would double-report the same failure.
        }
        else
            _log.WriteLine($"Copy failed for {mu.KeyspaceName}.{mu.TableName} — will retry on resume", LogType.Error);
    }

    private void HandleMigrationUnitError(TableMigration mu, Exception ex)
    {
        _log.WriteLine($"Error processing {mu.KeyspaceName}.{mu.TableName}: {ex}", LogType.Error);
        mu.SourceStatus = TableStatus.Failed;

        // Stamped on the unit JSON so the failure reason survives a
        // process restart even when in-memory log buffer is lost.
        var phase = mu.BulkCopyPhase.ToString();
        mu.FailedOperation =
            $"[{DateTime.UtcNow:O}] phase={phase} {ex.GetType().Name}: {Truncate(ex.Message, 500)}";

        if (ExceptionClassifier.IsAuth(ex))
        {
            Interlocked.Exchange(ref _lastAuthException, ex);
            Interlocked.Increment(ref _consecutiveAuthErrors);
            _log.WriteLine($"Auth failure #{Volatile.Read(ref _consecutiveAuthErrors)} on {mu.KeyspaceName}.{mu.TableName}", LogType.Warning);
        }
        else
        {
            Interlocked.Exchange(ref _consecutiveAuthErrors, 0);
            Interlocked.Exchange(ref _lastAuthException, null);
        }

        MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
    }

    private static string Truncate(string? s, int max)
    {
        if (string.IsNullOrEmpty(s)) return string.Empty;
        return s.Length <= max ? s : s.Substring(0, max) + "…";
    }

    /// <summary>
    /// Records a per-unit early-exit failure: writes <paramref name="errorMessage"/>
    /// to the run log, stamps the unit's <see cref="TableMigration.SourceStatus"/>,
    /// and saves it so the failure survives a process restart. Used by
    /// per-table preflight checks where we don't want to fall into the
    /// generic exception path.
    /// </summary>
    private void MarkUnitFailedAndSave(TableMigration mu, string errorMessage, TableStatus status)
    {
        _log.WriteLine(errorMessage, LogType.Error);
        mu.SourceStatus = status;
        MigrationJobContext.Instance.SaveMigrationUnit(mu, true);
    }

    public void Stop()
    {
        // JobControl token (already cancelled by the caller via
        // RequestPause/RequestStop/RequestCutover) cascades cancellation
        // to every running coordinator and every worker. Here we just
        // eagerly tear down the pipeline so resources are released
        // without waiting for the outer Task to observe the cancel.
        MigrationUtilities.SafeDispose(_pipeline, "JobPipeline (Stop)");
        _pipeline = null;
        _tokenRefreshManager.StopTokenRefreshTimer();
    }

    /// <summary>
    /// Resolves any "keyspace.*" wildcard entries in <c>job.Namespaces</c>
    /// by connecting to the source and listing the keyspace's tables,
    /// then probing each one to filter out those that cannot be read.
    /// Resolved tables are appended to <c>job.Tables</c> via
    /// <see cref="UnitStore.AddMigrationUnits"/>; wildcard placeholders
    /// are removed from <c>job.Tables</c>.
    /// </summary>
    private async Task ExpandWildcardTablesAsync(Job job, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(job.Namespaces)) return;

        var entries = job.Namespaces
            .Split(new[] { ',', '\n', '\r', ';' })
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrEmpty(s));

        var expandedUnits = new List<TableMigration>();

        void AddExpandedUnit(string keyspaceName, string tableName) =>
            expandedUnits.Add(new TableMigration(job, keyspaceName, tableName)
            {
                SourceStatus = TableStatus.OK,
            });

        foreach (var fullName in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string keyspace, table;
            try
            {
                (keyspace, table) = CqlIdentifier.SplitNamespaceEntry(fullName);
            }
            catch (ArgumentException ex)
            {
                _log.WriteLine(
                    $"Skipping invalid namespace entry '{fullName}': {ex.Message}",
                    LogType.Warning);
                continue;
            }
            if (string.IsNullOrEmpty(keyspace) || string.IsNullOrEmpty(table))
            {
                _log.WriteLine(
                    $"Skipping namespace entry '{fullName}' — empty keyspace or table after parsing.",
                    LogType.Warning);
                continue;
            }

            if (table != "*")
            {
                AddExpandedUnit(keyspace, table);
                continue;
            }

            try
            {
                var tables = await CassandraQueries.ListTablesAsync(_sourceSession, keyspace);
                foreach (var tableName in tables)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (await IsTableAccessibleAsync(_sourceSession, keyspace, tableName, cancellationToken))
                    {
                        AddExpandedUnit(keyspace, tableName);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _log.WriteLine($"Failed to discover tables in keyspace {keyspace}: {ex.Message}", LogType.Error);
            }
        }

        if (expandedUnits.Count > 0)
        {
            job.Tables?.RemoveAll(m => m.TableName == "*");
            UnitStore.AddMigrationUnits(expandedUnits, job, _log);
        }
    }

    private async Task<bool> IsTableAccessibleAsync(
        ISession session, string keyspace, string tableName, CancellationToken cancellationToken)
    {
        try
        {
            return await RetryExecutor.ExecuteAsync<bool>(
                operation: _ =>
                {
                    var probe = new SimpleStatement(
                        $"SELECT * FROM \"{keyspace}\".\"{tableName}\" WHERE COSMOS_CHANGEFEED_FROM_START() = true");
                    probe.SetPageSize(1);
                    probe.SetAutoPage(false);
                    probe.SetReadTimeoutMillis(15_000);
                    session.Execute(probe);
                    return Task.FromResult(true);
                },
                maxAttempts: 10,
                shouldRetry: ExceptionClassifier.IsThrottle,
                delayFor: (_, attempt) => TimeSpan.FromSeconds(Math.Min(attempt * 3, 30)),
                cancellationToken: cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception vex)
        {
            _log.WriteLine($"Skipping {keyspace}.{tableName}: {vex.Message}", LogType.Warning);
            return false;
        }
    }
}
