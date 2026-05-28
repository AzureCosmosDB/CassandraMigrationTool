# Job Execution Walkthrough — End to End

## 1. Job Creation (UI → Persistence)

### User fills form → `MigrationDetails.razor:HandleSubmit()`
Creates a `Job` object from form fields: name, source/target connections, namespaces, parallelism, page size, CDC mode. Fires `OnSubmit(job, sourceCS, targetCS)` callback to parent.

### Parent saves → `MigrationJobViewer.razor:OnMigrationDetailsPopUpSubmit()`
- Sets `job.Status = Running`, `job.StartedOn = DateTime.UtcNow`
- Stores passwords in-memory: `MigrationJobContext.SourceConnectionString[jobId] = sourceCS`
- Persists job: `JobStore.SaveJob(job)` → writes `migrationjobs/{jobId}/jobdefinition.json`
- Persists registry: `MigrationJobContext.SaveJobList()` → writes `JobRegistry.json`
- Fires background: `Task.Run(() => JobManager.StartMigration(job, ...))`

## 2. Job Startup (JobManager → MigrationJobRunner)

### `JobManager.StartMigration(job, sourceCS, targetCS, namespaces, jobType, online)`
```
Guards: no concurrent runs (locks _migrationLock)
Creates: MigrationLog, MigrationJobRunner(log)
Sets: _runningJobId, ActiveMigrationJobId
Stores: connection strings in context dictionaries
Background: Task.Run → MigrationJobRunner.StartAsync(job, config, ct)
```

### `MigrationJobRunner.StartAsync(job, config, ct)`
```
Gets pending tables: UnitStore.GetMigrationUnitsToMigrate(job)
Creates source session: CassandraClientFactory.CreateSourceSession(...)
Creates TokenRefreshManager (for AAD token refresh)
Parallel.ForEachAsync(units, maxParallel, (unit, ct) => ProcessWithRetryAsync(...))
  └── per table: ProcessMigrationUnitAsync(job, config, unit, ct)
        └── Creates TableMigrationEngine(log, sourceSession, config, job, tokenRefreshManager)
        └── Calls engine.StartProcessAsync(unitId)
On completion: engine.StopOfflineOrInvokeChangeFeed()
```

## 3. Bulk Copy Engine (Per-Table Orchestration)

### `TableMigrationEngine.StartProcessAsync(migrationUnitId)`
```
Loads: TableMigration from MigrationJobContext
Creates: TableCopySpec(keyspace, table, targetKeyspace, targetTable, sourceSession)
Ensures: at least one CopyChunk exists

FOR EACH chunk (typically 1):
  └── RetryHelper.ExecuteTask(() => ProcessChunkAsync(...))
      └── On Canceled / Abort → returns to caller; finally-block FinalizeStatus(result)

On all chunks complete:
  └── Sets CopyComplete = true, BulkCopyEndedOn
  └── changeFeedManager.AddTable(unit) ← starts replay for this table
  └── Saves unit to disk
```

### `TableMigrationEngine.ProcessChunkAsync(unit, chunkIndex, context, ...)`
```
1. Count rows: CassandraQueries.GetRowCountAsync(sourceSession, keyspace, table)
2. Ensure target: SchemaManager.EnsureKeyspaceExistsAsync()
3. Discover ranges: CassandraQueries.GetFeedRangesAsync(sourceSession, keyspace, table)
4. Log: "{keyspace}.{table}: {rowCount} rows, {ranges.Count} feed range(s)"
5. Pipeline stages (inline):
   SeedAsync → SyncSchemaAsync → ExecuteAsync → Finalize
```

## 4. Pipeline Execution (4-Stage Pattern, within TableMigrationEngine)

```
Stage 1: SeedAsync(request)
  ├── Filters completed feed ranges
  ├── Restores checkpoints (base64 → paging state)
  ├── Creates Channel<Partition>(pendingRanges.Count)
  ├── Seeds partitions into channel
  └── Returns allComplete (bool)

Stage 2: SyncSchemaAsync(TableCopySpec, targetSession)
  ├── SchemaManager.SyncSchemaAsync(source, target, keyspace, table)
  ├── Creates/alters target table to match source schema
  └── Returns column list

Stage 3: ExecuteAsync(request, seed, schema, targetSession)
  ├── Creates CopyProgressTracker(log, workerCount, migration, progressConfig)
  ├── Creates PipelineContext(partitionPool, workerConfig, rangeState, counters, tracker)
  ├── Creates WorkerPool(log, workerCount)
  ├── Starts N workers: BulkCopyWorker(log, ct, workerId, pageSize).RunAsync(ctx)
  ├── Awaits pool.WaitForCompletionAsync()
  └── Returns ExecutionResult(tracker, context, elapsed)

Stage 4: Finalize(execution, request)
  ├── tracker.LogFinal()
  ├── tracker.UpdateMigrationUnit()
  ├── LogPipelineSummary()
  └── DetermineOutcome(counters) → Success/Retry/Abort/Canceled
```

## 5. Worker Loop (Per-Partition Read/Write)

### `BulkCopyWorker.RunAsync(PipelineContext ctx)`
```
Creates: PageReader(log, workerConfig, pageSize, workerId, ct)
Creates: PageWriter(log, workerConfig, pageSize, workerId, ct)

LOOP while !cancelled && !fatalError:
  ├── partition = TakeNextPartitionAsync(ctx.PartitionPool)
  ├── if null → break (channel completed)
  │
  ├── result = reader.ReadAsync(partition, ctx)
  │     ├── SELECT * WHERE COSMOS_CHANGEFEED_FROM_START()=true AND COSMOS_FEEDRANGE()='...'
  │     ├── SetPageSize, SetAutoPage(false), SetPagingState
  │     ├── Retries transient errors (3 attempts)
  │     ├── Maps rows to object[] arrays
  │     ├── Updates partition: SetPageState(nextPaging, isLastPage)
  │     ├── Creates WorkChunk via partition.AddChunkAndTrim(nextPaging)
  │     └── Returns ReadResult(rows, workChunk, isLastPage)
  │
  ├── if !isLastPage → recycle partition back to channel
  │
  ├── writer.WriteAsync(rows, workChunk, ctx)
  │     ├── For each row: bind PreparedStatement, execute async
  │     ├── Parallel writes with WriteRowAsync()
  │     ├── Tracks done/failed/latency via WriteCounters
  │     ├── Only marks workChunk.IsCompleted if ALL writes succeed
  │     └── Updates tracker: AddCopied, AddFailed, AddWriteTime, AddBytes
  │
  ├── SaveCheckpoint(partition, ctx)
  │     └── ctx.Ranges.Checkpoints[feedRange] = base64(resumeToken)
  │
  ├── if partition.IsExhausted → MarkCompleted(partition, ctx)
  │     └── ctx.Ranges.Completed.Add(feedRange)
  │     └── Close channel if all ranges done
  │
  └── FINALLY: ctx.Tracker.UpdateMigrationUnit()
        ├── Writes totals to TableMigration (CopyRowsCopied, CopyPercent)
        ├── Updates parent job summary
        └── Checkpoint saves every 10 seconds
```

## 6. Change Feed Replay (After Bulk Copy)

### `ChangeFeedManager.AddTable(unit, ct)` — called per table after copy completes
```
EnsureReplayProcessor():
  ├── Creates fresh source session
  └── new ReplayProcessor(log, source, target, cache, config, job)
Enqueues: replayProcessor.AddTableToProcess(unitId, ct)
```

### `ReplayProcessor.AddTableToProcess(unitId, ct)`
```
Queues unitId → StartPendingTables(ct)
  └── Task.Run → ReplayWorker(log, source, target, config, isCancelled).RunAsync(mu, ct)
```

### `ReplayWorker.RunAsync(mu, ct)`
```
Discovers feed ranges
If multiple → RunParallelAsync (SemaphoreSlim throttled)
If single   → RunSingleAsync

PollLoopAsync(mu, feedRange, ps, colNames, ct):
  LOOP while !cancelled:
    ├── Execute SELECT * WHERE COSMOS_CHANGEFEED_START_TIME()='...'
    ├── ReplayRows: bind + insert each row to target
    ├── UpdateStats: Interlocked counters on TableMigration
    ├── SaveContinuation: persist paging state
    ├── Delay(pollIntervalMs)
    └── On transient error → TryReconnectSourceAsync + exponential backoff
```

## Session Ownership Summary

```
MigrationJobRunner
  └── creates _sourceSession (metadata: row count, feed ranges)

TableMigrationEngine
  └── creates _target session in constructor (schema sync, keyspace creation)
  └── passes target session to pipeline stages and ChangeFeedManager

BulkCopyWorker (N instances)
  └── PageReader creates own source session (per-worker data reads)
  └── PageWriter creates own target session (per-worker data writes)
  └── Both disposed when worker exits

ReplayWorker
  └── uses shared source + target sessions from ReplayProcessor
```
