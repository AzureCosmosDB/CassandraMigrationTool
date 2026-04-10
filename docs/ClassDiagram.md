# Cassandra Migration Tool — Class Diagram

## Ownership & Creation Chain

```
Program.cs (DI setup)
 ├─► JobManager(IConfiguration, MigrationContextService)
 │    ├─► MigrationLog()                         [creates per job]
 │    ├─► MigrationWorker(MigrationLog)          [creates per job]
 │    │    └─► BulkCopyEngine(log, sourceSession, config, job, worker)
 │    │         │   inherits MigrationProcessor
 │    │         │
 │    │         ├─► BulkCopyRunner(log, job, config, cts, ensureTargetSession)
 │    │         │    ├─► CopyProgressTracker(log, keyspace, table, workerCount, ...)
 │    │         │    ├─► WorkerPool(log, workerCount, cts)
 │    │         │    │    └─► BulkCopyWorker(log, cts, workerId, pageSize)  [N workers]
 │    │         │    │         ├─► PageReader(log, connOpts, keyspace, cols, pageSize, id, cts)
 │    │         │    │         └─► PageWriter(log, connOpts, cols, keyspace, table, pageSize, id, cts)
 │    │         │    └─► Partition(feedRange, pagingState)  [per feed range]
 │    │         │         └─► WorkChunk { ContinuationToken, IsCompleted, Next }
 │    │         │
 │    │         └─► ReplayProcessor(log, sourceSession, targetSession, muCache, config, job)
 │    │              └─► ReplayWorker(log, sourceSession, targetSession, config, job, isCancelled)
 │    │
 │    └─► MigrationSettings                     [from IConfiguration]
 │
 ├─► MigrationContextService()                   [DI singleton, wraps static context]
 │    └── delegates to ──► MigrationJobContext (static)
 │                          ├─► DiskPersistence() : IPersistenceStorage
 │                          │    └─► LogPersistence(storagePath)
 │                          ├─► MigrationUnitCache()
 │                          ├── JobStore (static)
 │                          └── UnitStore (static)
 │
 ├─► AuthenticationService(sessionStorage, PasswordManager)
 │    └─► PasswordManager()
 ├─► CustomAuthenticationStateProvider(AuthenticationService)
 └─► MigrationHostedService(JobManager, ILogger)
```

## Static Utility Classes (no instances, called directly)

```
CassandraDriver/
  CassandraClientFactory  ──► creates ISession (source + target)
       calls ──► TokenRefreshManager    (AAD token refresh)
       calls ──► ArmCredentialDiscovery (ARM-based credential lookup)
  CassandraQueries        ──► ListKeyspaces, ListTables, GetRowCount, GetFeedRanges, PrepareInsert
  SchemaManager           ──► SyncSchemaAsync, EnsureKeyspace, CreateTable, AlterColumns

Infrastructure/
  ExceptionClassifier     ──► IsTransient, IsFatal, IsNotFound, IsThrottle
  MigrationUtilities      ──► SafeDispose, IsOnline, GenerateMigrationUnitId, status helpers
  MigrationDefaults       ──► Constants: WorkerMultiplier, MinWorkers, DefaultPageSize
  TableDiscovery          ──► ParseNamespaceEntries, ValidateNamespaceFormat
  DataDirectoryResolver   ──► Resolve working data directory

Persistence/
  FileSystem              ──► File/directory abstraction
```

## Records (immutable data carriers)

```
ConnectionOptions(Host, Port, Username, Password, UseSsl, MaxConnectionsPerHost)
PipelineRequest(MigrationUnit, ChunkIndex, InitialPercent, ContributionFactor, TotalRowCount, Context, FeedRanges)
WorkerConfig(SourceConnection, TargetConnection, Columns, Context)
RangeState(Completed, Checkpoints, FeedRanges)
PipelineContext(PartitionPool, Worker, Ranges, Counters, Tracker)
ReadResult(Rows, WorkChunk, IsLastPage)                          [nested in PageReader]
PartitionStageResult(Pool, Completed, Checkpoints, PendingCount) [nested in BulkCopyRunner]
```

## Models (mutable POCOs, JSON-serialized)

```
MigrationJob
 ├── Id, Name, Status (JobStatus enum)
 ├── Source/Target connection fields
 ├── Tables: List<MigrationUnitBasic>
 └── SourceConnection / TargetConnection (ConnectionOptions)

MigrationUnit : MigrationUnitBasic
 ├── Per-table state: keyspace, table, copy progress, change feed counters
 ├── MigrationChunks: List<MigrationChunk>
 ├── CompletedCopyFeedRanges: HashSet<string>
 ├── CopyFeedRangeCheckpoints: Dictionary<string, string?>
 └── ParentJob: MigrationJob [JsonIgnore]

MigrationChunk
 ├── Row counts, download/upload flags
 └── Segments: List<Segment>

TableContext
 ├── MigrationUnitId, JobId
 ├── KeyspaceName, TableName, TargetKeyspaceName, TargetTableName
 └── SourceSession: ISession

JobRegistry
 └── MigrationJobIds: List<string>
```

## Data Flow: Bulk Copy

```
JobManager.StartMigration(jobId)
  └─► MigrationWorker.ExecuteAsync(job)
       └─► [parallel per table] BulkCopyEngine.StartProcessAsync(unitId)
            └─► ProcessChunkAsync(unit, chunkIndex, context)
                 ├── CassandraQueries.GetRowCountAsync()
                 ├── CassandraQueries.GetFeedRangesAsync()
                 └─► BulkCopyRunner.RunAsync(PipelineRequest)
                      ├── Stage 1: SeedPartitionsAsync → Channel<Partition>
                      ├── Stage 2: SchemaManager.SyncSchemaAsync()
                      ├── Stage 3: WorkerPool.Start()
                      │    └─► [N workers] BulkCopyWorker.RunAsync(ctx)
                      │         loop:
                      │           take Partition from Channel
                      │           PageReader.ReadAsync() → ReadResult
                      │           recycle Partition back to Channel
                      │           PageWriter.WriteAsync(rows, workChunk)
                      │           SaveCheckpoint / MarkCompleted
                      │           CopyProgressTracker.UpdateMigrationUnit()
                      └── Stage 4: Finalize → TaskResult
```

## Data Flow: Change Feed Replay

```
BulkCopyEngine (after table completes)
  └─► MigrationProcessor.AddTableToChangeFeedQueue(unit)
       └─► ReplayProcessor.AddTableToProcess(unitId, cts)
            └─► ReplayWorker.RunAsync(mu, ct)
                 ├── parallel mode: N range tasks via SemaphoreSlim
                 └── each range: PollLoopAsync(mu, feedRange, ps, colNames, ct)
                      loop:
                        ExecuteAsync(SELECT * WHERE COSMOS_CHANGEFEED_START_TIME()...)
                        write each row to target
                        save continuation token
                        delay(intervalMs)
```

## Inheritance

```
MigrationProcessor (abstract, IDisposable)
 └── BulkCopyEngine
      fields: _sourceSession (readonly), _targetSession (lazy via EnsureTargetSession)
      fields: _log, _job, _config, _cancellation, _worker, _changeFeedProcessor
```

## Session Ownership

```
┌─────────────────────────┬──────────────────────────────────┬──────────────┐
│ Session                 │ Purpose                          │ Lifetime     │
├─────────────────────────┼──────────────────────────────────┼──────────────┤
│ MigrationProcessor      │ Metadata: row count, feed ranges │ Caller-owned │
│   ._sourceSession       │                                  │ (readonly)   │
├─────────────────────────┼──────────────────────────────────┼──────────────┤
│ EnsureTargetSession()   │ Schema sync, keyspace creation   │ Lazy, one    │
│   → _targetSession      │                                  │ per engine   │
├─────────────────────────┼──────────────────────────────────┼──────────────┤
│ PageReader              │ Per-worker data reads            │ Worker-scoped│
│   ._sourceSession       │                                  │ (Disposed)   │
├─────────────────────────┼──────────────────────────────────┼──────────────┤
│ PageWriter              │ Per-worker data writes           │ Worker-scoped│
│   ._targetSession       │                                  │ (Disposed)   │
└─────────────────────────┴──────────────────────────────────┴──────────────┘
```
