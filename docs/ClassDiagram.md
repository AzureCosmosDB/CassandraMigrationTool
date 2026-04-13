# Cassandra Migration Tool — Class Diagram

## Ownership & Creation Chain

```
Program.cs (DI setup)
 ├─► MigrationJobContext()                         [created + Initialize(), registered as DI singleton]
 ├─► ICassandraSessionFactory → CassandraSessionFactory [DI singleton]
 ├─► JobManager(IConfiguration, MigrationContextService)
 │    ├─► MigrationLog()                         [creates per job]
 │    ├─► MigrationWorker(MigrationLog)          [creates per job]
 │    │    └─► BulkCopyEngine(log, sourceSession, config, job, worker)
 │    │         │
 │    │         ├─► CopyProgressTracker(log, keyspace, table, workerCount, ...)
 │    │         │    └─► ProgressCounters()
 │    │         ├─► WorkerPool(log, workerCount)
 │    │         │    └─► BulkCopyWorker(log, cts, workerId, pageSize)  [N workers]
 │    │         │         ├─► PageReader(log, connOpts, keyspace, cols, pageSize, id, cts)
 │    │         │         └─► PageWriter(log, connOpts, cols, keyspace, table, pageSize, id, cts)
 │    │         ├─► Partition(feedRange, pagingState)  [per feed range]
 │    │         │    └─► WorkChunk { ContinuationToken, IsCompleted, Next }
 │    │         │
 │    │         └─► ChangeFeedManager(log, sourceSession, config, job)
 │    │              └─► ReplayProcessor(log, sourceSession, targetSession, muCache, config, job)
 │    │                   └─► ReplayWorker(log, sourceSession, targetSession, config, job, isCancelled)
 │    │
 │    └─► AppSettings                            [from IConfiguration]
 │
 ├─► MigrationContextService(MigrationJobContext)  [DI singleton, constructor-injected]
 │    └── delegates to ──► MigrationJobContext      [DI singleton, instance class]
 │                          ├─► DiskPersistence() : IDocumentStorage
 │                          │    └─► LogPersistence() : ILogStorage
 │                          ├─► TableMigrationCache()
 │                          ├── JobStore (static)
 │                          ├── UnitStore (static)
 │                          └── SettingsManager (static)
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
  CassandraSessionFactory ──► ICassandraSessionFactory (DI wrapper)
  CassandraQueries        ──► ListKeyspaces, ListTables, GetRowCount, GetFeedRanges, PrepareInsert
  SchemaManager           ──► SyncSchemaAsync, EnsureKeyspace, CreateTable, AlterColumns

Infrastructure/
  ExceptionClassifier     ──► IsTransient, IsFatal, IsNotFound, IsThrottle
  MigrationUtilities      ──► SafeDispose, SafeExecute, IsOnline, status helpers
  MigrationDefaults       ──► Constants: WorkerMultiplier, MinWorkers, DefaultPageSize
  TableDiscovery          ──► ParseNamespaceEntries, ValidateNamespaceFormat
  TableMigrationMapper    ──► TableMigration ↔ TableMigrationSummary mapping
  DataDirectoryResolver   ──► Resolve working data directory

Persistence/
  FileSystem              ──► File/directory abstraction
```

## Records (immutable data carriers)

```
ConnectionOptions(Host, Port, Username, Password, UseSsl, MaxConnectionsPerHost)
PipelineRequest(TableMigration, ChunkIndex, InitialPercent, ContributionFactor, TotalRowCount, Context, FeedRanges)
PipelineConfig(PageSize, WorkerCount, CheckpointInterval, ...)   [resolved from Job + AppSettings]
ProgressConfig(ChunkIndex, InitialPercent, ContributionFactor, TotalRowCount)
WorkerConfig(SourceConnection, TargetConnection, Columns, Context)
RangeState(Completed, Checkpoints, FeedRanges)
PipelineContext(PartitionPool, Worker, Ranges, Counters, Tracker)
ReadResult(Rows, WorkChunk, IsLastPage)                          [nested in PageReader]
SeedResult(Pool, Completed, Checkpoints, PendingCount)            [nested in BulkCopyEngine]
```

## Models (mutable POCOs, JSON-serialized)

```
Job
 ├── Id, Name, Status (JobStatus enum)
 ├── Source/Target connection fields
 ├── Tables: List<TableMigrationSummary>
 └── SourceConnection / TargetConnection (ConnectionOptions)

TableMigration : TableMigrationSummary
 ├── Per-table state: keyspace, table, copy progress, change feed counters
 ├── CopyChunks: List<CopyChunk>
 │    └── CopyChunk { RowCount, Segments: List<ChunkSegment> }
 ├── CompletedCopyFeedRanges: HashSet<string>
 ├── CopyFeedRangeCheckpoints: Dictionary<string, string?>
 └── ParentJob: Job [JsonIgnore]

AppSettings : ICloneable
 └── Pipeline defaults: PageSize, WorkerMultiplier, MaxParallelTables, etc.

TableContext
 ├── TableMigrationId, JobId
 ├── KeyspaceName, TableName, TargetKeyspaceName, TargetTableName
 └── SourceSession: ISession

JobIndex
 └── MigrationJobIds: List<string>
```

## Data Flow: Bulk Copy

```
JobManager.StartMigration(jobId)
  └─► MigrationWorker.ExecuteAsync(job)
       └─► [parallel per table] BulkCopyEngine.StartProcessAsync(unitId)
            └─► ProcessChunkAsync(tableMigration, chunkIndex, context)
                 ├── CassandraQueries.GetRowCountAsync()
                 ├── CassandraQueries.GetFeedRangesAsync()
                 └─► Pipeline stages (inline in BulkCopyEngine):
                      ├── Stage 1: SeedAsync → Channel<Partition>
                      ├── Stage 2: SyncSchemaAsync
                      ├── Stage 3: ExecuteAsync → WorkerPool.Start()
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
  └─► ChangeFeedManager.AddTable(tableMigration, cts)
       └─► ReplayProcessor.AddTableToProcess(unitId, cts)
            └─► ReplayWorker.RunAsync(tableMigration, ct)
                 ├── parallel mode: N range tasks via SemaphoreSlim
                 └── each range: PollLoopAsync(tableMigration, feedRange, ps, colNames, ct)
                      loop:
                        ExecuteAsync(SELECT * WHERE COSMOS_CHANGEFEED_START_TIME()...)
                        write each row to target
                        save continuation token
                        delay(intervalMs)
```

## Inheritance

```
BulkCopyEngine (IDisposable)
     fields: _sourceSession (readonly), _targetSession (lazy via EnsureTargetSession)
     fields: _log, _job, _config, _cancellation, _worker, _changeFeedManager
```

## Session Ownership

```
┌─────────────────────────┬──────────────────────────────────┬──────────────┐
│ Session                 │ Purpose                          │ Lifetime     │
├─────────────────────────┼──────────────────────────────────┼──────────────┤
│ BulkCopyEngine          │ Metadata: row count, feed ranges │ Caller-owned │
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
