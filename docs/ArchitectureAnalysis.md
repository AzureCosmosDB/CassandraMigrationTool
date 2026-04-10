# Cassandra Migration Tool — Architecture

A Blazor Server web application for migrating data from Azure Cosmos DB for Apache Cassandra to Azure Managed Instance for Apache Cassandra (or any OSS Cassandra). Supports bulk copy with feed-range partitioned parallelism and optional online change-feed replay.

## Solution Structure

```
CassandraMigration.sln
├── CassandraMigrationProcessor/     Core library (46 files)
└── CassandraMigrationWebApp/        Blazor Server UI + services (30 files)
```

---

## CassandraMigrationProcessor

### CassandraDriver/ — DataStax driver wrappers

| File | Lines | Purpose |
|------|-------|---------|
| `CassandraClientFactory.cs` | 477 | Session creation, SSL, connection pooling |
| `CassandraClientFactory.ArmDiscovery.cs` | 239 | ARM API to discover MI contact points |
| `CassandraClientFactory.TokenRefresh.cs` | 170 | AAD token refresh for Cosmos DB auth |
| `CassandraQueries.cs` | 235 | List keyspaces/tables, COUNT(*), feed ranges, prepared inserts |
| `SchemaManager.cs` | 355 | DDL: SyncSchemaAsync, EnsureKeyspace, CreateTable, AlterColumns |

### DataTransfer/ — Bulk copy pipeline + change feed replay

**Bulk copy (3 classes, single-responsibility chain):**

| File | Lines | Purpose |
|------|-------|---------|
| `BulkCopyEngine.cs` | 185 | Orchestrator: `StartProcessAsync` → per-chunk retry loop → delegates to Runner |
| `BulkCopyRunner.cs` | 213 | Pipeline: seed partitions → schema sync → launch workers → finalize results |
| `BulkCopyWorker.cs` | 151 | Worker loop: take partition → read page → recycle → write rows → checkpoint |

**Pipeline infrastructure:**

| File | Lines | Purpose |
|------|-------|---------|
| `PageReader.cs` | 120 | Reads one page from source, owns its own session, returns `ReadResult` |
| `PageWriter.cs` | 145 | Writes rows concurrently to target, owns its own session + PreparedStatement |
| `Partition.cs` | 54 | Feed range state + WorkChunk linked list for checkpoint tracking |
| `WorkChunk.cs` | 13 | Continuation token + completion flag, linked list node |
| `PipelineContext.cs` | 40 | Records: `WorkerConfig`, `RangeState`, `PipelineCounters`, `PipelineContext` |
| `WorkerPool.cs` | 61 | Manages worker task lifecycle (Start, WaitForCompletion, Dispose) |
| `CopyProgressTracker.cs` | 294 | Single source of truth for row counters, speed, MigrationUnit updates |

**Change feed replay:**

| File | Lines | Purpose |
|------|-------|---------|
| `ReplayProcessor.cs` | 105 | Core: `AddTableToProcess`, `RunChangeFeedForAllTables` |
| `ReplayProcessor.Worker.cs` | 448 | Poll loops with reconnect logic, per-feed-range parallel replay |

**Base class:**

| File | Lines | Purpose |
|------|-------|---------|
| `MigrationProcessor.cs` | 224 | Base: session management, `EnsureTargetSession()`, cancel/pause, change feed queue |

### Context/ — Job & unit state management

| File | Lines | Purpose |
|------|-------|---------|
| `MigrationJobContext.cs` | 285 | Static coordinator: Initialize, Load/Save job list, facade for JobStore/UnitStore |
| `JobStore.cs` | 125 | Job CRUD with `SafeExecute` error handling |
| `UnitStore.cs` | 108 | Unit CRUD with `RemoveUnit` |
| `MigrationUnitCache.cs` | 57 | Thread-safe in-memory cache for active MigrationUnits |

### Infrastructure/ — Cross-cutting concerns

| File | Lines | Purpose |
|------|-------|---------|
| `MigrationLog.cs` | 171 | Structured logging with `LogType` levels, file output |
| `ExceptionClassifier.cs` | 96 | `IsTransient`, `IsFatal`, `IsNotFound`, `IsThrottle` (concrete types, no string matching) |
| `MigrationUtilities.cs` | 215 | `SafeDispose`, `IsOnline`, `GenerateMigrationUnitId`, status helpers |
| `MigrationDefaults.cs` | 15 | Constants: `WorkerMultiplier`, `MinWorkers`, `DefaultPageSize`, `CheckpointIntervalSeconds` |
| `RetryHelper.cs` | 77 | Generic retry with exponential backoff |
| `TableDiscovery.cs` | 168 | Parse/validate table name entries, wildcard expansion |
| `DataDirectoryResolver.cs` | 58 | Resolve working data directory path |

### Models/ — POCOs, enums, records

| File | Lines | Purpose |
|------|-------|---------|
| `MigrationJob.cs` | 153 | Job definition, `Status` (JobStatus enum), `ConnectionOptions` helpers |
| `MigrationUnit.cs` | 238 | Per-table state, `ToSummary()`, `UpdateParentJob()`, change feed counters |
| `MigrationSettings.cs` | 131 | App config DTO with defaults |
| `ConnectionOptions.cs` | 9 | Record: Host, Port, Username, Password, UseSsl, MaxConnectionsPerHost |
| `PipelineRequest.cs` | 13 | Record bundling pipeline params (MigrationUnit, ChunkIndex, FeedRanges, etc.) |
| `TableContext.cs` | 17 | Per-table context: keyspace/table names, source session |
| `MigrationChunk.cs` | 41 | Chunk progress: row counts, Segments list |
| `Enums.cs` | 36 | `CDCMode`, `JobStatus`, `TaskResult`, `JobType` |
| `LogTypes.cs` | 107 | `LogType` enum, `LogObject`, `LogTypeConverter` |
| `JobRegistry.cs` | 59 | Job ID list + `ConnectionAccessor` for password management |
| `TableMapping.cs` | 14 | Source→target keyspace/table name mapping |
| `TableStatus.cs` | 19 | Enum: OK, NotFound, Failed |

### Persistence/ — File-based JSON storage

| File | Lines | Purpose |
|------|-------|---------|
| `DiskPersistence.cs` | 330 | Read/write jobs and units as JSON files |
| `DiskPersistence.Logs.cs` | 436 | Log file management, rotation, bucketed reads |
| `FileSystem.cs` | 90 | File/directory abstraction (local disk only) |
| `IPersistenceStorage.cs` | 31 | Storage interface |

### Workers/ — Job lifecycle

| File | Lines | Purpose |
|------|-------|---------|
| `MigrationWorker.cs` | 328 | Per-job worker: creates `BulkCopyEngine`, manages parallel table execution |

---

## CassandraMigrationWebApp

### Pages/

| File | Lines | Purpose |
|------|-------|---------|
| `Index.razor` | 420 | Job listing with status badges |
| `MigrationJobViewer.razor` | 1852 | Job detail: table progress, logs, actions |
| `JobReport.razor` | 179 | Migration summary report |
| `Login.razor` | 135 | Authentication page |
| `ChangePassword.razor` | 197 | Password management |

### Components/

| File | Lines | Purpose |
|------|-------|---------|
| `MigrationDetails.razor` | 505 | Job create/edit form |
| `ManageCollections.razor` | 387 | Table selection and management |
| `ResetChangeStream.razor` | 290 | Change feed token reset |
| `PaginatedDownloadDialog.razor` | 207 | Paginated log download |
| `MigrationSettings.razor` | 170 | App settings editor |
| `CollectionDetails.razor` | 139 | Table detail view |
| `YesNoDialog.razor` | 87 | Confirmation dialog |
| `AuthenticationGuard.razor` | 39 | Route guard |

### Service/

| File | Lines | Purpose |
|------|-------|---------|
| `JobManager.cs` | 431 | Job lifecycle: Start/Stop/Pause/Resume, locked operations |
| `MigrationContextService.cs` | 108 | DI wrapper for static context classes |
| `PasswordManager.cs` | 154 | In-memory password storage (never persisted) |
| `AuthenticationService.cs` | 84 | Simple auth with hashed passwords |
| `MigrationHostedService.cs` | 38 | Background service for auto-start |
| `Program.cs` | 122 | DI setup, middleware, console redirect |

---

## Key Design Decisions

### Pipeline Architecture
- **Partition pool pattern**: Single `Channel<Partition>`, N unified workers. Each worker takes a partition, reads one page, recycles the partition back to the channel (so another worker can read the next page), then writes rows and marks the WorkChunk complete.
- **Per-worker sessions**: Each `PageReader`/`PageWriter` creates its own Cassandra session (1 connection/host default). No shared semaphore — driver handles backpressure.
- **Worker auto-sizing**: `CPU cores × 13 / parallel tables` (targets ~100 workers on 8 vCPU).

### Checkpoint Correctness
- Resume token = first non-completed WorkChunk's continuation token.
- Checkpoints only advance after confirmed writes (failed pages are NOT marked complete).
- `SaveCheckpoint` is unconditional; `MarkCompleted` only when partition is exhausted.
- Periodic checkpoint save via `CopyProgressTracker.UpdateMigrationUnit()`.

### Session Roles
| Session | Purpose | Lifetime |
|---------|---------|----------|
| `_sourceSession` (MigrationProcessor base) | Metadata: row count, feed ranges | Caller-owned, readonly |
| `EnsureTargetSession()` | Schema sync, keyspace creation | Lazy, one per processor |
| `PageReader._sourceSession` | Per-worker data reads | Worker-scoped |
| `PageWriter._targetSession` | Per-worker data writes | Worker-scoped |

### Online Mode
- Per-table change feed replay starts immediately after that table's bulk copy completes (not after all tables).
- Change feed runs continuously until user pauses (`MaxReconnectAttempts` = 50).
- Tables show "Replaying" status during change feed.

### Cosmos DB Specifics
- `system.size_estimates` NOT supported — only `COUNT(*)` used for row counts.
- Feed ranges = physical partitions (count depends on data size, ~50GB per partition).
- `ConsistencyLevel.One` for source reads, `ConsistencyLevel.LocalOne` for target writes.
- Passwords are `[JsonIgnore]` — never persisted to disk, re-entered on resume.
