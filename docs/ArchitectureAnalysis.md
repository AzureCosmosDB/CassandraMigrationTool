# Cassandra Migration Tool — Architecture

A Blazor Server web application for migrating data from Azure Cosmos DB for Apache Cassandra to Azure Managed Instance for Apache Cassandra (or any OSS Cassandra). Supports bulk copy with feed-range partitioned parallelism and optional online change-feed replay.

## Solution Structure

```
CassandraMigration.sln
├── CassandraMigrationProcessor/     Core library
└── CassandraMigrationWebApp/        Blazor Server UI + services
```

---

## CassandraMigrationProcessor

### Models/ — POCOs, enums, records

| File | Purpose |
|------|---------|
| `Job.cs` | Job definition: source/target connections, pipeline settings, status |
| `TableMigration.cs` | Per-table state (extends `TableMigrationSummary`): copy progress, change feed counters |
| `AppSettings.cs` | App-level configuration DTO with defaults and cloning |
| `ConnectionOptions.cs` | Record: Host, Port, Username, Password, UseSsl, MaxConnectionsPerHost |
| `PipelineRequest.cs` | Record bundling pipeline params (TableMigration, ChunkIndex, FeedRanges, etc.) |
| `TableContext.cs` | Per-table context: keyspace/table names, source session |
| `CopyChunk.cs` | Chunk progress: row counts, segments list |
| `ChunkSegment.cs` | Segment within a copy chunk and processing state |
| `JobIndex.cs` | Lightweight index of migration job IDs |
| `TableMapping.cs` | Source→target keyspace/table name mapping |
| `CDCMode.cs` | Enum: change data capture modes |
| `JobStatus.cs` | Enum: job lifecycle states |
| `JobType.cs` | Enum: migration job types |
| `TaskResult.cs` | Enum: pipeline task outcomes |
| `TableStatus.cs` | Enum: OK, NotFound, Failed |
| `LogTypes.cs` | `LogType` enum, `LogObject`, `LogTypeConverter` |
| `LogObject.cs` | Structured log entry model |

### Infrastructure/ — Cross-cutting concerns

| File | Purpose |
|------|---------|
| `MigrationLog.cs` | Structured logging with `LogType` levels, file output |
| `ExceptionClassifier.cs` | `IsTransient`, `IsFatal`, `IsNotFound`, `IsThrottle` (type-based dispatch) |
| `MigrationUtilities.cs` | `SafeDispose`, `SafeExecute`, `IsOnline`, status helpers |
| `MigrationDefaults.cs` | Constants: `WorkerMultiplier`, `MinWorkers`, `DefaultPageSize`, `CheckpointIntervalSeconds` |
| `RetryHelper.cs` | Generic retry with exponential backoff |
| `TableDiscovery.cs` | Parse/validate table name entries, wildcard expansion |
| `DataDirectoryResolver.cs` | Resolve working data directory path |
| `TableMigrationMapper.cs` | Maps `TableMigration` ↔ `TableMigrationSummary`, updates parent job tables |

### Persistence/ — File-based JSON storage

| File | Purpose |
|------|---------|
| `DiskPersistence.cs` | Read/write jobs and table migrations as JSON files |
| `LogPersistence.cs` | Log file management, rotation, bucketed reads |
| `FileSystem.cs` | File/directory abstraction (local disk) |
| `IDocumentStorage.cs` | Interface: document CRUD operations |
| `ILogStorage.cs` | Interface: log persistence, paging, export |

### Context/ — Job & table migration state management

| File | Purpose |
|------|---------|
| `MigrationJobContext.cs` | DI singleton: Initialize, Load/Save job list, facade for JobStore/UnitStore |
| `JobStore.cs` | Job CRUD with `SafeExecute` error handling |
| `UnitStore.cs` | TableMigration CRUD with `RemoveUnit` |
| `TableMigrationCache.cs` | Thread-safe in-memory cache for active TableMigration objects |
| `SettingsManager.cs` | Load/save `AppSettings` via config file |

### CassandraDriver/ — DataStax driver wrappers

| File | Purpose |
|------|---------|
| `CassandraClientFactory.cs` | Session creation, SSL, connection pooling, ARM discovery |
| `CassandraSessionFactory.cs` | DI-friendly wrapper implementing `ICassandraSessionFactory` |
| `ICassandraSessionFactory.cs` | Factory interface for creating source/target sessions |
| `CassandraQueries.cs` | List keyspaces/tables, COUNT(*), feed ranges, prepared inserts |
| `SchemaManager.cs` | DDL: SyncSchemaAsync, EnsureKeyspace, CreateTable, AlterColumns |
| `TokenRefreshManager.cs` | AAD token refresh for Cosmos DB auth |
| `ArmCredentialDiscovery.cs` | ARM API to discover MI contact points |

### DataTransfer/ — Bulk copy pipeline + change feed replay

| File | Purpose |
|------|---------|
| `TableMigrationEngine.cs` | Orchestrator: `StartProcessAsync` → per-chunk retry loop → delegates to Runner |
| `PipelineConfig.cs` | Resolved immutable pipeline settings (record) from job + app settings |
| `ProgressConfig` | Record (in PipelineContext.cs): chunk index, initial percent, contribution factor, total row count |

**BulkCopy/ — Pipeline internals:**

| File | Purpose |
|------|---------|
| `BulkCopyWorker.cs` | Worker loop: take partition → read page → recycle → write rows → checkpoint |
| `PageReader.cs` | Reads one page from source, owns its own session, returns `ReadResult` |
| `PageWriter.cs` | Writes rows concurrently to target, owns its own session + PreparedStatement |
| `Partition.cs` | Feed range state + WorkChunk linked list for checkpoint tracking |
| `WorkChunk.cs` | Continuation token + completion flag, linked list node |
| `PipelineContext.cs` | Records: `WorkerConfig`, `RangeState`, `ProgressConfig`, `PipelineContext` (with convenience properties) |
| `CopyProgressTracker.cs` | Row counters, speed calc, TableMigration updates, checkpoint saves |
| `ProgressCounters.cs` | Thread-safe atomic counters for pipeline progress/diagnostics |
| `WorkerPool.cs` | Manages worker task lifecycle (Start, WaitForCompletion, Dispose) |

**ChangeFeed/ — Online change feed replay:**

| File | Purpose |
|------|---------|
| `ChangeFeedManager.cs` | Manages change-feed replay lifecycle: create/start/stop/add tables |
| `ReplayProcessor.cs` | Core: `AddTableToProcess`, `RunChangeFeedForAllTables` |
| `ReplayWorker.cs` | Poll loops with reconnect logic, per-feed-range parallel replay |

### Workers/ — Job lifecycle

| File | Purpose |
|------|---------|
| `MigrationJobRunner.cs` | Per-job worker: creates `TableMigrationEngine`, manages parallel table execution |

---

## CassandraMigrationWebApp

### Pages/

| File | Purpose |
|------|---------|
| `Index.razor` | Job listing with status badges |
| `MigrationJobViewer.razor` | Job detail: table progress, logs, actions |
| `JobReport.razor` | Migration summary report |
| `Login.razor` | Authentication page |
| `ChangePassword.razor` | Password management |

### Components/

| File | Purpose |
|------|---------|
| `MigrationDetails.razor` | Job create/edit form |
| `ManageCollections.razor` | Table selection and management |
| `ResetChangeStream.razor` | Change feed token reset |
| `PaginatedDownloadDialog.razor` | Paginated log download |
| `MigrationSettings.razor` | App settings editor |
| `CollectionDetails.razor` | Table detail view |
| `JobSummaryCard.razor` | Job overview card component |
| `JobActionToolbar.razor` | Job action buttons |
| `TableListPanel.razor` | Table list display panel |
| `YesNoDialog.razor` | Confirmation dialog |
| `AuthenticationGuard.razor` | Route guard |

### Service/

| File | Purpose |
|------|---------|
| `JobManager.cs` | Job lifecycle: Start/Stop/Pause/Resume, locked operations |
| `MigrationContextService.cs` | DI wrapper: receives `MigrationJobContext` via constructor injection |
| `PasswordManager.cs` | In-memory password storage (never persisted) |
| `AuthenticationService.cs` | Simple auth with hashed passwords |
| `CustomAuthenticationStateProvider.cs` | Blazor auth state provider |
| `MigrationHostedService.cs` | Background service for auto-start |
| `Program.cs` | DI setup, middleware, console redirect |

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
| `_sourceSession` (TableMigrationEngine) | Metadata: row count, feed ranges | Caller-owned, readonly |
| `EnsureTargetSession()` | Schema sync, keyspace creation | Lazy, one per engine |
| `PageReader._sourceSession` | Per-worker data reads | Worker-scoped |
| `PageWriter._targetSession` | Per-worker data writes | Worker-scoped |

### Online Mode
- Per-table change feed replay starts immediately after that table's bulk copy completes (not after all tables).
- `ChangeFeedManager` manages `ReplayProcessor` lifecycle.
- Change feed runs continuously until user pauses (`MaxReconnectAttempts` = 50).
- Tables show "Replaying" status during change feed.

### Cosmos DB Specifics
- `system.size_estimates` NOT supported — only `COUNT(*)` used for row counts.
- Feed ranges = physical partitions (count depends on data size, ~50GB per partition).
- `ConsistencyLevel.One` for source reads, `ConsistencyLevel.LocalOne` for target writes.
- Passwords are `[JsonIgnore]` — never persisted to disk, re-entered on resume.
