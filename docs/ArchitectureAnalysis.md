# Cassandra Migration Tool — Architecture Analysis

## 1. Current Architecture

### Folder Structure & Responsibilities

```
CassandraMigration.sln
│
├── CassandraMigrationProcessor/          ← Core library (no DI, static state)
│   ├── Context/
│   │   └── MigrationJobContext.cs        ← STATIC god class: job store, persistence, cache, locking
│   ├── Models/
│   │   ├── MigrationJob.cs               ← Job definition + self-persistence
│   │   ├── MigrationUnit.cs              ← Table-level unit + self-persistence + parent sync
│   │   ├── MigrationChunk.cs             ← Chunk progress tracking
│   │   ├── CollectionInfo.cs             ← DTO for JSON/CSV input parsing
│   │   ├── CollectionStatus.cs           ← Enum: OK, NotFound, Failed
│   │   ├── JobList.cs                    ← Job ID registry + self-persistence
│   │   ├── MigrationSettings.cs          ← Config DTO + self-load/save
│   │   ├── ProcessorContext.cs           ← Per-table copy context DTO
│   │   ├── Segment.cs                    ← Sub-chunk tracking
│   │   ├── TaskResult.cs                 ← Enum: Success, Retry, Abort, etc.
│   │   ├── JobType.cs                    ← Enum: CqlCopy
│   │   ├── LogObject.cs / LogType.cs     ← Log entry model + severity enum
│   │   └── LogTypeConverter.cs           ← JSON converter for LogType
│   ├── Helpers/
│   │   ├── Helper.cs                     ← Static utility: validation, unit creation, formatting
│   │   ├── Cassandra/
│   │   │   ├── CassandraClientFactory.cs ← Session creation, AAD auth, retry logic (743 lines)
│   │   │   └── CassandraHelper.cs        ← CQL operations: DDL, DML, feed ranges (666 lines)
│   │   └── JobManagement/
│   │       ├── ActiveMigrationUnitCache.cs ← In-memory cache backed by MigrationJobContext
│   │       └── RetryHelper.cs            ← Generic retry-with-backoff
│   ├── Persistence/
│   │   ├── IPersistenceStorage.cs        ← Storage interface
│   │   ├── DiskPersistence.cs            ← File-based JSON persistence (716 lines)
│   │   └── StorageStreamFactory.cs       ← Stream helpers
│   ├── Processors/
│   │   ├── MigrationProcessor.cs         ← Abstract base: sessions, cancel, change feed
│   │   ├── CopyProcessor.cs             ← Partial class root
│   │   ├── CopyProcessor.StartProcess.cs ← Entry point for copy
│   │   ├── CopyProcessor.Pipeline.cs     ← Feed-range pipeline
│   │   ├── CopyProcessor.Worker.cs       ← Per-range worker logic
│   │   ├── CopyProcessor.Helpers.cs      ← Copy helpers
│   │   └── ChangeFeedProcessor.cs        ← CDC replication (1006 lines!)
│   ├── Workers/
│   │   ├── MigrationWorker.cs            ← Top-level orchestrator (596 lines)
│   │   ├── DocumentCopyWorker.cs         ← Row-level copy logic (490 lines)
│   │   └── CopyProgressTracker.cs        ← Progress aggregation + logging
│   └── Log.cs                            ← In-memory log bucket + file persistence
│
├── CassandraMigrationWebApp/             ← Blazor Server UI
│   ├── Program.cs                        ← DI setup, middleware
│   ├── Service/
│   │   ├── JobManager.cs                 ← Singleton: job CRUD, start/stop migration
│   │   ├── MigrationHostedService.cs     ← Background service (placeholder)
│   │   ├── AuthenticationService.cs      ← Login logic
│   │   ├── CustomAuthenticationStateProvider.cs
│   │   ├── PasswordManager.cs            ← Password hashing/storage
│   │   └── FileService.cs               ← File download support
│   ├── Pages/                            ← Blazor pages (Index, Login, Viewer, etc.)
│   ├── Components/                       ← Blazor components (details, dialogs)
│   ├── Controller/                       ← REST: health, keepalive, file download
│   └── wwwroot/                          ← Static assets
```

### Layer Diagram

```
┌─────────────────────────────────────────────────────┐
│  PRESENTATION (Blazor Server)                        │
│  Pages: Index, MigrationJobViewer, Login, JobReport  │
│  Components: CollectionDetails, MigrationDetails     │
│  Controllers: Health, KeepAlive, File                │
└──────────┬──────────────────────────────────────┬────┘
           │                                      │
           ▼                                      │
┌──────────────────────┐                          │
│  SERVICE LAYER       │                          │
│  JobManager (single) │                          │
│  AuthService         │                          │
│  PasswordManager     │                          │
└──────────┬───────────┘                          │
           │                                      │
           ▼                                      │
┌──────────────────────────────────────────────────▼──┐
│  STATIC GOD CLASS: MigrationJobContext              │
│  ┌─ Job CRUD ─┐ ┌─ Persistence ─┐ ┌─ Cache ─┐     │
│  │ Load/Save  │ │ Store (Disk)   │ │ MU Cache│     │
│  │ JobList    │ │ Conn Strings   │ │ Active  │     │
│  └────────────┘ └────────────────┘ └─────────┘     │
│  ┌─ State ────┐ ┌─ Logging ──────┐                  │
│  │ ActiveJob  │ │ VerboseLog     │                  │
│  │ PauseFlag  │ │ InitializeLog  │                  │
│  └────────────┘ └────────────────┘                  │
└──────────┬──────────────────────────────────────────┘
           │ (everything calls this directly)
           ▼
┌──────────────────────────────────────────────────────┐
│  PIPELINE / WORKERS                                   │
│  MigrationWorker → CopyProcessor → DocumentCopyWorker │
│                  → ChangeFeedProcessor                │
│  CopyProgressTracker                                  │
└──────────┬───────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────┐
│  INFRASTRUCTURE                                       │
│  CassandraClientFactory (session creation, AAD)       │
│  CassandraHelper (CQL DDL/DML operations)             │
│  DiskPersistence (file-based JSON storage)             │
│  RetryHelper                                           │
└──────────────────────────────────────────────────────┘
```

---

## 2. Top Issues (Ranked by Impact)

### CRITICAL

| # | Issue | Location | Impact |
|---|-------|----------|--------|
| 1 | **Static God Class: `MigrationJobContext`** | `Context/MigrationJobContext.cs` (335 lines) | Untestable, prevents DI, holds ALL mutable state (jobs, cache, persistence, connection strings, pause flags, log). Every class in the system calls it directly — ~130 call sites across the codebase. |
| 2 | **Models own persistence** | `MigrationJob.Persist()`, `MigrationUnit.Persist()`, `MigrationUnit.Remove()`, `JobList.Persist()`, `MigrationSettings.Load()/Save()` | Domain models are tightly coupled to `MigrationJobContext.Store`. Prevents unit testing models, violates SRP. |
| 3 | **Silent exception swallowing** | 22+ `catch { }` / `catch { return false/null; }` across 9 files | Persistence failures, auth errors, and data corruption are silently hidden. Most dangerous in `SaveMigrationUnit`, `SaveMigrationJob`, `LoadMigrationJob`. |

### HIGH

| # | Issue | Location | Impact |
|---|-------|----------|--------|
| 4 | **No dependency injection in Processor library** | All of `CassandraMigrationProcessor` | `new Log()`, `new DiskPersistence()`, `new MigrationSettings()` — all created inline. Zero constructor injection. |
| 5 | **God files** | `ChangeFeedProcessor.cs` (1006 lines), `CassandraClientFactory.cs` (743), `DiskPersistence.cs` (716), `CassandraHelper.cs` (666), `MigrationWorker.cs` (596) | Hard to navigate, review, and test. Multiple responsibilities in single files. |
| 6 | **Connection strings stored in static `ConcurrentDictionary`** | `MigrationJobContext.SourceConnectionString`, `.TargetConnectionString` | Credentials live in static memory indefinitely; no expiration, no secure storage. |
| 7 | **Fire-and-forget `Task.Run` in `JobManager.StartMigration`** | `JobManager.cs:311` | Unobserved exceptions possible. Status management split between `Task.Run` finally block and caller. |

### MEDIUM

| # | Issue | Location | Impact |
|---|-------|----------|--------|
| 8 | **Mongo/document terminology in Cassandra code** | Throughout (see §6) | Confusing for maintainers. "Collection", "Document", "TotalDocCount", "NameSpaces" are all MongoDB terms. |
| 9 | **Helper as static utility dumping ground** | `Helper.cs` (318 lines) | Mixes: validation, unit creation, formatting, file I/O, job queries. No cohesion. |
| 10 | **Duplicate logic** | `ExpandWildcardTables` (JobManager) vs `PopulateJobTablesAsync` (Helper) | Two different paths to parse namespace strings into MigrationUnits. |
| 11 | **`Log` class not IDisposable properly** | `Log.cs` — has `Dispose()` method but doesn't implement `IDisposable` | Misleading API; can't use in `using` blocks. |
| 12 | **Hardcoded file paths** | `"migrationjobs\\joblist.json"`, `"migrationjobs\\{id}\\jobdefinition.json"` scattered in 5+ files | Path construction duplicated; Windows-only separators. |

### LOW

| # | Issue | Location | Impact |
|---|-------|----------|--------|
| 13 | **Unused `MigrationHostedService`** | `MigrationHostedService.cs` | Registered as hosted service but does nothing (just `await Task.Delay(Infinite)`). |
| 14 | **`#pragma warning disable CS8618`** | `MigrationUnit.cs`, `MigrationJob.cs`, `JobList.cs` | Hides nullable warnings instead of fixing them. |
| 15 | **Magic numbers** | `PageSize=500`, `MaxRetries=10`, `delay*3`, `cap=30`, `300 logs`, etc. | Scattered constants with no documentation. |

---

## 3. Refactoring Plan

### Phase 1: Safety Net (Effort: Small)

| Step | Action | Files | Effort |
|------|--------|-------|--------|
| 1.1 | **Add logging to all `catch { }` blocks** — Replace silent catches with `_log?.WriteLine(ex.Message, LogType.Warning)` or at minimum `Console.Error.WriteLine`. | 9 files, 22 sites | S |
| 1.2 | **Extract path constants** — Create `StoragePaths` static class with `JobDefinitionPath(id)`, `MigrationUnitPath(jobId, unitId)`, `JobListPath`, `ConfigPath`. | New file + update 5 files | S |
| 1.3 | **Remove unused `MigrationHostedService`** or implement auto-resume. | 2 files | S |
| 1.4 | **Fix `Log` to implement `IDisposable`** properly. | `Log.cs` | S |

### Phase 2: Break the God Class (Effort: Large)

| Step | Action | Files | Effort |
|------|--------|-------|--------|
| 2.1 | **Extract `IMigrationJobRepository`** interface from `MigrationJobContext` — methods: `GetJob`, `SaveJob`, `GetUnit`, `SaveUnit`, `GetJobList`, `SaveJobList`. Implement with `DiskMigrationJobRepository`. | New interface + impl | M |
| 2.2 | **Extract `IMigrationStateManager`** — wraps `ActiveMigrationJobId`, `ControlledPauseRequested`, `CurrentlyActiveJob`, connection string storage. | New interface + impl | M |
| 2.3 | **Register both as singletons in DI** — Replace static calls with injected instances. Start with `JobManager` (it already has constructor injection for `IConfiguration`). | `Program.cs`, `JobManager.cs` | M |
| 2.4 | **Migrate callers incrementally** — Each processor/worker receives the repository through constructor. `MigrationJobContext` becomes a thin facade delegating to the new interfaces until fully removed. | All processor files | L |

### Phase 3: Separate Persistence from Models (Effort: Medium)

| Step | Action | Files | Effort |
|------|--------|-------|--------|
| 3.1 | **Remove `Persist()` from `MigrationJob`, `MigrationUnit`, `JobList`, `MigrationSettings`**. Move to the repository. | 4 model files + repository | M |
| 3.2 | **Remove `Remove()` from `MigrationUnitBasic`** — deletion belongs in the repository. | `MigrationUnit.cs` | S |
| 3.3 | **Remove direct `MigrationJobContext.Store` calls from models**. | 4 model files | S |

### Phase 4: Decompose God Files (Effort: Medium)

| Step | Action | Files | Effort |
|------|--------|-------|--------|
| 4.1 | **Split `ChangeFeedProcessor`** (1006 lines) into: `ChangeFeedReader`, `ChangeFeedWriter`, `ChangeFeedOrchestrator`. | 1 → 3 files | M |
| 4.2 | **Split `CassandraClientFactory`** (743 lines) — Extract `AadTokenProvider`, `SessionBuilder`, `RetryPolicy` into separate classes. | 1 → 3 files | M |
| 4.3 | **Split `Helper.cs`** into: `NamespaceParser` (validation/parsing), `MigrationUnitFactory` (unit creation), `TimeFormatter`. | 1 → 3 files | S |
| 4.4 | **Consolidate duplicate namespace parsing** — `ExpandWildcardTables` (JobManager) should delegate to `PopulateJobTablesAsync` (Helper). | 2 files | S |

### Phase 5: Naming Cleanup (Effort: Small)

See §4 below for the full rename list.

### Phase 6: Eliminate Fire-and-Forget (Effort: Medium)

| Step | Action | Files | Effort |
|------|--------|-------|--------|
| 6.1 | **Move migration execution to `MigrationHostedService`** via a `Channel<MigrationRequest>`. JobManager enqueues; hosted service dequeues and runs. | 2 files | M |
| 6.2 | **Remove `Task.Run` from `StartMigration`**. | `JobManager.cs` | S |

---

## 4. Naming Cleanup — Full Rename List

### MongoDB Terminology Still Present

| Current Name | Location | Proposed Name | Rationale |
|---|---|---|---|
| `TotalDocCount` | `MigrationUnitBasic.cs:39` | `TotalRowCount` | Cassandra has rows, not documents |
| `CollectionInfo` | `Models/CollectionInfo.cs` | `TableMapping` or `TableInfo` | Cassandra has tables, not collections |
| `CollectionStatus` | `Models/CollectionStatus.cs` | `TableStatus` | Same reason |
| `SourceStatus` (type `CollectionStatus`) | `MigrationUnitBasic.cs:40` | `SourceTableStatus` (type `TableStatus`) | Consistency |
| `NameSpaces` | `MigrationJob.cs:58` | `KeyspaceTables` | Cassandra calls them keyspaces, and this holds `keyspace.table` pairs |
| `DocumentCopyWorker` | `Workers/DocumentCopyWorker.cs` | `RowCopyWorker` | Cassandra copies rows |
| `UpsertDocument` / `ReadDocument` / `DeleteDocument` / `DocumentExists` / `ListDocumentIds` | `IPersistenceStorage.cs`, `DiskPersistence.cs` | `UpsertFile` / `ReadFile` / `DeleteFile` / `FileExists` / `ListFileIds` | These are file operations, not document DB ops |
| `source documents` / `document count` | Log messages in `CopyProcessor.cs:74,86` | `source rows` / `row count` | |

### Inconsistent Naming Patterns

| Current | Location | Proposed | Issue |
|---|---|---|---|
| `MigrationUnitBasics` | `MigrationJob.cs:155` | `MigrationUnitSummaries` | "Basics" is unclear |
| `GetBasic()` | `MigrationUnit.cs:249` | `ToSummary()` | Verb should describe the transform |
| `ActiveMigrationUnitsCache` | Cache class | `MigrationUnitCache` | "Active" is redundant (all cached units are active) |
| `GetCurentLogBucket` | `Log.cs:143` | `GetCurrentLogBucket` | Typo: "Curent" |
| `_migrationJobsBackingField` | `JobList.cs:19` | `_legacyMigrationJobs` | Clearer intent |
| `stateStoreCSorPath` | `MigrationJobContext.cs:147` | `stateStorePathOrConnectionString` | Abbreviation harms readability |
| `mub` | Throughout | `unitSummary` or `summary` | Cryptic abbreviation |
| `mu` | Throughout | `unit` or `migrationUnit` | Cryptic abbreviation |
| `ks` | `Helper.cs`, `JobManager.cs` | `keyspace` | |
| `tbl` | `Helper.cs`, `JobManager.cs` | `table` | |

---

## 5. Dependency Graph — Key Coupling Issues

### Static Dependency Hub: `MigrationJobContext`

Every significant class depends on this static class:

```
MigrationJobContext (static)
  ← MigrationJob.Persist()
  ← MigrationUnit.Persist(), Remove()
  ← MigrationUnitBasic.Persist(), Remove()
  ← JobList.Persist()
  ← MigrationSettings.Load(), Save()
  ← Helper (6+ methods)
  ← MigrationWorker
  ← MigrationProcessor (base)
  ← CopyProcessor (all partials)
  ← ChangeFeedProcessor
  ← DocumentCopyWorker
  ← CopyProgressTracker (indirect)
  ← ActiveMigrationUnitsCache
  ← JobManager
  ← Log
  ← Blazor Pages (via JobManager)
```

### Circular-ish Dependencies

- `MigrationUnit` → `MigrationJobContext` → `ActiveMigrationUnitsCache` → `MigrationJobContext`
- `MigrationJob.Persist()` → `MigrationJobContext.Store` → `DiskPersistence`, but `MigrationJobContext.Initialize()` creates `DiskPersistence` and loads `JobList` which calls `Persist()` during init.
- `MigrationProcessor` holds `MigrationWorker?` reference; `MigrationWorker` creates `MigrationProcessor` instances. Bidirectional ownership.

### Class Instantiation Map

```
JobManager
  └─ new MigrationWorker(log)
  └─ new MigrationSettings() → .Load()
  └─ new Log()

MigrationWorker
  └─ new CopyProcessor(log, session, config, job, this)
  └─ CassandraClientFactory.CreateSourceSession(...)

CopyProcessor (extends MigrationProcessor)
  └─ new DocumentCopyWorker() → .Initialize(...)
  └─ new CopyProgressTracker(...)
  └─ CassandraClientFactory.CreateTargetSession(...)
  └─ new ChangeFeedProcessor(...)

MigrationProcessor (base)
  └─ new CancellationTokenSource()
  └─ CassandraClientFactory.Create*Session(...)
```

---

## 6. Code Smells Summary

### Silent Exception Handling (22 sites)

| File | Line | Pattern |
|---|---|---|
| `MigrationJobContext.cs` | 159, 201, 253, 325, 341, 373 | `catch { }`, `catch { return null; }`, `catch { return false; }` |
| `Helper.cs` | 60, 213, 329 | `catch { }` |
| `MigrationWorker.cs` | 326, 529, 572 | `catch { }` |
| `CopyProcessor.Worker.cs` | 192, 261, 262, 423 | `catch { }` |
| `CassandraClientFactory.cs` | 79, 149, 677 | `catch { }`, `catch { return null; }` |
| `ChangeFeedProcessor.cs` | 869 | `catch { }` |
| `MigrationProcessor.cs` | 260 | `catch { }` |
| `Program.cs` | 24 | `catch { }` |
| `JobManager.cs` | 136 | `catch { }` |

### Files Over 300 Lines (God Files)

| File | Lines | Recommendation |
|---|---|---|
| `ChangeFeedProcessor.cs` | 1006 | Split into 3 classes |
| `CassandraClientFactory.cs` | 743 | Extract AAD + retry |
| `DiskPersistence.cs` | 716 | Extract log persistence |
| `CassandraHelper.cs` | 666 | Extract DDL vs DML |
| `MigrationWorker.cs` | 596 | Extract unit processing |
| `DocumentCopyWorker.cs` | 490 | OK for partial class |
| `CopyProcessor.Worker.cs` | 448 | Already a partial |
| `JobManager.cs` | 408 | Extract wildcard expansion |
| `MigrationJobContext.cs` | 335 | Split into repository + state |
| `Helper.cs` | 318 | Split into 3 classes |

### Deep Nesting (3+ levels)

- `MigrationWorker.ProcessMigrationUnitAsync`: try → if → try → if → foreach → try (6 levels)
- `JobManager.ExpandWildcardTables`: foreach → if → try → using → foreach → try → for (7 levels)
- `ChangeFeedProcessor`: multiple 5+ level nesting blocks

---

## 7. Estimated Effort Summary

| Phase | Description | Effort | Risk |
|-------|-------------|--------|------|
| **Phase 1** | Safety net (logging, paths, cleanup) | **Small** (2-3 days) | Low |
| **Phase 2** | Break `MigrationJobContext` god class | **Large** (5-8 days) | Medium — touches every file |
| **Phase 3** | Remove persistence from models | **Medium** (2-3 days) | Low — follows Phase 2 |
| **Phase 4** | Decompose god files | **Medium** (3-4 days) | Low — mechanical splits |
| **Phase 5** | Naming cleanup | **Small** (1-2 days) | Low — rename refactor |
| **Phase 6** | Eliminate fire-and-forget | **Medium** (2-3 days) | Medium — concurrency changes |
| **Total** | | **~15-23 days** | |

### Recommended Order

**Phase 1 → 5 → 2 → 3 → 4 → 6**

Start with safety (Phase 1) and naming (Phase 5) since they're low-risk and immediately improve readability. Then tackle the god class (Phase 2) which is the highest-impact change, followed by the dependent phases.
