# Cassandra Migration Tool — Architecture Analysis (Current State)

## 1. Current Architecture

### Folder Structure

```
CassandraMigration.sln
│
├── CassandraMigrationProcessor/          ← Core library
│   ├── Context/
│   │   ├── MigrationJobContext.cs        ← Static coordinator (248 lines, down from 335)
│   │   ├── JobStore.cs                   ← Job CRUD + caching (120 lines) — extracted
│   │   └── UnitStore.cs                  ← Unit CRUD + caching (82 lines) — extracted
│   ├── Models/
│   │   ├── MigrationJob.cs              ← Job definition (134 lines, no Persist())
│   │   ├── MigrationUnit.cs             ← Table-level unit (235 lines)
│   │   ├── MigrationChunk.cs            ← Chunk progress tracking
│   │   ├── MigrationSettings.cs         ← Config DTO with named constants (108 lines)
│   │   ├── CollectionInfo.cs            ← DTO for input parsing
│   │   ├── CollectionStatus.cs          ← Enum: OK, NotFound, Failed
│   │   ├── JobList.cs                   ← Job ID registry (50 lines, no Persist())
│   │   ├── ProcessorContext.cs          ← Per-table copy context
│   │   ├── Segment.cs                   ← Sub-chunk tracking
│   │   ├── TaskResult.cs / JobType.cs   ← Enums
│   │   └── LogObject.cs / LogType.cs / LogTypeConverter.cs
│   ├── Helpers/
│   │   ├── Helper.cs                    ← Validation, ParseNamespaceEntries (343 lines)
│   │   ├── Cassandra/
│   │   │   ├── CassandraClientFactory.cs ← Session creation, AAD auth (758 lines)
│   │   │   └── CassandraHelper.cs       ← CQL DDL/DML with named constants (684 lines)
│   │   └── JobManagement/
│   │       ├── ActiveMigrationUnitCache.cs
│   │       └── RetryHelper.cs           ← Named constants, exponential backoff (75 lines)
│   ├── Persistence/
│   │   ├── IPersistenceStorage.cs
│   │   ├── DiskPersistence.cs           ← File-based JSON persistence (710 lines)
│   │   └── StorageStreamFactory.cs
│   ├── Processors/
│   │   ├── MigrationProcessor.cs        ← Abstract base (236 lines)
│   │   ├── CopyProcessor.cs             ← Partial class root
│   │   ├── CopyProcessor.StartProcess.cs ← Entry point (141 lines)
│   │   ├── CopyProcessor.Pipeline.cs    ← Partition pool pipeline (243 lines)
│   │   ├── CopyProcessor.Worker.cs      ← Per-range worker + WorkChunk (539 lines)
│   │   ├── CopyProcessor.Helpers.cs     ← Partition, WorkChunk classes (209 lines)
│   │   ├── ChangeFeedProcessor.cs       ← CDC root (110 lines) — split from 1006
│   │   ├── ChangeFeedProcessor.Worker.cs ← CDC worker logic (639 lines)
│   │   └── ChangeFeedProcessor.Helpers.cs ← CDC helpers (285 lines)
│   ├── Workers/
│   │   ├── MigrationWorker.cs           ← Top-level orchestrator (605 lines)
│   │   └── CopyProgressTracker.cs       ← Progress aggregation (230 lines)
│   └── Log.cs                           ← In-memory log bucket (138 lines)
│
├── CassandraMigrationWebApp/            ← Blazor Server UI
│   ├── Program.cs                       ← DI setup, middleware
│   ├── Service/
│   │   ├── JobManager.cs                ← Singleton: job CRUD, start/stop (408 lines)
│   │   ├── MigrationHostedService.cs    ← Background service placeholder
│   │   ├── AuthenticationService.cs     ← Login logic
│   │   ├── CustomAuthenticationStateProvider.cs
│   │   ├── PasswordManager.cs           ← Password hashing/storage
│   │   └── FileService.cs              ← File download support
│   ├── Pages/                           ← Blazor pages
│   ├── Components/                      ← Blazor components
│   ├── Controller/                      ← REST: health, keepalive, file download
│   └── wwwroot/                         ← Static assets
```

### Layer Diagram

```
┌──────────────────────────────────────────────────────────┐
│  PRESENTATION (Blazor Server)                             │
│  Pages: Index, MigrationJobViewer, Login, JobReport       │
│  Components: CollectionDetails, MigrationDetails          │
│  Controllers: Health, KeepAlive, File                     │
└──────────┬───────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────┐
│  SERVICE LAYER       │
│  JobManager           │
│  AuthService          │
│  PasswordManager      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────┐
│  CONTEXT LAYER (static, partially decomposed)             │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐    │
│  │ JobStore  │  │ UnitStore│  │ MigrationJobContext   │    │
│  │ CRUD+path │  │ CRUD     │  │ State, ConnStrings,   │    │
│  │ constants │  │ + cache  │  │ Pause, Log, JobList   │    │
│  └──────────┘  └──────────┘  └──────────────────────┘    │
└──────────┬───────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────┐
│  PIPELINE / WORKERS                                       │
│  MigrationWorker ──► CopyProcessor (partition pool)       │
│                  ──► ChangeFeedProcessor (split into 3)   │
│  CopyProgressTracker                                      │
└──────────┬───────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────┐
│  INFRASTRUCTURE                                           │
│  CassandraClientFactory · CassandraHelper                 │
│  DiskPersistence · RetryHelper                            │
└──────────────────────────────────────────────────────────┘
```

---

## 2. Completed Improvements

| # | Issue | What Changed |
|---|-------|-------------|
| ✅ | **God class decomposition** | `MigrationJobContext` (335→248 lines) split into `JobStore` (120 lines) and `UnitStore` (82 lines). Job/unit CRUD moved out. |
| ✅ | **Models no longer own persistence** | `MigrationJob.Persist()`, `JobList.Persist()` removed. Persistence handled by `JobStore.SaveJob()` and `MigrationJobContext.SaveJobList()`. |
| ✅ | **Silent exceptions reduced** | Original 22+ bare `catch {}` reduced to 3 (`Helper.cs:214,385`, `CopyProcessor.Worker.cs:120`). Most replaced with `catch (Exception ex)` + `Console.WriteLine` logging. |
| ✅ | **Hardcoded file paths centralized** | `JobStore.JobsFolder` constant + `JobStore.GetJobDefinitionPath()`. All path construction references `JobStore.JobsFolder`. |
| ✅ | **`ChangeFeedProcessor` split** | Original 1006-line monolith → 3 partial class files: root (110), Worker (639), Helpers (285). |
| ✅ | **`DocumentCopyWorker` removed** | Row-level copy logic inlined into `CopyProcessor.Worker.cs`. `SingleRangeCopyWorker` also removed. |
| ✅ | **`TotalDocCount` renamed** | Now `TotalRowCount` in `MigrationUnit` and `ProcessorContext`. |
| ✅ | **Namespace parsing consolidated** | `Helper.ParseNamespaceEntries()` extracted as single source for parsing `keyspace.table` input strings. |
| ✅ | **Magic numbers extracted** | `CassandraHelper`: timeout/retry constants. `MigrationSettings`: 10+ `Default*` constants with validation. `RetryHelper`: `DefaultMaxTries`, `DefaultInitialDelayMs`, `MaxBackoffMs`. |
| ✅ | **`MigrationJob.Tables` property** | Renamed from `NameSpaces` internally (JSON wire format preserved via `[JsonProperty("NameSpaces")]`). |
| ✅ | **`JobStatus` enum** | Replaces individual boolean flags with `Pending/Running/Paused/Completed/Cancelled/Faulted`. |
| ✅ | **Partition pool + WorkChunk pipeline** | `CopyProcessor.Pipeline.cs` implements channel-based partition recycling (see §4). |

---

## 3. Remaining Improvements

### HIGH

| # | Issue | Location | Impact |
|---|-------|----------|--------|
| 1 | **`MigrationJobContext` still static** | `Context/MigrationJobContext.cs` | Not injectable. `JobStore`/`UnitStore` also static. All 3 need DI. |
| 2 | **No DI in Processor library** | All of `CassandraMigrationProcessor` | `new Log()`, `new DiskPersistence()`, `new MigrationSettings()` inline. Zero constructor injection. |
| 3 | **Connection strings in static memory** | `MigrationJobContext.SourceConnectionString` | Credentials in `ConcurrentDictionary` indefinitely; no secure storage. |
| 4 | **Fire-and-forget `Task.Run`** | `JobManager.cs` | Unobserved exceptions. `MigrationHostedService` exists but is a no-op. |

### MEDIUM

| # | Issue | Location | Impact |
|---|-------|----------|--------|
| 5 | **`MigrationUnit.Remove()` on model** | `MigrationUnit.cs:46` | Calls `MigrationJobContext.Store.DeleteDocument` directly. Should be in `UnitStore`. |
| 6 | **`MigrationSettings.Load()` on model** | `MigrationSettings.cs:79` | Self-loading config; should be in a settings service. |
| 7 | **`Log` not `IDisposable`** | `Log.cs` | Has `Dispose()` but class doesn't implement the interface. |
| 8 | **3 remaining bare `catch {}`** | `Helper.cs:214,385`, `CopyProcessor.Worker.cs:120` | Silent swallowing in edge cases. |
| 9 | **Persistence uses document terminology** | `IPersistenceStorage.cs`, `DiskPersistence.cs` | `UpsertDocument`/`ReadDocument`/`DeleteDocument` — file ops named as document ops. |

### LOW

| # | Issue | Location | Notes |
|---|-------|----------|-------|
| 10 | **Large files** | `CassandraClientFactory` (758), `DiskPersistence` (710), `CassandraHelper` (684), `ChangeFeedProcessor.Worker` (639), `MigrationWorker` (605), `CopyProcessor.Worker` (539) | Could benefit from further decomposition. |
| 11 | **Naming inconsistencies** | `GetBasic()`, `MigrationUnitBasics`, `GetCurentLogBucket` (typo), `ActiveMigrationUnitsCache`, `mub`/`mu` abbreviations | See §5. |
| 12 | **`#pragma warning disable CS8618`** | `MigrationJob.cs` | Hides nullable warnings. |
| 13 | **`CollectionInfo`/`CollectionStatus`** | Models | MongoDB terminology; should be `TableInfo`/`TableStatus`. |

---

## 4. Pipeline Architecture

The copy pipeline uses a channel-based partition pool with `WorkChunk` tracking:

```
                     ┌───────────────────────────────────┐
                     │         Partition Pool             │
                     │    (Channel<Partition>)             │
                     │  ┌─────┐ ┌─────┐ ┌─────┐         │
  seed N ranges ───► │  │ FR1 │ │ FR2 │ │ FR3 │ ...      │
                     │  └─────┘ └─────┘ └─────┘         │
                     └──────┬────────────────────────────┘
                            │
               ┌────────────┼────────────┐
               ▼            ▼            ▼
          ┌─────────┐ ┌─────────┐ ┌─────────┐
          │ Worker 1│ │ Worker 2│ │ Worker N│  (auto-scaled)
          │         │ │         │ │         │
          │ 1. Take │ │         │ │         │
          │    part. │ │         │ │         │
          │ 2. Read  │ │         │ │         │
          │    page  │ │         │ │         │
          │ 3.Create │ │         │ │         │
          │  WorkChk │ │         │ │         │
          │ 4.Recycle│ │         │ │         │
          │   part.  │ │         │ │         │
          │ 5. Write │ │         │ │         │
          │   rows   │ │         │ │         │
          │ 6. Mark  │ │         │ │         │
          │   done   │ │         │ │         │
          └─────────┘ └─────────┘ └─────────┘
               │            │            │
               └────────────┼────────────┘
                            ▼
                   Recycle back to pool
                   (if partition has more pages)

  Key types:
  ─────────
  Partition      — Feed range + paging state + WorkChunk linked list
  WorkChunk      — Continuation token + completion flag + linked list
  PipelineContext — Shared counters (TotalRead/Written/Failed)
```

Worker count: `max(4, ProcessorCount × 13 / parallelTables)`,
overridable via `MigrationJob.MaxFeedRangeParallelism`.

---

## 5. Remaining Naming Issues

| Current | Location | Suggested | Issue |
|---|---|---|---|
| `MigrationUnitBasics` | `MigrationJob.cs` | `MigrationUnitSummaries` | "Basics" unclear |
| `GetBasic()` | `MigrationUnit.cs:237` | `ToSummary()` | Doesn't describe transform |
| `ActiveMigrationUnitsCache` | Cache class | `MigrationUnitCache` | "Active" redundant |
| `GetCurentLogBucket` | `Log.cs:141` | `GetCurrentLogBucket` | Typo |
| `CollectionInfo` / `CollectionStatus` | Models | `TableInfo` / `TableStatus` | MongoDB terms |
| `UpsertDocument` / `ReadDocument` | `IPersistenceStorage`, `DiskPersistence` | `UpsertFile` / `ReadFile` | File ops, not document ops |
| `mub`, `mu`, `ks`, `tbl` | Throughout | `summary`, `unit`, `keyspace`, `table` | Cryptic abbreviations |
