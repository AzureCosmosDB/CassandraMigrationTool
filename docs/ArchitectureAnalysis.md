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
│   │   └── UnitStore.cs                  ← Unit CRUD + caching + RemoveUnit (82 lines)
│   ├── Models/
│   │   ├── MigrationJob.cs              ← Job definition (134 lines, no Persist())
│   │   ├── MigrationUnit.cs             ← Table-level unit (235 lines, ToSummary)
│   │   ├── MigrationChunk.cs            ← Chunk progress tracking
│   │   ├── MigrationSettings.cs         ← Config DTO with named constants (108 lines)
│   │   ├── TableMapping.cs              ← DTO for input parsing (renamed from CollectionInfo)
│   │   ├── TableStatus.cs               ← Enum: OK, NotFound, Failed (renamed)
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
│   │       ├── MigrationUnitCache.cs    ← Renamed from ActiveMigrationUnitCache
│   │       └── RetryHelper.cs           ← Named constants, exponential backoff (75 lines)
│   ├── Persistence/
│   │   ├── IPersistenceStorage.cs       ← Write/Read/Delete (renamed from document terms)
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
│   └── Log.cs                           ← IDisposable, in-memory log bucket (138 lines)
│
├── CassandraMigrationWebApp/            ← Blazor Server UI
│   ├── Program.cs                       ← DI setup, middleware
│   ├── Service/
│   │   ├── JobManager.cs                ← Singleton: job CRUD, _migrationTask tracked (408 lines)
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
│  │ constants │  │ +Remove  │  │ Pause, Log, JobList   │    │
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
| ✅ | **Silent exceptions fixed** | Original 22+ bare `catch {}` all replaced with `catch (Exception ex)` + logging. Zero bare catches remain. |
| ✅ | **Hardcoded file paths centralized** | `JobStore.JobsFolder` constant + `JobStore.GetJobDefinitionPath()`. All path construction references `JobStore.JobsFolder`. |
| ✅ | **`ChangeFeedProcessor` split** | Original 1006-line monolith → 3 partial class files: root (110), Worker (639), Helpers (285). |
| ✅ | **`DocumentCopyWorker` removed** | Row-level copy logic inlined into `CopyProcessor.Worker.cs`. `SingleRangeCopyWorker` also removed. |
| ✅ | **`TotalDocCount` renamed** | Now `TotalRowCount` in `MigrationUnit` and `ProcessorContext`. |
| ✅ | **Namespace parsing consolidated** | `Helper.ParseNamespaceEntries()` extracted as single source for parsing `keyspace.table` input strings. |
| ✅ | **Magic numbers extracted** | `CassandraHelper`: timeout/retry constants. `MigrationSettings`: 10+ `Default*` constants with validation. `RetryHelper`: `DefaultMaxTries`, `DefaultInitialDelayMs`, `MaxBackoffMs`. |
| ✅ | **`MigrationJob.Tables` property** | Renamed from `NameSpaces` internally (JSON wire format preserved via `[JsonProperty("NameSpaces")]`). |
| ✅ | **`JobStatus` enum** | Replaces individual boolean flags with `Pending/Running/Paused/Completed/Cancelled/Faulted`. |
| ✅ | **Partition pool + WorkChunk pipeline** | `CopyProcessor.Pipeline.cs` implements channel-based partition recycling (see §4). |
| ✅ | **`MigrationUnit.Remove()` moved** | Delete logic moved from model to `UnitStore.RemoveUnit()`. Model no longer calls persistence. |
| ✅ | **`Log` implements `IDisposable`** | `Log : IDisposable` declared; was missing the interface despite having `Dispose()`. |
| ✅ | **All bare `catch {}` fixed** | 3 remaining bare catches (Helper.cs, CopyProcessor.Worker.cs) replaced with `catch (Exception ex)` + logging. |
| ✅ | **Persistence terminology** | `IPersistenceStorage`: `UpsertDocument`→`Write`, `ReadDocument`→`Read`, `DeleteDocument`→`Delete`. Matches file-based reality. |
| ✅ | **Fire-and-forget `Task.Run` tracked** | `JobManager._migrationTask` field stores the task. No longer fire-and-forget. |
| ✅ | **Naming fixes** | `GetBasic()`→`ToSummary()`, `GetCurentLogBucket`→`GetCurrentLogBucket` (typo), `ActiveMigrationUnitCache`→`MigrationUnitCache`. |
| ✅ | **`CollectionInfo`/`CollectionStatus` renamed** | Now `TableMapping` and `TableStatus`. MongoDB terminology removed from models. |
| ✅ | **`#pragma CS8618` removed** | `MigrationJob.cs` no longer suppresses nullable warnings. |

---

## 3. Remaining Improvements

| Priority | # | Issue | Location | Notes |
|----------|---|-------|----------|-------|
| HIGH | 1 | **`MigrationJobContext` still static** | `Context/MigrationJobContext.cs` | Not injectable. `JobStore`/`UnitStore` also static. All 3 need DI. TODO documented in code. |
| HIGH | 2 | **No DI in Processor library** | All of `CassandraMigrationProcessor` | `new Log()`, `new DiskPersistence()`, `new MigrationSettings()` inline. Zero constructor injection. TODO documented. |
| HIGH | 3 | **Connection strings in static memory** | `MigrationJobContext.SourceConnectionString` | Credentials in `ConcurrentDictionary` indefinitely. Documented as intentional for current scope. |
| LOW  | 4 | **Large files (600+ lines)** | `CassandraClientFactory` (758), `DiskPersistence` (710), `CassandraHelper` (684), `ChangeFeedProcessor.Worker` (639), `MigrationWorker` (605), `CopyProcessor.Worker` (539) | Future decomposition candidates. |

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
| `mub`, `mu`, `ks`, `tbl` | Throughout | `summary`, `unit`, `keyspace`, `table` | Cryptic abbreviations |
