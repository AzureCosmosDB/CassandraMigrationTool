# Principal Architect Review — Improvement Recommendations

## Executive Summary

The codebase has strong pipeline design (BulkCopyEngine → Runner → Worker chain) with proper separation of concerns in the DataTransfer layer. However, the Infrastructure, Context, and CassandraDriver layers are heavily static with global mutable state, making them untestable and fragile under concurrency.

---

## A. Critical: Static Global State (affects testability + thread safety)

### A.1 MigrationJobContext is a static god class (285 lines, 31 static members)
- Holds global mutable state: `CurrentlyActiveJob`, `Store`, `MigrationUnitsCache`, `JobRegistry`
- Every class reaches into it via `MigrationJobContext.X` static calls
- Cannot be mocked, tested, or run in parallel

**Fix:** Convert to a singleton registered in DI. Replace all `MigrationJobContext.X` calls with injected instance. `MigrationContextService` already wraps it — make that the actual implementation.

### A.2 Six static utility classes with hidden state
| Class | Static State | Risk |
|-------|-------------|------|
| `TokenRefreshManager` | Timer, session ref, token cache | Race conditions on token refresh |
| `JobStore` | `_jobs` dict, `_cachedActiveJob` | Stale cache, no invalidation |
| `UnitStore` | `_writeMULock` | Partial locking |
| `DataDirectoryResolver` | `_workingFolder` | Mutable static cache |
| `DiskPersistence` | `_storagePath`, `_isInitialized` | Pseudo-singleton |
| `CassandraClientFactory` | None, but blocking async | `GetAwaiter().GetResult()` deadlock risk |

**Fix:** Convert to instance classes registered as singletons in DI. Start with `TokenRefreshManager` (highest risk) and `CassandraClientFactory` (most widely used).

---

## B. High: Model Layer Issues

### B.1 Models contain business logic
- `MigrationUnit.UpdateParentJob()` — mutates parent's Tables list
- `MigrationUnit.ToSummary()` — projection logic
- `MigrationSettings.Load()/Save()` — config persistence
- `MigrationJob` — backward-compat bool properties + computed `ConnectionOptions`

**Fix:** Extract `MigrationUnitService` for UpdateParentJob/ToSummary. Move Settings load/save to a `SettingsManager`. Keep models as pure data.

### B.2 Legacy MongoDB naming persists
- `QueryDocCount` / `ResultDocCount` in `Segment` → `QueryRowCount` / `ResultRowCount`
- `[JsonObject("CollectionInfo")]` on `TableMapping` → remove (breaking change for old files)
- `NameValuePair` class appears unused — delete

### B.3 Nullable fields that should be initialized
- `MigrationJob.Tables` → `= new()` (like we did for `CompletedCopyFeedRanges`)
- `JobRegistry.MigrationJobs` / `MigrationJobIds` → `= new()`
- `MigrationUnit.FeedRangeContinuationTokens` → `= new()`

---

## C. Medium: DataTransfer Layer Improvements

### C.1 CopyProgressTracker has too many responsibilities (294 lines, 24 fields)
Currently: row counters + speed calc + logging + MigrationUnit updates + checkpoint saves.

**Fix:** Split into:
- `ProgressCounters` — pure atomic counters (AddCopied, AddFailed, AddRead)
- `ProgressReporter` — logging, speed calc, bottleneck analysis
- Keep `UpdateMigrationUnit()` in tracker (it's the integration point)

### C.2 ReplayWorker still complex (435 lines)
Despite the recent refactor, `PollLoopAsync` still mixes:
- Statement setup + page read
- Row iteration + target writes  
- Stats + continuation save
- Error handling + reconnect

**Fix:** Further extract `ReplayPageProcessor` for the inner read-write-save cycle.

### C.3 Partition mutable state not fully synchronized
`IsExhausted` and `LastPagingState` are public mutable fields accessed from the worker loop without locks, while `_head`/`_tail` are locked.

**Fix:** Make `IsExhausted` and `LastPagingState` only modifiable through methods that hold the lock.

---

## D. Low: Cleanup Items

### D.1 Unused code
- `NameValuePair` class — appears unused, delete
- `MigrationJob` backward-compat bool properties (`IsCompleted`, `IsCancelled`, etc.) — only needed for deserializing old job files. Add `[Obsolete]` and document removal timeline.
- `WorkerPool._ct` field — accepted but never used in the pool itself

### D.2 Missing `IDisposable`
- `BulkCopyRunner` creates `WorkerPool` (disposed) but also creates `CopyProgressTracker` and `PipelineContext` — no cleanup
- `ReplayProcessor` launches `Task.Run` tasks that are never awaited on dispose — potential fire-and-forget leak
- `ChangeFeedManager` holds `ReplayProcessor` but has no `Dispose`

### D.3 Exception handling gaps
- `ReplayProcessor.StartPendingTables` catches exceptions with `Console.Error.WriteLine` — should use `_log`
- `CassandraClientFactory` has `GetAwaiter().GetResult()` — potential deadlock on sync context

---

## Recommended Priority Order

| # | Item | Impact | Effort |
|---|------|--------|--------|
| 1 | A.1 MigrationJobContext → DI singleton | High (testability, concurrency) | Large |
| 2 | B.3 Initialize nullable collections | High (null safety) | Small |
| 3 | B.2 Rename legacy Doc/Collection terms | Medium (clarity) | Small |
| 4 | D.1 Delete unused code | Low (cleanup) | Small |
| 5 | A.2 TokenRefreshManager → instance | High (thread safety) | Medium |
| 6 | C.1 Split CopyProgressTracker | Medium (SRP) | Medium |
| 7 | B.1 Extract logic from models | Medium (SRP) | Medium |
| 8 | C.3 Partition thread safety | Medium (correctness) | Small |
| 9 | D.2 Add missing IDisposable | Medium (resource leaks) | Small |
| 10 | A.2 CassandraClientFactory → instance | High (testability) | Large |
