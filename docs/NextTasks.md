# Cassandra Migration Tool — Next Tasks

## Code Quality

### 1. Split ReplayProcessor.Worker.cs (448 lines)
Has 4 poll methods with ~60% shared logic:
- `PollLoopAsync` — entry point, dispatches to parallel or single
- `PollLoopParallelAsync` — parallel feed range polling
- `PollRangeLoopAsync` — per-range poll loop
- `PollLoopSingleAsync` — single-range poll loop (near-duplicate of PollRangeLoopAsync)

**Action:** Extract shared read/write/reconnect logic into a helper, or unify Single and Range loops with a strategy parameter. Split into `ReplayProcessor.cs` (core) + `ReplayWorker.cs` (poll loop).

### 2. Split MigrationJobViewer.razor (1852 lines)
Largest file in the codebase. Mixes table list, log viewer, action toolbar, progress display.

**Action:** Extract sub-components: `TableListPanel`, `LogViewer`, `JobActionToolbar`, `ProgressSummary`.

### 3. Eliminate remaining partial classes
Still 3 sets of partials:
- `CassandraClientFactory` (3 files: main + ArmDiscovery + TokenRefresh)
- `ReplayProcessor` (2 files: main + Worker)
- `DiskPersistence` (2 files: main + Logs)

**Action:** Follow the same pattern used for BulkCopyEngine — extract into individual classes with constructor injection.

### 4. Remove dead fields
- `_appId` in `DiskPersistence.cs:18` — assigned but never read
- `_syncBackLock` in `JobManager.cs:28` — declared but never used

### 5. MigrationJobContext is still fully static (285 lines)
Has 31 static members. Should evolve toward instance-based DI — `MigrationContextService` already wraps it but the underlying class is static.

**Action:** Convert to a singleton registered in DI. Replace `MigrationJobContext.X` calls with injected instance.

---

## Architecture

### 6. Add unit tests
No tests exist. Priority test targets:
- `BulkCopyWorker` — checkpoint correctness (WorkChunk linked list, resume token)
- `ExceptionClassifier` — concrete type classification
- `Partition.AddChunkAndTrim` / `GetResumeToken` — linked list edge cases
- `TableDiscovery` — wildcard expansion, validation

### 7. Add interface for CassandraClientFactory
Currently a static class. Extract `ICassandraSessionFactory` interface for testability.

### 8. Add interface for CassandraQueries
Currently a static class. Extract `ICassandraQueries` for mocking in tests.

---

## Features

### 9. Validation improvements
- Pre-migration connectivity check (source + target reachable before starting)
- Schema compatibility check (column type mismatches between source and target)

### 10. Observability
- Structured logging (JSON format option for Azure Monitor / Log Analytics)
- Metrics export (Prometheus/OpenTelemetry for worker counts, throughput, error rates)

### 11. Error recovery
- Dead letter tracking for persistently failed rows (currently only counted, not captured)
- Per-row error log with partition key for manual retry

---

## File Size Reference (top 10)

| File | Lines | Notes |
|------|-------|-------|
| MigrationJobViewer.razor | 1852 | Split candidate |
| MigrationDetails.razor | 505 | |
| CassandraClientFactory.cs | 477 | Partial — split candidate |
| ReplayProcessor.Worker.cs | 448 | Partial — split candidate |
| DiskPersistence.Logs.cs | 436 | Partial — split candidate |
| JobManager.cs | 431 | |
| Index.razor | 420 | |
| SchemaManager.cs | 355 | |
| DiskPersistence.cs | 330 | Partial — split candidate |
| MigrationWorker.cs | 328 | |
