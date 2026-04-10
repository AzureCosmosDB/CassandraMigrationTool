# Method-Level Audit — Findings & Action Items

## 1. Duplicated Code

### 1.1 Sync/Async wrapper pairs (CassandraDriver)
`CassandraQueries` and `SchemaManager` have sync wrappers that just call `.GetAwaiter().GetResult()`:
- `ListKeyspaces` / `ListKeyspacesAsync`
- `ListTables` / `ListTablesAsync`
- `GetRowCount` / `GetRowCountAsync`
- `TruncateTable` / `TruncateTableAsync`
- `GetFeedRanges` / `GetFeedRangesAsync`
- `PrepareInsert` / `PrepareInsertAsync`
- `EnsureKeyspaceExists` / `EnsureKeyspaceExistsAsync`
- `TableExists` / `TableExistsAsync`
- `GetTableColumns` / `GetTableColumnsAsync`
- `CreateTableFromSource` / `CreateTableFromSourceAsync`

**Action:** Delete sync wrappers. Convert all callers to use async versions. Sync callers (ReplayWorker.PrepareReplay, ReplayWorker.TryReconnectSource) should be made async.

### 1.2 Row-write logic duplicated
- `PageWriter.WriteAsync()` — parallel row writes for bulk copy
- `ReplayWorker.ReplayRows()` — sequential row writes for change feed

Both do: bind values → execute insert → count success/error. Differ in parallelism.

**Action:** Extract shared `RowWriter` class or make `PageWriter` reusable for both modes.

### 1.3 SafeExecute duplicated
- `DiskPersistence.SafeExecute<T>()` / `SafeExecuteVoid()`
- `LogPersistence.SafeExecute<T>()` / `SafeExecuteVoid()`
- `MigrationUtilities.SafeExecute<T>()`

Three identical try-catch-fallback patterns.

**Action:** Use `MigrationUtilities.SafeExecute` everywhere, delete the duplicates.

### 1.4 UpdateMigrationStats duplicated
- `CopyProgressTracker.UpdateMigrationUnit()` — called per-page from workers
- `BulkCopyRunner.UpdateMigrationStats()` — called once at finalization

Both write tracker totals to chunk/MU fields.

**Action:** Remove `UpdateMigrationStats` from Runner. `Finalize` should just call `tracker.UpdateMigrationUnit()` one last time.

### 1.5 Lazy ReplayProcessor init duplicated
- `ChangeFeedManager.AddTable()` — lock + create if null
- `ChangeFeedManager.StartAll()` — same pattern

**Action:** Extract `EnsureReplayProcessor()` private method.

### 1.6 Token acquisition duplicated
- `TokenRefreshManager.AcquireAadToken()` — one-shot, no expiry tracking
- `TokenRefreshManager.GetFreshAadToken()` — with expiry tracking

**Action:** Make `AcquireAadToken` call `GetFreshAadToken` internally.

## 2. Misnamed Methods

| Method | Issue | Suggested Name |
|--------|-------|----------------|
| `LogPersistence.DownloadLogsAsJsonBytes` | Returns pipe-delimited text, not JSON | `ExportLogsAsBytes` |
| `CopyProgressTracker.RangeCompleted(range, result)` | Ignores both params, just increments counter | `IncrementCompletedRanges()` |
| `UnitStore.GetUnit(key, jobId)` | `key` is vague | `GetUnit(unitId, jobId)` |
| `MigrationUnitCache.GetMigrationUnit(id, JobId)` | PascalCase `JobId` param | `GetMigrationUnit(id, jobId)` |
| `MigrationJobContext.GetMigrationUnit` | Thin facade, name hides delegation | Fine but should be removed (caller should use UnitStore directly) |

## 3. Unused / Redundant Parameters

| Method | Param | Issue |
|--------|-------|-------|
| `WorkerPool(log, workerCount, ct)` | `ct` | Never used in pool |
| `RangeCompleted(range, result)` | `range`, `result` | Both ignored |
| `TableDiscovery.ValidateNamespaceFormat(input, jobType)` | `jobType` | Never read |
| `DiskPersistence.Initialize(path, appId)` | `appId` | Stored but never read |
| `DetermineOutcome(ctx, failedCount)` | `ctx` | Only `ctx.Counters` used |

## 4. Methods Doing Too Much

### Critical (should split)
- `BulkCopyEngine.StartProcessAsync` — loads MU, builds context, chunk loop, retry, pause, finalize, change feed
- `MigrationWorker.ProcessMigrationUnitAsync` — session, schema, logging, copy, persist, errors
- `MigrationJobContext.Initialize` — storage, registry, cache, globals

### High (consider splitting)
- `PageWriter.WriteAsync` — parallel writes + metrics + chunk completion + byte counting
- `PageReader.ReadAsync` — retry + read + map rows + update partition + update tracker
- `ReplayWorker.PollLoopAsync` — read + write + stats + reconnect + delay

## 5. Over-broad `PipelineContext` parameter

These methods receive full `PipelineContext` but only use specific fields:

| Method | Actually needs |
|--------|---------------|
| `TakeNextPartitionAsync(ctx)` | `ctx.PartitionPool` only |
| `SaveCheckpoint(partition, ctx)` | `ctx.Ranges.Checkpoints` only |
| `MarkCompleted(partition, ctx)` | `ctx.Ranges` + `ctx.Tracker` + `ctx.PartitionPool` |
| `DetermineOutcome(ctx, failed)` | `ctx.Counters` only |
| `ReadAsync(partition, ctx)` | `ctx.Worker.Context` + `ctx.Tracker` + `ctx.Counters` |

**Action:** For now, document as technical debt. Narrowing params would improve testability but would change many signatures.

## 6. MigrationJobContext Facade Methods (7 thin wrappers)

These just forward to `JobStore` / `UnitStore`:
```
GetMigrationJob → JobStore.GetJob
PopulateMigrationJobs → JobStore.GetAllJobs
SaveMigrationJob → JobStore.SaveJob
ClearCurrentlyActiveJobCache → JobStore.ClearCache
SaveMigrationUnit → UnitStore.SaveUnit
GetMigrationUnit → UnitStore.GetUnit
GetMigrationUnitFromStorage → UnitStore.GetFromStorage
```

**Action:** When converting MigrationJobContext to DI (A.1 from architect review), remove these facades and inject `JobStore`/`UnitStore` directly.
