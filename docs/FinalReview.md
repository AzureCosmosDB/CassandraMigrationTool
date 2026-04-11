# Final Review — Senior Architect + C# Engineer

## Architecture Assessment

### What's Good
- **Clean dependency hierarchy**: Models → Infrastructure → Persistence → Context → CassandraDriver → DataTransfer → Workers (verified, no circular deps)
- **Pipeline pattern**: BulkCopyEngine → BulkCopyRunner (4-stage pipeline) → BulkCopyWorker is well-structured
- **Single responsibility**: each class has a clear purpose, no partial classes, records used appropriately
- **Separation of bulk copy and change feed**: independent sub-folders, zero cross-references

### Top 3 Architectural Risks

| # | Risk | Impact | Location |
|---|------|--------|----------|
| 1 | **MigrationJobContext is static global state** | Untestable, thread-unsafe, blocks multi-job | `Context/MigrationJobContext.cs` |
| 2 | **Disk write amplification from frequent checkpoints** | I/O bottleneck under high throughput | `JobStore.SaveJob`, `UnitStore.SaveUnit` |
| 3 | **ReplayProcessor fire-and-forget task lifecycle** | Tasks not awaited on shutdown, session leaks | `ChangeFeed/ReplayProcessor.cs` |

---

## C# Code Quality Findings

### High Severity

#### 1. Sync-over-async (deadlock risk)
- `CassandraClientFactory.cs:370-397` — `.GetAwaiter().GetResult()` 
- `MigrationJobContext.cs:234-263` — `Task.Delay(...).Wait()`

**Fix:** Make call chain fully async.

#### 2. ISession not disposed in ReplayProcessor
- `ReplayProcessor` owns a source `ISession` but `Dispose()` doesn't dispose it
- `ChangeFeedManager` creates `ReplayProcessor` but never disposes it on shutdown

**Fix:** Dispose owned session in `ReplayProcessor.Dispose()`. Have `ChangeFeedManager.Dispose()` dispose the processor.

### Medium Severity

#### 3. SemaphoreSlim never disposed
- `ChangeFeedManager._lock` (SemaphoreSlim) — never disposed
- `ReplayWorker` creates a SemaphoreSlim per parallel call — never disposed

**Fix:** Dispose in class Dispose() or use `using var`.

#### 4. HTTP objects not disposed in ArmCredentialDiscovery
- `HttpRequestMessage`/`HttpResponseMessage` created without `using`

**Fix:** Wrap in `using`/`await using`.

#### 5. Unsynchronized static cache in JobStore/MigrationJobContext
- `ActiveMigrationJobId`, `CachedActiveJob`, `MigrationUnitsCache` read/written without locks

**Fix:** Guard with lock or use immutable/atomic patterns.

#### 6. ContinueWith in hot path (PageWriter)
- Per-row `ContinueWith` with closure capture — poor exception semantics, GC pressure

**Fix:** Use direct `await` with throttling (e.g., `SemaphoreSlim` or `Parallel.ForEachAsync`).

#### 7. CQL injection risk
- `PageReader.cs:114-116` — `$"SELECT * FROM \"{keyspace}\".\"{table}\""` — identifier injection
- `ReplayWorker.cs:344-350` — same pattern

**Fix:** Validate identifiers (alphanumeric + underscore only) before interpolation.

### Low Severity

#### 8. Catch-and-swallow in LogPersistence
- Multiple `catch {}` blocks hide corruption/parse bugs

**Fix:** Log unexpected exceptions, only swallow expected truncation.

#### 9. C# modernization opportunities
- File-scoped namespaces would reduce indentation across all files
- Pattern matching and switch expressions in ExceptionClassifier
- `target-typed new` already used in most places

---

## Recommended Action Plan

### Do Now (high impact, manageable effort)
1. Fix sync-over-async in CassandraClientFactory and MigrationJobContext
2. Fix ISession/SemaphoreSlim disposal chain (ReplayProcessor → ChangeFeedManager)
3. Add `using` to HTTP objects in ArmCredentialDiscovery
4. Add CQL identifier validation helper

### Do Next (medium impact)
5. Convert MigrationJobContext from static to DI singleton
6. Add throttled PageWriter (replace ContinueWith with await-based batching)
7. Synchronize JobStore/Context cache access

### Do Later (cleanup)
8. Adopt file-scoped namespaces across all files
9. Improve LogPersistence error handling
10. Add unit tests for pipeline correctness
