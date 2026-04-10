# Cassandra Migration Tool — Next Tasks

## Priority 1: Data Consistency

Silent error swallowing can cause data loss. The job must fail loudly rather than skip rows.

### 1.1 Audit all silent catch blocks
Review every `catch` in the pipeline and change feed paths. Any catch that swallows an error without failing the job or logging it as a retryable failure is a data consistency risk.

**Key areas to audit:**
- `ReplayProcessor.Worker.cs` — poll loops catch exceptions and continue; must track per-row failures
- `PageWriter.cs` — individual row write failures are counted but the page may still be marked as progressed
- `DiskPersistence.cs` — save failures could lose checkpoint state silently

### 1.2 Fail-fast on persistent errors
Currently some transient errors are retried but persistent errors (auth failures, schema mismatches, non-existent tables) should fail the job immediately with a clear error message rather than retrying forever.

### 1.3 Checkpoint integrity
- Verify that no checkpoint advances unless ALL rows in the page are confirmed written
- Ensure resume from checkpoint doesn't skip any rows (test: kill mid-page, resume, verify no gaps)
- Write validation: after bulk copy completes, optionally run a row count comparison (source vs target)

### 1.4 Failed row tracking
- Capture partition keys of persistently failed rows (currently only counted, not recorded)
- Provide a "failed rows" report per table so users can manually fix or retry specific rows
- Consider a dead-letter file per table with the failed row data

---

## Priority 2: Optimum Resource Utilization

### 2.1 Connection pool tuning
- Profile actual connection usage per worker — are per-worker sessions over-provisioning?
- Consider shared connection pools with semaphore throttling as an alternative to per-worker sessions
- Expose connection pool metrics (active/idle/total) in the progress tracker

### 2.2 Worker auto-scaling
- Current formula: `CPU × 13 / parallel_tables` — validate this against actual bottlenecks
- Add adaptive worker count: if source is throttling (429s), reduce workers; if throughput is low, increase
- Monitor CPU/memory usage and cap workers if the host is saturated

### 2.3 Memory pressure
- Large pages (5000+ rows) with wide rows can cause GC pressure
- Consider streaming rows instead of buffering entire pages in `List<object[]>`
- Profile memory allocation per worker under heavy load

### 2.4 Target write optimization
- Measure whether `LocalOne` is always optimal or if `Any` would improve throughput
- Batch small rows where total batch size < 50KB (currently all rows written individually)
- Concurrent write limit per worker — currently unbounded `Task.WhenAll` over entire page

---

## Priority 3: Online Copy with FFCF (Full Fidelity Change Feed)

Currently uses regular change feed (`COSMOS_CHANGEFEED_START_TIME()`) which only captures inserts and updates. Deletes are NOT replicated. FFCF provides full fidelity including deletes.

### 3.1 FFCF integration
- Add FFCF mode alongside existing regular change feed
- Parse FFCF JSON payload to detect operation type (INSERT, UPDATE, DELETE)
- Generate appropriate CQL: INSERT for inserts/updates, DELETE for deletes
- Handle FFCF-specific pagination and continuation tokens

### 3.2 Delete replication
- Map FFCF delete events to `DELETE FROM target WHERE pk = ?` statements
- Handle range deletes if supported by FFCF
- Track delete counts separately in progress (inserts vs updates vs deletes)

### 3.3 Schema change handling
- Detect column additions/removals during online replication
- Auto-alter target schema to match source changes
- Pause replication and alert user if incompatible schema change detected

### 3.4 Consistency validation
- After switching to FFCF, provide a "consistency check" mode that compares row counts and checksums between source and target
- Detect drift between bulk copy and change feed replay

---

## Code Quality (lower priority)

### 4.1 Split ReplayProcessor.Worker.cs (448 lines)
Has 4 poll methods with ~60% shared logic. Extract shared read/write/reconnect logic into a helper.

### 4.2 Split MigrationJobViewer.razor (1852 lines)
Extract sub-components: `TableListPanel`, `LogViewer`, `JobActionToolbar`, `ProgressSummary`.

### 4.3 Eliminate remaining partial classes
- `CassandraClientFactory` (3 files) → individual classes
- `ReplayProcessor` (2 files) → individual classes
- `DiskPersistence` (2 files) → individual classes

### 4.4 Remove dead fields
- `_appId` in `DiskPersistence.cs:18` — assigned but never read
- `_syncBackLock` in `JobManager.cs:28` — declared but never used

### 4.5 Convert MigrationJobContext from static to DI singleton

### 4.6 Add unit tests
Priority: checkpoint correctness, ExceptionClassifier, Partition linked list, TableDiscovery.

### 4.7 Extract interfaces for testability
`ICassandraSessionFactory`, `ICassandraQueries` for mocking.

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
