# Cassandra Migration Tool — Next Tasks

## Priority 1: Data Consistency

Silent error swallowing can cause data loss. The job must fail loudly rather than skip rows.

### 1.1 Audit all silent catch blocks
Review every `catch` in the pipeline and change feed paths. Any catch that swallows an error without failing the job or logging it as a retryable failure is a data consistency risk.

**Key areas to audit:**
- `ReplayWorker.cs` — poll loops catch exceptions and continue; must track per-row failures
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

## Priority 2: Pause/Resume Data Consistency

Pause and resume must guarantee zero data loss and zero duplicate rows.

### 2.1 Graceful drain on pause
- When pause is requested, workers must finish their current page write before stopping
- No partition should be abandoned mid-write — either complete the page or roll back the checkpoint
- Verify: after pause, all in-flight WorkChunks are either completed (checkpoint advanced) or not started (checkpoint unchanged)

### 2.2 Resume correctness
- On resume, verify that checkpoint state matches actual data in target
- Completed feed ranges must not be re-processed (test: pause after 50% ranges, resume, verify no duplicate rows)
- In-progress ranges must resume from exact checkpoint (not from start of range)
- CancellationToken must be recreated on resume — verify `ResetCancellationToken()` is called before all worker paths

### 2.3 Change feed pause/resume
- Change feed continuation tokens must be persisted before acknowledging pause
- On resume, replay must start from last persisted token (not from bulk copy start time)
- Verify no gap between last persisted token and first event after resume

### 2.4 Concurrent pause safety
- Multiple rapid pause/resume clicks must not corrupt state
- Verify `StopProcessing` and `StartProcessAsync` are properly locked
- Test: pause during cancel, resume during pause, cancel during resume

---

## Priority 3: Optimum Resource Utilization

### 3.1 Connection pool tuning
- Profile actual connection usage per worker — are per-worker sessions over-provisioning?
- Consider shared connection pools with semaphore throttling as an alternative to per-worker sessions
- Expose connection pool metrics (active/idle/total) in the progress tracker

### 3.2 Proportional worker distribution across tables
- Currently each table gets `CPU × 13 / parallel_tables` workers equally, regardless of table size
- A 1B-row table and a 1K-row table get the same worker count — wasteful
- Distribute workers proportionally based on feed range count or estimated row count
- Small tables (few feed ranges) should get fewer workers; large tables should get more
- As tables complete, redistribute their workers to remaining tables dynamically

### 3.3 Worker auto-scaling
- Add adaptive worker count: if source is throttling (429s), reduce workers; if throughput is low, increase
- Monitor CPU/memory usage and cap workers if the host is saturated

### 3.4 Memory pressure
- Large pages (5000+ rows) with wide rows can cause GC pressure
- Consider streaming rows instead of buffering entire pages in `List<object[]>`
- Profile memory allocation per worker under heavy load

### 3.5 Target write optimization
- Measure whether `LocalOne` is always optimal or if `Any` would improve throughput
- Batch small rows where total batch size < 50KB (currently all rows written individually)
- Concurrent write limit per worker — currently unbounded `Task.WhenAll` over entire page

---

## Priority 4: Online Copy with FFCF (Full Fidelity Change Feed)

Currently uses regular change feed (`COSMOS_CHANGEFEED_START_TIME()`) which only captures inserts and updates. Deletes are NOT replicated. FFCF provides full fidelity including deletes.

### 4.1 FFCF integration
- Add FFCF mode alongside existing regular change feed
- Parse FFCF JSON payload to detect operation type (INSERT, UPDATE, DELETE)
- Generate appropriate CQL: INSERT for inserts/updates, DELETE for deletes
- Handle FFCF-specific pagination and continuation tokens

### 4.2 Delete replication
- Map FFCF delete events to `DELETE FROM target WHERE pk = ?` statements
- Handle range deletes if supported by FFCF
- Track delete counts separately in progress (inserts vs updates vs deletes)

### 4.3 Schema change handling
- Detect column additions/removals during online replication
- Auto-alter target schema to match source changes
- Pause replication and alert user if incompatible schema change detected

### 4.4 Consistency validation
- After switching to FFCF, provide a "consistency check" mode that compares row counts and checksums between source and target
- Detect drift between bulk copy and change feed replay

---

## Priority 5: Code Quality

### 5.1 Split MigrationJobViewer.razor (1852 lines)
Extract sub-components: `TableListPanel`, `LogViewer`, `JobActionToolbar`, `ProgressSummary`.

### 5.2 Convert MigrationJobContext from static to DI singleton

### 5.3 Add unit tests
Priority: checkpoint correctness, ExceptionClassifier, Partition linked list, TableDiscovery.

### 5.4 Extract interfaces for testability
`ICassandraSessionFactory`, `ICassandraQueries` for mocking.
