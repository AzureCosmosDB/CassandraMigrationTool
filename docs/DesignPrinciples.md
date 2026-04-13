# Design Principles Assessment

## Scorecard

| Principle | Verdict | Key Issue |
|-----------|---------|-----------|
| **S** Single Responsibility | ⚠️ Partial | BulkCopyEngine, MigrationJobContext, CopyProgressTracker do too much |
| **O** Open/Closed | ❌ Fail | ExceptionClassifier, pipeline stages, storage backend all hardcoded |
| **L** Liskov Substitution | ✅ Pass | No problematic inheritance |
| **I** Interface Segregation | ❌ Fail | IPersistenceStorage is fat (CRUD + logs + pagination) |
| **D** Dependency Inversion | ❌ Fail | Concrete dependencies everywhere, no DI for pipeline classes |
| **DRY** | ⚠️ Partial | Session creation, stop/pause flow duplicated |
| **YAGNI** | ✅ Pass | Minimal over-engineering |
| **Law of Demeter** | ❌ Fail | Deep chains: ctx.Worker.Context.KeyspaceName, ctx.Ranges.Checkpoints |
| **Fail Fast** | ⚠️ Partial | Some early validation, but init errors swallowed |
| **Separation of Concerns** | ❌ Fail | Orchestration mixed with logging, persistence, retries |

## What's Already Good
- Clean dependency hierarchy (Models → Infra → Persist → Context → Driver → DataTransfer → Workers)
- Pipeline pattern (Engine → Runner → Worker) with typed stage results
- Records for immutable data carriers (PipelineContext, WorkerConfig, PipelineConfig, etc.)
- No inheritance abuse (Liskov pass)
- Zero force-unwraps, file-scoped namespaces, proper CancellationToken ownership

## Remaining Improvements (prioritized)

### 1. Law of Demeter — flatten PipelineContext access
**Current:** `ctx.Worker.Context.KeyspaceName`, `ctx.Ranges.Checkpoints`
**Fix:** Add convenience properties on PipelineContext:
```csharp
record PipelineContext(...) {
    public string KeyspaceName => Worker.Context.KeyspaceName;
    public string TableName => Worker.Context.TableName;
}
```

### 2. Interface Segregation — split IPersistenceStorage
**Current:** One fat interface with CRUD + logs + pagination
**Fix:** Split into `IDocumentStorage` (Read/Write/Delete) + `ILogStorage` (PushLog/ReadLogs/etc.)

### 3. Dependency Inversion — inject abstractions
**Current:** BulkCopyEngine creates ChangeFeedManager, BulkCopyRunner directly
**Fix:** Accept interfaces or factory delegates in constructor. Start with extracting `ICassandraSessionFactory`.

### 4. Open/Closed — ExceptionClassifier
**Current:** Hardcoded type checks
**Fix:** Make it configurable (register exception types) or use a chain-of-responsibility pattern.

### 5. MigrationJobContext static → DI singleton
The single biggest remaining architectural debt. Blocks testability and multi-job support.
