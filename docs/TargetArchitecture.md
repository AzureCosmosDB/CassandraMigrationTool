# Target architecture: two-tier ownership

This document captures the agreed direction for removing process-wide
ambient state (`MigrationJobContext.Instance`, static `JobStore` /
`UnitStore` / `SettingsManager`) and replacing it with a strict
two-tier ownership model. It is the north star for the singleton-removal
work; individual PRs reference back to this document.

## The model

```
AppHost  (singleton, registered in DI — one per process)
│
├── IDocumentStorage Store               ← infra, no per-job content
├── ILogStorage LogStore                 ← infra
├── string AppId                         ← config
├── AppSettings Config                   ← config
├── JobIndex JobIndex                    ← "which jobs exist on this host"
│
├── ConnectionCredentialCache Credentials
│       ConcurrentDictionary<jobId,
│         (sourceCs, targetCs)>          ← sole cross-job dictionary;
│                                          sole purpose: "Resume with
│                                          Existing Connection Strings"
│
├── JobSupervisor JobSupervisor          ← the one polling object
│       • on startup, scans JobIndex for Status=Running and
│         (a) marks them Pending (crash-recovery) or
│         (b) auto-resumes them (policy-controlled)
│       • serializes UI Start requests (one runner at a time today;
│         architecture trivially supports N later)
│       • on Start: constructs a new JobRunner, holds it as
│         ActiveRunner, awaits its Task
│       • on Runner completion: releases ActiveRunner reference → GC
│
└── JobRunner? ActiveRunner               ← null when idle


JobRunner  (lifetime = one Run of one job — created per Start,
            disposed when run ends)
│
├── Job Job                              ← the document we operate on
├── MigrationLog Log                     ← per-job log
├── JobRunState State                    ← per-job pause flag + event;
│                                          replaces the global flag in
│                                          MigrationJobContext
├── CancellationTokenSource Cts          ← per-job CTS
│
├── JobPipeline Pipeline                 ← worker pool, cooldown, channel
├── PipelineSupervisor Watchdog          ← top-down lifecycle watchdog
├── TableCopyCoordinator[] Coordinators  ← per-table during Phase 2
├── JobPartitioning Partitioning         ← partition map for this job
│
├── TableMigrationCache Units            ← per-job unit cache
│                                          (today: shared
│                                          MigrationUnitsCache)
├── Job? CachedDocument                  ← replaces static
│                                          JobStore._cachedActiveJob
└── TokenRefreshManager Tokens           ← AAD token refresh for this job

       On dispose:
         • cancel CTS, await pipeline drain
         • dispose pipeline (already idempotent post PR #71)
         • dispose log, dispose token manager
         • the whole subtree becomes unreachable → GC handles the rest
         • NO shared dictionaries to scrub
```

## What survives as a singleton

| Component | Why |
|---|---|
| `AppHost` | one process |
| `JobRepository(IDocumentStorage)` | one filesystem; pure I/O wrapper; instance-based, not static, so it is testable |
| `AppSettings` (cached config object) | pure data |
| Pure static helpers (`CassandraClientFactory`, `CqlIdentifier`, `ExceptionClassifier`, `MigrationUtilities`, `MigrationDefaults`, `CassandraQueries`, `SchemaManager`, `RowWriteRetry`, `RowWriteStrategyFactory`, `TableMigrationMapper`, `DataDirectoryResolver`, `AppVersion`) | stateless (or AppId on `DataDirectoryResolver` is set-once at boot); no per-job content |

## What disappears

| Today | Becomes |
|---|---|
| `MigrationJobContext.Instance` static accessor | DI-registered `AppHost` |
| `MigrationJobContext.ActiveMigrationJobId` (string) | `AppHost.ActiveRunner?.Job.Id` |
| `MigrationJobContext.CurrentlyActiveJob` (getter) | `AppHost.ActiveRunner?.Job` |
| `MigrationJobContext.MigrationUnitsCache` (shared dict) | `JobRunner.Units` (per-runner) |
| `MigrationJobContext.ControlledPauseRequested` (process-wide volatile bool) | `JobRunner.State.ControlledPaused` |
| `MigrationJobContext.PauseRequested` (process-wide event) | `JobRunner.State.PauseRequested` (subscribers in the same runner only) |
| `MigrationJobContext.PendingAutoStartJobIds` | `JobSupervisor`'s internal queue (not in shared state at all) |
| `MigrationJobContext.SourceConnectionString` / `TargetConnectionString` | `AppHost.Credentials` (`ConnectionCredentialCache`) |
| `JobStore._jobs` / `_cachedActiveJob` static caches | `JobRepository.LoadJob(id)` returns fresh from disk; runner caches its own doc |
| `JobManager.MigrationJobRunner` field, `_migrationCts`, `_runningJobId`, `_log` | All collapse into `AppHost.ActiveRunner` |
| Static `UnitStore` / `JobStore` / `SettingsManager` | `JobRepository(IDocumentStorage)` instance |

## Migration sequence

Each step is independently mergeable. The order is dependency-driven.

| # | PR | Behaviour change | Risk |
|---|---|---|---|
| 0 | **Extract `ConnectionCredentialCache`** | None — the two `ConcurrentDictionary`s on `MigrationJobContext` move into a dedicated single-responsibility class held by `MigrationJobContext` as a property. Establishes the "one credential cache" the target architecture calls out, and gives the eventual `AppHost` a place to hand the cache to `JobRunner` without touching every other call site. | Very low |
| 1 | **Test scaffold** | Add an `IClusterSession` seam over the driver's `ISession`, a `FakeClusterSession`, and one integration test that runs a 2-table synthetic Job end-to-end through `JobPipeline`. Unblocks safe refactoring on #2-#5. | Low (additive only) |
| 2 | **JobRepository extraction** | Static `JobStore` + `UnitStore` + `SettingsManager` → instance class `JobRepository(IDocumentStorage)`. Static classes become thin forwarders to a singleton instance during transition. ~50 callsites unchanged. | Low if #1 lands first; Medium otherwise |
| 3 | **Introduce `JobRunner` class** | New `JobRunner` owns Pipeline + Supervisor + Coordinators + Log + per-job CTS + per-job pause state. `MigrationJobContext` per-job fields become forwarders into `AppHost.ActiveRunner?.X` (compat shim). | Medium |
| 4 | **Cut the forwarders** | Every caller reads from `AppHost.ActiveRunner` directly. Delete forwarder properties from `MigrationJobContext`. | Medium (mechanical) |
| 5 | **Delete `MigrationJobContext.Instance` static accessor** | DI is now the only path. Rename `MigrationJobContext` → `AppHost`. | Low (deletion) |

## Why this matters

Today the application is structurally single-job (the singletons assume
one active migration; `ActiveMigrationJobId` is a single string slot).
Even features that look multi-job (the homepage list, parallel job
status) are read-only views; only one job can actually be running. With
the target model:

- **Cross-job interference becomes a type error.** Two `JobRunner` instances cannot accidentally share state because each has its own pause flag, unit cache, CTS, pipeline. Today's `MigrationJobContext.ControlledPauseRequested` is a process-wide bool that, if set during one job's run and not reset, leaks into the next.
- **Per-job state cleanup becomes "drop the reference."** Today's `RetireJob` (PR #75) is mandatory because the singletons accumulate per-job entries that have to be manually evicted; in the target, GC handles teardown the moment `AppHost.ActiveRunner` is reassigned.
- **Testability becomes plausible.** `JobRunner` can be constructed with a `FakeClusterSession` and run a synthetic job in unit-test time. Today's static singletons make this impossible without process restart.
- **Multi-job operation later becomes a `Dictionary<jobId, JobRunner>` swap on `AppHost`**, not an architectural rewrite.

## Status

- PR #75 (`fix/retire-job-state`) introduced `RetireJob` as a surrogate for the cleanup the target architecture would give us for free. It is the right behaviour today but is technical debt against this target.
- This PR (Step 0 above) extracts `ConnectionCredentialCache` as a first concrete step. Subsequent PRs reference this document.
