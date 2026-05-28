# Cassandra Migration Tool — Black-box Test Plan

**Tester role**: senior QA, customer perspective, no source-code reading
during test-case design (only during triage of confirmed defects).

**Bug repo**: <https://github.com/AzureCosmosDB/CassandraMigrationToolInternal>

## Test environments

| Slot | Web app | Source | Target |
|---|---|---|---|
| ENV-A (Linux, primary) | <https://cassandra-migration-webapp.azurewebsites.net> | Cassandra MI `niteshcassandrami-sea` (seasia) | Cassandra MI `cassandra-mi-demo-sea` (seasia) |
| ENV-B (Windows) | <https://cassandra-migration-win.azurewebsites.net> | same | same |
| ENV-C (eastasia latest) | <https://cassandra-migration-tool-nitesh.azurewebsites.net> | Cassandra MI `niteshcassandrami` | Cassandra MI `niteshcassandramitarget` |
| ENV-D (Cosmos Cassandra source) | ENV-A | Cosmos Cassandra `cassandra-demo-nitesh` | `cassandra-mi-demo-sea` |

Browser: Playwright Chromium via `playwright-browser_*` MCP tools.

## Test areas & ID ranges

| Range | Area |
|---|---|
| TC-001..009 | Smoke / availability |
| TC-010..049 | Data-type coverage (CQL types) |
| TC-050..079 | Schema-shape coverage (keys / indexes / MVs / TTL) |
| TC-080..119 | Add-job UI inputs (positive) |
| TC-120..149 | Add-job UI inputs (negative & security) |
| TC-150..189 | Job-lifecycle controls |
| TC-190..219 | Migration-mode × tool matrix (3×2) |
| TC-220..259 | Edge cases / failure injection |
| TC-260..279 | Schema-migration tool |
| TC-280..299 | Web app settings & RU-optimized path |

Detailed cases live in `CASES.md` (generated next).

## Definition of done per case

* Pass = expected behaviour matches README/UI, no console errors, no
  data-loss/parity violation.
* Fail = file GitHub issue in internal repo, save screenshot under
  `evidence/TC-NNN/`, set `test_cases.status='failed'`.
* Block = environment broken; file infra issue, set `status='blocked'`.

## Out of scope (this pass)

* Performance / scale tuning beyond TB scale.
* On-prem Docker deployment (ACA + Web App only).
* Bicep/IaC validation (treat infra as given).
