# Cassandra Migration Tool

A .NET 9 Blazor Server web app that migrates data from **Azure Cosmos DB for Apache Cassandra** to **Apache Cassandra** — including Azure Managed Instance for Apache Cassandra (MI) and self-hosted OSS clusters. The tool handles schema discovery, DDL generation, feed-range-partitioned bulk copy, and optional live sync via the Cosmos DB change feed, all driven from a browser-based UI.

## Features

- **Schema auto-sync** — discovers source keyspaces, tables, columns, clustering keys, and static columns; generates DDL on the target automatically.
- **Feed-range partitioned bulk copy** — splits each table into token-range chunks and copies them with configurable parallelism.
- **Efficient Cassandra sessions** — workers share the thread-safe source session while retaining independent target sessions for write throughput.
- **Checkpoint-based pause / resume** — stop at any time and continue later with zero data loss; state is persisted to disk.
- **Online mode (change feed)** — after the initial bulk copy, replays Cosmos DB change feed events to keep the target in sync until cutover.
- **Wildcard table selection** — migrate all tables in a keyspace with `keyspace.*` or pick individual tables.
- **AAD / Entra ID authentication** — passwordless auth to Cosmos DB via Managed Identity.
- **Simulation mode** — dry-run that validates connectivity and schema without writing data.
- **Real-time progress UI** — monitor rows copied, throughput, and errors per table from the dashboard.

## Architecture

```
Source                              Target
Cosmos DB Cassandra API             Apache Cassandra / MI
(Port 10350, SSL)                   (Port 9042)
        \                           /
         \   +------------------+  /
          +->| Migration Tool   |--+
              | (.NET 9)         |
              |                  |
              | 1. Schema Copy   |
              | 2. Bulk Data     |
              | 3. Change Feed   |
              +------------------+
```

### Folder structure

```
CassandraMigrationTool/
├── CassandraMigrationWebApp/     # Blazor Server UI, controllers, pages
├── CassandraMigrationProcessor/  # Core migration engine
│   ├── CassandraDriver/          # Session factory, schema manager, token refresh
│   ├── DataTransfer/             # BulkCopy + ChangeFeed workers
│   ├── Models/                   # Job, AppSettings, TableMapping, enums
│   ├── Infrastructure/           # Retry, logging, table discovery
│   ├── Context/                  # Job store, settings manager
│   └── Persistence/              # Disk-based state & log storage
├── docs/                         # Setup guide
├── Dockerfile.aca                # Container image for Azure Container Apps
└── CassandraMigration.sln
```

## Quick Start

1. **Deploy** the web app to Azure App Service (or run locally with `dotnet run`).
2. **Open** the app in a browser and set a local UI password on first launch.
3. **Create a job** — enter source/target connection details, select tables to migrate.
4. **Start the migration** — monitor progress from the dashboard.

> **Full walkthrough:** [docs/CassandraMigrationSetupGuide.md](docs/CassandraMigrationSetupGuide.md)

## Deployment Options

| Option | Notes |
|--------|-------|
| **Azure App Service** (recommended) | Windows, B2+ plan, .NET 9 runtime. VNet-integrate for MI targets. |
| **Azure Container Apps** | Use the provided `Dockerfile.aca`. |
| **On-premises / IIS** | Install the .NET 9 Hosting Bundle; point IIS to the publish output. |
| **Local** | `dotnet run --project CassandraMigrationWebApp` for dev/testing. |

## Configuration

### Application settings (`appsettings.json` or App Service env vars)

| Setting | Default | Description |
|---------|---------|-------------|
| `StateStore:ConnectionStringOrPath` | *(empty)* | Path for local state persistence (e.g. `C:\MigrationDrive`) |
| `StateStore:UseLocalDisk` | `true` | Use local disk for job/checkpoint state |

### Per-job settings (configured in the UI)

| Setting | Default | Description |
|---------|---------|-------------|
| `ParallelThreads` | 5 | Number of parallel copy threads |
| `MaxFeedRangeParallelism` | auto (CPU × 2) | Max concurrent feed-range workers per table |
| `PageSize` | 500 | Rows per page when reading from source |
| `MaxConnectionsPerHost` | 1 per worker | Cassandra driver connections per host |
| `CDCMode` | Offline | `Offline` or `Online` (change feed) |
| `ChangeFeedPollIntervalMs` | 5000 | Polling interval for change feed (ms) |
| `IsSimulatedRun` | false | Dry run — validate without writing |
| `AppendMode` | false | Append to existing target data instead of failing on duplicates |
| `DropTargetTableBeforeStart` | false | Drop and recreate target tables from source schema |
| `UseJsonCopy` | true | **Data copy mode** for regular (non-counter) tables. `true` = preserve TTL & writetime (`SELECT JSON *` → `INSERT … JSON ? USING TIMESTAMP ? AND TTL ?`). `false` = fast binary copy (`SELECT *` → typed `INSERT`), materially faster but **does not preserve TTL or writetime** and is incompatible with per-cell preservation. Counter tables always use the typed UPDATE path. |
| `PreserveCellTtlAndWritetime` | false | Preserve *per-cell* (per-column) writetime/TTL instead of a single row-level value; divergent rows are split into one partial `INSERT … JSON DEFAULT UNSET USING TIMESTAMP … AND TTL …` per distinct (writetime, TTL) group. Requires `UseJsonCopy = true`. |

> **Choosing a copy mode:** the default JSON path preserves TTL and writetime end-to-end but does more per-row server work. If your workload does not rely on TTL or on source writetime (LWW) ordering, the **fast binary copy** mode (`UseJsonCopy = false`) is significantly faster on large datasets. It cannot preserve TTL/writetime, so use it only when those semantics don't matter for the target.

## Migration Modes

### Offline

One-time bulk copy. The tool reads every row from each source table (split by token range), writes it to the target, and marks the job complete.

### Online

Bulk copy followed by continuous change-feed replay. After the initial copy finishes, the tool tails the Cosmos DB Cassandra API change feed and applies inserts/updates to the target in near-real-time. Stop the change feed when you are ready to cut over.

## Tables Input Format

Specify tables in the job creation form as a comma-separated list. Wildcards are supported:

```
keyspace1.*
keyspace1.table_a, keyspace1.table_b
ks1.*, ks2.orders, ks2.customers
```

Each entry is resolved to a `TableMapping`:

```json
{
  "KeyspaceName": "source_ks",
  "TableName": "users",
  "TargetKeyspaceName": "target_ks",
  "TargetTableName": "users"
}
```

When the target keyspace/table names are omitted, the source names are reused.

## Security

- **Passwords are never persisted to disk.** Source AAD tokens are fetched at runtime via `Azure.Identity`; target passwords must be re-supplied on resume.
- **AAD / Entra ID** — enable System-Assigned Managed Identity on the App Service and grant `Cosmos DB Built-in Data Reader` (offline) or `Cosmos DB Built-in Data Contributor` (online) on the Cosmos DB account.
- **UI password** — the tool requires a local password to access the dashboard; this is stored encrypted on disk.
- **Optional App Service authentication** — layer Azure AD / EasyAuth in front of the App Service for additional protection.

## Supported Migration Paths

| Source | Target |
|--------|--------|
| Cosmos DB Cassandra API | Azure Managed Instance for Apache Cassandra |
| Cosmos DB Cassandra API | Self-hosted Apache Cassandra (OSS) |
| Cosmos DB Cassandra API | Any CQL-compatible cluster |

## Prerequisites

- [.NET 9 SDK](https://dotnet.microsoft.com/download/dotnet/9.0) (build) or .NET 9 runtime / Hosting Bundle (deploy)
- Network access from the migration host to the source (port **10350**, SSL) and target (port **9042**)
- For MI targets: the host must be VNet-integrated into the same or a peered VNet
- For AAD auth: System-Assigned Managed Identity with the appropriate Cosmos DB RBAC role

## Building & Running Locally

```bash
# Restore & build
dotnet build CassandraMigration.sln

# Run the web app
dotnet run --project CassandraMigrationWebApp
```

The app starts at `https://localhost:5001` (or the port shown in console output).

## License

[MIT](LICENSE.md) — Copyright (c) Microsoft Corporation.
