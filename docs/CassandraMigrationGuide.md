# Cassandra Migration Guide

## Azure Cosmos DB Cassandra API → Apache Cassandra / Managed Instance

This guide walks you through migrating your data from
Azure Cosmos DB Cassandra API to an Apache Cassandra
cluster (or Azure Managed Instance for Apache Cassandra)
with **minimal downtime** using the Cassandra Migration
Tool.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Architecture](#architecture)
4. [Step 1 — Deploy the Migration Service](#step-1--deploy-the-migration-service)
5. [Step 2 — Configure the Migration Job](#step-2--configure-the-migration-job)
6. [Step 3 — Run the Migration](#step-3--run-the-migration)
7. [Step 4 — Monitor Progress](#step-4--monitor-progress)
8. [Step 5 — Cutover](#step-5--cutover)
9. [Supported Data Types](#supported-data-types)
10. [Troubleshooting](#troubleshooting)
11. [FAQ](#faq)

---

## Overview

The migration tool performs a **live, online migration** in
three phases:

| Phase | What happens | Downtime |
|-------|-------------|----------|
| **Schema Copy** | Reads source keyspace schema (tables, columns, primary keys, clustering order) and recreates it on the target | None |
| **Bulk Data Copy** | Copies all existing rows from source to target in parallel batches | None |
| **Change Feed Replay** | Continuously reads Cosmos DB change feed and replays inserts and updates to the target | None |

When you are ready to switch your application to the new
cluster, you perform a brief **cutover** (seconds to
minutes) by stopping writes to the source, waiting for
the change feed to drain, then redirecting your
application.

---

## Prerequisites

### Source (Cosmos DB Cassandra API)

- An Azure Cosmos DB account with Cassandra API
- **Authentication**: Either local auth (username/password)
  or Azure AD (AAD) authentication. AAD is recommended
  for production use.

### Target (Apache Cassandra / Managed Instance)

- An Apache Cassandra cluster (3.11+ or 4.x) or Azure
  Managed Instance for Apache Cassandra
- Network connectivity from the migration service to both
  source and target (VNet peering or public endpoints)
- Sufficient throughput to handle the migration write load

### Migration Service Host

- An Azure App Service (Windows, .NET 9.0 runtime) or any
  Windows/Linux host with .NET 9.0
- If using AAD auth: the host must have a managed identity
  with `Cosmos DB Account Reader` role (or higher) on the
  source Cosmos DB account
- Network access to both source and target clusters

---

## Architecture

```
┌──────────────────────┐                ┌────────────────────┐
│  Cosmos DB            │   CQL / TLS   │                    │
│  Cassandra API        │◄──────────────│   Migration        │
│  (Source)             │               │   Service          │
│                       │  Change Feed  │   (App Service)    │
│                       │──────────────►│                    │
└──────────────────────┘                │   Web UI (:443)    │
                                        │                    │
┌──────────────────────┐   CQL          │                    │
│  Target Cassandra     │◄──────────────│                    │
│  (MI / OSS)          │               └────────────────────┘
└──────────────────────┘
```

---

## Step 1 — Deploy the Migration Service

### Option A: Azure App Service (Recommended)

1. **Create an App Service Plan and Web App:**

   ```bash
   az appservice plan create \
     --name migration-plan \
     --resource-group <your-rg> \
     --sku B2

   az webapp create \
     --name <your-app-name> \
     --resource-group <your-rg> \
     --plan migration-plan \
     --runtime "dotnet:9"
   ```

2. **Enable VNet integration** (if the target Cassandra
   cluster is in a private network):

   ```bash
   az webapp vnet-integration add \
     --name <your-app-name> \
     --resource-group <your-rg> \
     --vnet <vnet-name> \
     --subnet <subnet-name>
   ```

3. **Enable managed identity** (for AAD auth to
   Cosmos DB):

   ```bash
   az webapp identity assign \
     --name <your-app-name> \
     --resource-group <your-rg>
   ```

   Then grant the identity access to your Cosmos DB
   account:

   ```bash
   # Get the principal ID from the previous command output
   az cosmosdb sql role assignment create \
     --account-name <cosmos-account> \
     --resource-group <cosmos-rg> \
     --role-definition-name "Cosmos DB Built-in Data Reader" \
     --principal-id <principal-id> \
     --scope "/"
   ```

4. **Deploy the migration tool:**

   ```bash
   az webapp deploy \
     --name <your-app-name> \
     --resource-group <your-rg> \
     --src-path deploy.zip \
     --type zip
   ```

5. **Configure app settings:**

   ```bash
   az webapp config appsettings set \
     --name <your-app-name> \
     --resource-group <your-rg> \
     --settings \
       StateStore__ConnectionStringOrPath="D:\\home"
   ```

### Option B: Run Locally

1. Install [.NET 9.0 Runtime](https://dotnet.microsoft.com/download/dotnet/9.0)
2. Extract the published files to a directory
3. Set the environment variable:
   ```powershell
   $env:StateStore__ConnectionStringOrPath = "C:\MigrationData"
   ```
4. Run:
   ```powershell
   dotnet CassandraMigrationWebApp.dll
   ```
5. Open `http://localhost:5000` in your browser

---

## Step 2 — Configure the Migration Job

1. Open the migration tool in your browser:
   `https://<your-app-name>.azurewebsites.net`

2. Click **"Create New Migration"**

3. Fill in the **Source** (Cosmos DB) connection:

   | Field | Value | Example |
   |-------|-------|---------|
   | Host | Cosmos DB Cassandra endpoint | `myaccount.cassandra.cosmos.azure.com` |
   | Port | `10350` | `10350` |
   | Username | Cosmos DB account name | `myaccount` |
   | Password | Primary key *(leave blank for AAD)* | |
   | Use AAD Auth | ✅ if using managed identity | ✅ |
   | Keyspace | Source keyspace name | `production` |

4. Fill in the **Target** (Cassandra) connection:

   | Field | Value | Example |
   |-------|-------|---------|
   | Host | Target cluster address | `<target-ip>` |
   | Port | CQL native port | `9042` |
   | Username | Cassandra user *(if auth enabled)* | `cassandra` |
   | Password | Cassandra password *(if auth enabled)* | |
   | Keyspace | Target keyspace name | `production` |

   > **Note**: Create the target keyspace before starting
   > migration. The tool creates tables but not keyspaces:
   > ```cql
   > CREATE KEYSPACE production WITH replication = {
   >   'class': 'NetworkTopologyStrategy',
   >   'datacenter1': 3
   > };
   > ```

5. Click **"Start Migration"**

---

## Step 3 — Run the Migration

The migration executes automatically in this order:

### Phase 1: Schema Discovery & Creation
- Reads all table schemas from the source keyspace
  (columns, types, primary keys, clustering order,
  static columns)
- Creates matching tables on the target
- Handles compound partition keys and composite
  clustering keys

### Phase 2: Bulk Data Copy
- Reads all rows from each source table using paged
  queries
- Writes to the target in parallel batches
- Progress is tracked per table with row counts
- Automatically handles all data types including
  collections, blobs, and UUIDs

### Phase 3: Change Feed Replay
- Starts polling the Cosmos DB change feed
- Replays changes to the target:
  - **Inserts** → INSERT on target
  - **Updates** → INSERT (upsert) on target
- Continues running until you trigger cutover

---

## Step 4 — Monitor Progress

The web UI shows real-time status for each table:

| Status | Meaning |
|--------|---------|
| **Pending** | Queued, waiting to start |
| **SchemaCreated** | Target table created successfully |
| **Copying** | Bulk data copy in progress |
| **CopyComplete** | All existing rows copied |
| **ChangeFeedActive** | Replaying live changes |
| **Complete** | Migration finished (after cutover) |
| **Failed** | Error occurred — check logs for details |

### What to Watch

- **Rows Copied** — should match source row count
  after bulk copy completes
- **Change Feed Events** — increments as live changes
  are replayed
- **Lag** — should decrease toward 0 as the change
  feed catches up

---

## Step 5 — Cutover

When you're ready to switch to the target cluster:

1. **Stop application writes** to the source Cosmos DB
   account (pause traffic or set app to read-only mode)

2. **Wait for change feed to drain** — watch the UI until
   change feed events stop incrementing (typically
   seconds after writes stop)

3. **Verify data on the target:**
   - Compare row counts between source and target
   - Spot-check important records
   - Run application smoke tests against the target

4. **Update your application connection strings** to
   point to the target Cassandra cluster

5. **Resume application traffic**

6. **Mark migration as complete** in the tool UI

### Expected Cutover Downtime

| Scenario | Typical Downtime |
|----------|-----------------|
| Low-traffic app with change feed | **< 30 seconds** |
| High-traffic app with change feed | **1–5 minutes** |

---

## Supported Data Types

| CQL Type | Supported | Notes |
|----------|-----------|-------|
| `text`, `ascii`, `varchar` | ✅ | |
| `int` | ✅ | |
| `bigint` | ✅ | |
| `smallint` | ✅ | |
| `tinyint` | ✅ | |
| `float` | ✅ | |
| `double` | ✅ | |
| `decimal` | ✅ | Full precision preserved |
| `boolean` | ✅ | |
| `uuid` | ✅ | |
| `timeuuid` | ✅ | |
| `timestamp` | ✅ | |
| `date` | ✅ | |
| `blob` | ✅ | Handles both hex and Base64 |
| `inet` | ✅ | IPv4 and IPv6 |
| `varint` | ✅ | Arbitrary precision |
| `counter` | ✅ | |
| `set<T>` | ✅ | All element types |
| `list<T>` | ✅ | All element types |
| `map<K,V>` | ✅ | All key/value types |
| `frozen<T>` | ✅ | Frozen collections |

### Schema Features

| Feature | Supported |
|---------|-----------|
| Simple partition key | ✅ |
| Compound partition key | ✅ |
| Composite clustering key | ✅ |
| Clustering order (ASC/DESC) | ✅ |
| Static columns | ✅ |
| Empty tables | ✅ |
| Primary-key-only tables | ✅ |

---

## Troubleshooting

### Authentication Errors

**"Invalid Cosmos DB account or key"**
- **AAD auth**: Ensure the App Service managed identity
  has the correct RBAC role on the Cosmos DB account.
  The identity needs at minimum `Cosmos DB Built-in
  Data Reader` role.
- **Password auth**: Verify you're using the Primary Key
  from Azure Portal → Cosmos DB → Connection String.
- AAD tokens are automatically refreshed before expiry.
  Check App Service logs if auth errors persist.

### Network Errors

**"Connection refused" or timeout to target**
- Verify VNet integration is configured if target is in
  a private network
- Check NSG (Network Security Group) rules allow
  outbound traffic on port 9042
- Test connectivity: in App Service Kudu console, try
  `tcpping <target-ip>:9042`

**"All hosts tried for query failed"**
- The Cosmos DB endpoint may be unreachable. Check
  firewall rules on the Cosmos DB account allow the
  App Service outbound IPs.

### Data Errors

**"Keyspace does not exist" on target**
- Create the target keyspace manually before starting.
  The tool creates tables but not keyspaces.

**Table shows "Failed" status**
- Check detailed logs in the migration UI
- Common causes: schema incompatibility, insufficient
  throughput (429 throttling), network timeout
- You can **resume** a failed migration — it picks up
  where it left off

### Performance

**Migration is slow**
- Increase source Cosmos DB throughput (RU/s)
  temporarily
- Use a larger App Service plan (B2 or P1v2)
- Check for throttling (429 errors) in logs

---

## FAQ

**Q: Can I migrate multiple keyspaces?**
A: Yes. Create one migration job per keyspace. Multiple
jobs can run in parallel.

**Q: What happens if the migration service restarts?**
A: The tool persists state to disk. On restart, it
automatically resumes all active jobs. AAD tokens are
re-acquired automatically. If using password auth, you
need to re-enter the source password via the UI (passwords
are never stored on disk for security).

**Q: How much throughput (RU/s) should I provision?**
A: Recommended RU/s on the source during migration:

| Data Size | Recommended RU/s |
|-----------|-----------------|
| < 1 GB | 10,000 |
| 1–10 GB | 50,000 |
| 10–100 GB | 100,000+ |

You can scale back down after migration completes.

**Q: Does the tool handle schema changes during migration?**
A: No. Do not alter table schemas during migration.
Complete the migration first, then modify schemas on the
target.

**Q: Can I migrate to any CQL-compatible database?**
A: Yes. The tool uses standard CQL INSERT/DELETE
statements. It has been tested with:
- Apache Cassandra 3.11 and 4.x
- Azure Managed Instance for Apache Cassandra
- Any standard CQL-compatible cluster

**Q: Are passwords stored securely?**
A: Source and target passwords are held in memory only
during the active migration session. They are never
written to disk or persisted in job state files. When
using AAD authentication, no passwords are needed at all.

---

*Document version: 1.2 — March 2026*
*Updated: Uses standard Cosmos DB Cassandra change feed
(`COSMOS_CHANGEFEED_START_TIME()`).*
