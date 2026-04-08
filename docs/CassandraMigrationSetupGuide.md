# Cosmos DB Cassandra API → Apache Cassandra Migration Tool

## Setup & Operations Guide

**Version:** 1.0
**Last Updated:** March 2026

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Prerequisites](#3-prerequisites)
4. [Deployment](#4-deployment)
5. [Configuration](#5-configuration)
6. [Running a Migration](#6-running-a-migration)
7. [Online Migration with Change Feed](#7-online-migration-with-change-feed)
8. [Monitoring & Management](#8-monitoring--management)
9. [Data Verification](#9-data-verification)
10. [Troubleshooting](#10-troubleshooting)
11. [FAQ](#11-faq)

---

## 1. Overview

This tool migrates data from **Azure Cosmos DB Cassandra API**
to **Apache Cassandra** (including Azure Managed Instance for
Apache Cassandra). It provides:

| Feature | Description |
|---------|-------------|
| **Offline Migration** | One-time bulk copy of all rows |
| **Online Migration** | Bulk copy + live sync via Full Fidelity Change Feed (FFCF) |
| **Schema Discovery** | Auto-discovers keyspaces, tables, columns, clustering keys |
| **DDL Generation** | Creates target tables with correct schema (static columns, clustering order, etc.) |
| **Wildcard Selection** | Migrate all tables in a keyspace with `keyspace.*` |
| **AAD / Managed Identity** | Passwordless auth to Cosmos DB source |
| **Pause / Resume** | Stop and continue migration at any time |
| **Simulation Mode** | Dry run to validate connectivity without writing data |
| **All CQL Data Types** | text, int, blob, uuid, timestamp, inet, varint, collections, frozen types |

### Supported Migration Paths

| Source | Target |
|--------|--------|
| Cosmos DB Cassandra API | Azure Managed Instance for Apache Cassandra |
| Cosmos DB Cassandra API | Self-hosted Apache Cassandra (OSS) |
| Cosmos DB Cassandra API | Any CQL-compatible cluster |

---

## 2. Architecture

```
┌───────────────────────────┐       ┌───────────────────────────┐
│  Source                   │       │  Target                   │
│  Cosmos DB Cassandra API  │       │  Apache Cassandra / MI    │
│  (Port 10350, SSL)        │       │  (Port 9042)              │
└───────────┬───────────────┘       └───────────┬───────────────┘
            │                                   │
            │   ┌───────────────────────────┐   │
            └──►│  Migration Tool           │───┘
                │  (.NET 9 Web App)         │
                │                           │
                │  Phase 1: Schema Copy     │
                │  Phase 2: Bulk Data Copy  │
                │  Phase 3: Change Feed     │
                │          (Online only)    │
                └───────────────────────────┘
```

**Key design points:**

- **Web-based UI** — no CLI or SDK knowledge required.
- **Deployed as Azure App Service** (Windows) or on-premises
  IIS. Also supports Azure Container Apps.
- **Source auth** via Azure AD Managed Identity — no
  passwords stored for the source.
- **Target connectivity** via VNet integration — the App
  Service must have network access to the target Cassandra
  cluster (same VNet or VNet peering).
- **State persistence** — job state is saved to local disk
  (default) or a remote Cassandra-backed store. Jobs
  survive App Service restarts and auto-resume.

---

## 3. Prerequisites

### 3.1 Azure Resources

| Resource | Purpose | Notes |
|----------|---------|-------|
| **Source Cosmos DB Cassandra API account** | Data source | Must have Cassandra API enabled |
| **Target Cassandra cluster** | Migration destination | MI, OSS, or any CQL-compatible |
| **Azure App Service Plan** (Windows, B2+) | Hosts the migration tool | Minimum B2 recommended for production workloads |
| **Azure App Service** (Windows) | The migration web app | .NET 9 runtime |
| **Virtual Network** | Network connectivity to target | Required if target is VNet-isolated (e.g., MI) |

### 3.2 Network Requirements

The migration tool must be able to reach:

| Endpoint | Port | Protocol |
|----------|------|----------|
| Source Cosmos DB Cassandra API | **10350** | SSL/TLS |
| Target Cassandra cluster | **9042** (default) | SSL or plaintext |

**For Azure Managed Instance targets:**
- The App Service **must** be VNet-integrated into the same
  VNet as the MI cluster (different subnet) or a peered VNet.
- MI uses a **delegated subnet** — you cannot deploy the App
  Service into the same subnet. Create a separate subnet.

### 3.3 Authentication

**Source (Cosmos DB Cassandra API):**
- **Recommended:** Azure AD with Managed Identity
  (passwordless). Enable System-Assigned Managed Identity on
  the App Service and grant it the
  `Cosmos DB Built-in Data Reader` role (or
  `Cosmos DB Built-in Data Contributor` if using Change Feed)
  on the Cosmos DB account.
- **Alternative:** Username + primary key from the Cosmos DB
  account's Connection String blade.

**Target (Apache Cassandra / MI):**
- Username + password (if authentication is enabled).
- Leave blank if no authentication is configured.

### 3.4 For Online Migration (FFCF)

Full Fidelity Change Feed must be enabled **at table
creation time** on the source Cosmos DB account:

```sql
CREATE TABLE keyspace.tablename (
    ...
) WITH cosmosdb_cell_level_timestamp = true
  AND cosmosdb_cell_level_timestamp_tombstones = true
  AND cosmosdb_cell_level_timetolive = true;
```

> ⚠️ **Important:** FFCF cannot be enabled retroactively
> with `ALTER TABLE`. Tables must be created with these
> properties. If your tables were created without FFCF,
> use **Offline** migration mode instead.

---

## 4. Deployment

### 4.1 Option A — Azure App Service (Recommended)

#### Step 1: Create App Service

```bash
# Create resource group (if needed)
az group create --name <rg-name> --location <region>

# Create App Service Plan (Windows, B2 minimum)
az appservice plan create \
  --name <plan-name> \
  --resource-group <rg-name> \
  --sku B2 \
  --is-linux false

# Create Web App
az webapp create \
  --name <app-name> \
  --resource-group <rg-name> \
  --plan <plan-name> \
  --runtime "dotnet:9"
```

#### Step 2: Enable Managed Identity

```bash
az webapp identity assign \
  --name <app-name> \
  --resource-group <rg-name>
```

Note the `principalId` from the output — you'll need it
for RBAC.

#### Step 3: Grant Cosmos DB RBAC

```bash
# For read-only (offline migration):
az cosmosdb sql role assignment create \
  --account-name <cosmos-account> \
  --resource-group <cosmos-rg> \
  --role-definition-id 00000000-0000-0000-0000-000000000001 \
  --principal-id <managed-identity-principal-id> \
  --scope "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.DocumentDB/databaseAccounts/<account>"

# For read + change feed (online migration):
az cosmosdb sql role assignment create \
  --account-name <cosmos-account> \
  --resource-group <cosmos-rg> \
  --role-definition-id 00000000-0000-0000-0000-000000000002 \
  --principal-id <managed-identity-principal-id> \
  --scope "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.DocumentDB/databaseAccounts/<account>"
```

#### Step 4: Configure VNet Integration

```bash
# Create a subnet for the App Service (if needed)
az network vnet subnet create \
  --vnet-name <target-vnet> \
  --name appservice-subnet \
  --resource-group <rg-name> \
  --address-prefixes 10.x.x.0/27 \
  --delegation Microsoft.Web/serverFarms

# Add VNet integration
az webapp vnet-integration add \
  --name <app-name> \
  --resource-group <rg-name> \
  --vnet <target-vnet> \
  --subnet appservice-subnet
```

#### Step 5: Deploy the Application

**Option A — ZIP Deploy:**
```bash
# Build
dotnet publish CassandraMigrationWebApp.csproj \
  -c Release -o ./publish

# Zip and deploy
cd publish
zip -r ../deploy.zip .
az webapp deploy \
  --name <app-name> \
  --resource-group <rg-name> \
  --src-path ../deploy.zip \
  --type zip
```

**Option B — Visual Studio Publish:**
1. Right-click the `CassandraMigrationWebApp` project
2. Select **Publish** → **Azure** → **Azure App Service**
3. Select or create the App Service
4. Click **Publish**

#### Step 6: Verify Connectivity

Open the Kudu console (`https://<app-name>.scm.azurewebsites.net`)
and test:

```bash
# Test source connectivity
tcpping <source>.cassandra.cosmos.azure.com:10350

# Test target connectivity
tcpping <target-ip>:9042
```

### 4.2 Option B — On-Premises IIS

1. Install the [.NET 9 Hosting Bundle](https://dotnet.microsoft.com/download/dotnet/9.0)
2. Build the application:
   ```bash
   dotnet publish CassandraMigrationWebApp.csproj \
     -c Release -o C:\MigrationTool
   ```
3. Create an IIS site pointing to `C:\MigrationTool`
4. Set the Application Pool to **No Managed Code**
5. Ensure network connectivity from the IIS server to both
   source (port 10350) and target (port 9042)

### 4.3 Option C — Azure Container Apps

See `ACA/README.md` in the source repository for container
deployment instructions.

---

## 5. Configuration

### 5.1 Application Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `StateStore:ConnectionStringOrPath` | `C:\MigrationDrive` | Path for local state persistence |
| `StateStore:UseLocalDisk` | `true` | Use local disk for job state |
| `DetailedErrors` | `true` | Show detailed error pages |

These can be set as App Service **Application Settings**
(environment variables):

| Environment Variable | Maps To |
|---------------------|---------|
| `StateStoreConnectionStringOrPath` | State store path |
| `StateStoreUseLocalDisk` | `true` / `false` |

### 5.2 Migration Settings (Per-Job, via UI)

These are configured in the **Settings** dialog within the
tool (gear icon on the dashboard):

| Setting | Default | Description |
|---------|---------|-------------|
| CQL Copy Page Size | 500 | Rows per page during bulk copy |
| Change Feed Max Rows/Batch | 10,000 | Max rows per change feed batch |
| Change Feed Batch Duration | 120s | Seconds per change feed batch |
| Change Feed Poll Interval | 5,000ms | Polling interval for new changes |
| Change Feed Full Fidelity | true | Use FFCF (vs. standard change feed) |
| Parallel Threads | configurable | Number of parallel copy threads |

---

## 6. Running a Migration

### 6.1 First-Time Login

1. Navigate to `https://<your-app>.azurewebsites.net`
2. You will be redirected to the **Set Password** page
3. Enter and confirm a local application password
4. This password protects the tool's UI — it is stored
   locally (encrypted) and is separate from Azure AD

### 6.2 Create an Offline Migration Job

1. Click **"New Job"** on the dashboard
2. Fill in the **Basic** tab:

   **Source Connection:**
   | Field | Value | Notes |
   |-------|-------|-------|
   | Contact Point | `<account>.cassandra.cosmos.azure.com` | Your Cosmos DB endpoint |
   | Port | `10350` | Cosmos DB Cassandra API port |
   | Username | *(leave blank if using AAD)* | |
   | Password | *(leave blank if using AAD)* | |
   | Use AAD | ☑ Check this | Token fetched via Managed Identity |

   **Target Connection:**
   | Field | Value | Notes |
   |-------|-------|-------|
   | Contact Point | `<target-ip-or-hostname>` | Target MI cluster IP or DNS |
   | Port | `9042` | Default Cassandra port |
   | Username | *(if auth enabled)* | e.g., `cassandra` |
   | Password | *(if auth enabled)* | |

   **Tables to Migrate:**
   | Pattern | Description |
   |---------|-------------|
   | `keyspace.*` | All tables in a keyspace |
   | `keyspace.table1,keyspace.table2` | Specific tables |
   | `ks1.*,ks2.tableA` | Mix of wildcards and specific |

   **Migration Mode:** Select **Offline**

3. *(Optional)* Review the **Advanced** tab:
   - **Simulation Mode** — validates connectivity and schema
     without writing data
   - **Log Level** — `Info` (default), `Debug`, or `Error`
   - **Append Mode** — if target tables already have data,
     append instead of failing

4. Click **Submit**

### 6.3 What Happens During Offline Migration

The tool executes these phases automatically:

```
Phase 1: Schema Discovery
  └─ Connects to source
  └─ Discovers keyspaces, tables, columns, keys
  └─ Generates CQL DDL statements

Phase 2: Schema Creation
  └─ Connects to target
  └─ Creates keyspaces (if not exist)
  └─ Creates tables with matching schema

Phase 3: Data Copy
  └─ Reads source rows page-by-page (configurable page size)
  └─ Inserts rows into target
  └─ Tracks progress per table
  └─ Saves checkpoint state to disk (survives restarts)

Phase 4: Completion
  └─ Marks job as complete
  └─ Generates summary report
```

---

## 7. Online Migration with Change Feed

Online migration adds a **live sync phase** after the bulk
copy, using Cosmos DB Full Fidelity Change Feed (FFCF).

### 7.1 Create an Online Migration Job

Follow the same steps as Section 6.2, but select
**Online** for Migration Mode.

> ⚠️ All tables selected for online migration **must** have
> FFCF enabled at creation time (see Section 3.4).

### 7.2 How Online Migration Works

```
Phase 1-2: Schema Discovery & Creation (same as offline)

Phase 3: Bulk Copy
  └─ Copies all existing rows
  └─ Captures a change feed continuation token at start

Phase 4: Change Feed Sync
  └─ Polls source for new changes since the token
  └─ Replays INSERTs, UPDATEs, DELETEs on target
  └─ Handles TTL expirations
  └─ Continues indefinitely until cutover

Phase 5: Cutover (manual)
  └─ User clicks "Cut Over" when lag approaches zero
  └─ Final batch of changes is applied
  └─ Job marked as complete
```

### 7.3 Performing Cutover

1. Open the **Job Viewer** for your online job
2. Monitor the **Change Feed Lag** — this shows how far
   behind the target is from the source
3. When lag is minimal (near zero):
   - **Stop writes to the source** (application-level)
   - Wait for the final batch to complete
   - Click **"Cut Over"**
4. The tool applies any remaining changes and marks the
   job as complete
5. Redirect your application to the target cluster

---

## 8. Monitoring & Management

### 8.1 Job Viewer

The Job Viewer (`eye icon` on dashboard) shows:

| Column | Description |
|--------|-------------|
| **Table Name** | Source keyspace.table |
| **Status** | Pending / In Progress / Completed / Failed |
| **Rows Copied** | Count of rows transferred |
| **Progress** | Visual progress indicator |
| **Change Feed** | Active / Idle / N/A (for online jobs) |

### 8.2 Controls

| Button | Action |
|--------|--------|
| **Resume Job** | Start or continue a paused job |
| **Pause Job** | Pause after current batch completes |
| **Controlled Pause** | Finish current table, then pause |
| **Cancel** | Stop the job permanently |
| **Cut Over** | (Online only) Finalize and complete |
| **Update Tables** | Add or remove tables mid-migration |
| **Reset Change Feed** | Reprocess change feed from an earlier point |

### 8.3 Logs

- Click the **download** button in the Job Viewer to
  download migration logs
- Logs include per-table copy progress, errors, and
  change feed events
- App-level diagnostics are written to
  `D:\home\LogFiles\app-diag.log` on App Service

### 8.4 Job Report

Click the **printer icon** on the dashboard to generate a
printable migration report with:
- Job configuration summary
- Per-table status and row counts
- Start/end timestamps
- Error summary (if any)

### 8.5 Auto-Resume

If the App Service restarts (planned or unplanned):
- The tool automatically detects interrupted jobs on startup
- Jobs resume from the last saved checkpoint
- No manual intervention required

---

## 9. Data Verification

### 9.1 Using the Cassandra Data Viewer

A companion **Cassandra Data Viewer** web app is available
for browsing and querying the target cluster.

1. Deploy the viewer app to the same App Service Plan
   (or any host with network access to the target)
2. Open the viewer in a browser
3. Enter target connection details:
   - **Host:** Target IP or hostname
   - **Port:** 9042
   - **SSL:** ☑ (required for MI)
   - **Username/Password:** if applicable
4. Click **Connect**
5. Use **Browse Data** to inspect migrated keyspaces
   and tables
6. Use the **Query** tab to run CQL:
   ```sql
   SELECT COUNT(*) FROM keyspace.tablename;
   ```

### 9.2 Verification Checklist

| Check | How |
|-------|-----|
| Row counts match | Compare `SELECT COUNT(*)` on source vs target |
| Schema matches | Compare `DESCRIBE TABLE` on both sides |
| Sample data spot-check | Query specific rows by primary key |
| Collection types | Verify list/set/map columns have correct values |
| Static columns | Verify static column values are correct |
| TTL values | Check TTL on rows if applicable |

---

## 10. Troubleshooting

### 10.1 Common Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| **"Connection refused" to target** | App Service not VNet-integrated, or wrong subnet | Verify VNet integration in App Service → Networking. Test with `tcpping` from Kudu. |
| **"Authentication error" on source** | Managed Identity not configured or missing RBAC | Ensure System MI is enabled. Verify Cosmos DB RBAC role assignment. |
| **AAD token error / 401** | Expired token or wrong scope | The tool auto-refreshes tokens. If stuck, restart the job. |
| **"Table not found" on source** | Wrong keyspace/table name or case sensitivity | Cassandra names are case-sensitive. Verify exact names. |
| **FFCF query rejected** | Table created without FFCF properties | FFCF must be enabled at CREATE TABLE time. Use Offline mode for these tables. |
| **Slow migration speed** | Small page size or low parallelism | Increase `CQL Copy Page Size` and `Parallel Threads` in Settings. Scale up App Service plan. |
| **Job stuck after restart** | State corruption or connectivity loss | Check logs. Try pausing and resuming. If needed, cancel and recreate the job. |
| **Target SSL connection fails** | Certificate validation or wrong port | The tool tries SSL first, then falls back to plaintext. Ensure port 9042 is open. |

### 10.2 Network Debugging

From the App Service Kudu console
(`https://<app>.scm.azurewebsites.net/DebugConsole`):

```bash
# Test source Cosmos DB
tcpping <account>.cassandra.cosmos.azure.com:10350

# Test target MI / Cassandra
tcpping <target-ip>:9042

# DNS resolution
nameresolver <hostname>
```

### 10.3 Log Locations

| Log | Location |
|-----|----------|
| App diagnostics | `D:\home\LogFiles\app-diag.log` |
| Migration job logs | Download via Job Viewer UI |
| App Service platform logs | Azure Portal → App Service → Log stream |
| Detailed errors | Enable `ASPNETCORE_DETAILEDERRORS=true` in App Settings |

---

## 11. FAQ

**Q: Can I migrate multiple keyspaces in one job?**
A: Yes. Use comma-separated patterns:
`keyspace1.*,keyspace2.*,keyspace3.specific_table`

**Q: What happens if the App Service restarts mid-migration?**
A: The tool auto-resumes from the last checkpoint. No data
is lost or duplicated.

**Q: Can I run multiple migration jobs simultaneously?**
A: Yes. Each job runs independently with its own state.
Be mindful of source RU consumption and target write
capacity.

**Q: Does the tool handle schema differences?**
A: The tool auto-generates target DDL from the source
schema. If the target table already exists, it uses the
existing schema. Use **Append Mode** to add rows to
existing tables.

**Q: What is Simulation Mode?**
A: Simulation mode connects to source and target, discovers
schema, and validates everything — but does not write any
data. Use it to verify connectivity before starting a real
migration.

**Q: How do I migrate only specific tables?**
A: Enter comma-separated fully-qualified table names:
`keyspace.table1,keyspace.table2`

**Q: What Cassandra versions are supported as targets?**
A: The tool uses the standard CQL protocol. Any Cassandra
3.x or 4.x cluster should work, including Azure Managed
Instance for Apache Cassandra.

**Q: Can I use this tool for Cassandra-to-Cassandra
(non-Cosmos DB source)?**
A: The tool is designed for Cosmos DB Cassandra API as the
source. For OSS-to-OSS migration, the bulk copy may work
but Change Feed is Cosmos DB-specific.

**Q: How do I estimate migration time?**
A: Migration speed depends on data volume, row size, source
RU throughput, target write capacity, network latency, and
parallelism settings. As a rough guide, expect 500–2,000
rows/second per thread with default settings. A 10 GB
dataset with ~1.8M rows completes in approximately 30–60
minutes with default settings.

---

## Appendix A: Quick Reference

### Connection Defaults

| Parameter | Cosmos DB Source | Cassandra MI Target |
|-----------|-----------------|---------------------|
| Port | 10350 | 9042 |
| SSL | Required | Required |
| Auth | AAD (Managed Identity) | Username/Password (optional) |

### Minimum RBAC Roles

| Migration Type | Cosmos DB Role |
|---------------|----------------|
| Offline | `Cosmos DB Built-in Data Reader` |
| Online (FFCF) | `Cosmos DB Built-in Data Contributor` |

### App Service Requirements

| Setting | Minimum | Recommended |
|---------|---------|-------------|
| SKU | B1 | B2 or S1 |
| .NET Runtime | 9.0 | 9.0 |
| OS | Windows | Windows |
| VNet Integration | Required (for MI targets) | Required |
| Managed Identity | System-Assigned | System-Assigned |

---

## Appendix B: Networking Options

### Option A: Same VNet (Recommended)

```
VNet: <target-vnet>
├── Subnet: mi-subnet (delegated to MI)
│   └── Cassandra MI cluster
└── Subnet: appservice-subnet (/27 min)
    └── Migration Tool (App Service)
```

- Simplest setup — direct connectivity
- App Service and MI in the same VNet, different subnets

### Option B: VNet Peering (Cross-Region)

```
VNet A: <appservice-vnet> (Region 1)
├── Subnet: appservice-subnet
│   └── Migration Tool
│
│   ↕ VNet Peering
│
VNet B: <target-vnet> (Region 2)
├── Subnet: mi-subnet
│   └── Cassandra MI cluster
```

- Use when App Service and target are in different regions
- Requires bidirectional VNet peering
- Slight latency increase due to cross-region traffic
