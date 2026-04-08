# Networking Guide

## Cassandra Migration Tool — VNet Integration & Private Connectivity

This guide explains how to deploy the migration tool with private
network connectivity to both the source (Cosmos DB Cassandra API)
and target (Azure Managed Instance for Apache Cassandra).

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Azure Virtual Network                        │
│                        (e.g. 10.0.0.0/16)                          │
│                                                                     │
│  ┌─────────────────────────┐    ┌─────────────────────────────────┐ │
│  │   App Service Subnet     │    │   Managed Instance Subnet       │ │
│  │   (10.0.1.0/24)          │    │   (10.0.0.0/24)                 │ │
│  │                           │    │                                 │ │
│  │  ┌─────────────────────┐ │    │  ┌───────────────────────────┐ │ │
│  │  │  App Service         │ │    │  │  Azure Managed Instance   │ │ │
│  │  │  (Migration Tool)    │ │    │  │  for Apache Cassandra     │ │ │
│  │  │                      │ │    │  │                           │ │ │
│  │  │  Blazor Server App   │─┼────┼─▶│  Port 9042 (CQL)         │ │ │
│  │  │  (.NET 8)            │ │    │  │  Target cluster           │ │ │
│  │  └─────────────────────┘ │    │  └───────────────────────────┘ │ │
│  │           │               │    │                                 │ │
│  └───────────┼───────────────┘    └─────────────────────────────────┘ │
│              │                                                       │
│  ┌───────────┼───────────────────────────────────────────────┐       │
│  │   Private Endpoint Subnet                                  │       │
│  │   (10.0.2.0/24)                                            │       │
│  │              │                                              │       │
│  │  ┌──────────▼──────────┐                                   │       │
│  │  │  Private Endpoint    │                                   │       │
│  │  │  for Cosmos DB       │                                   │       │
│  │  │  (10.0.2.4)          │                                   │       │
│  │  └──────────┬──────────┘                                   │       │
│  └─────────────┼─────────────────────────────────────────────┘       │
│                │                                                     │
└────────────────┼─────────────────────────────────────────────────────┘
                 │  Private Link
                 ▼
    ┌────────────────────────────┐
    │  Azure Cosmos DB           │
    │  Cassandra API             │
    │                            │
    │  Port 10350 (CQL over TLS) │
    │  Source database            │
    └────────────────────────────┘
```

### Network Flow

| Path | From | To | Port | Method |
|------|------|----|------|--------|
| **Writes** | App Service (10.0.1.x) | Managed Instance (10.0.0.x) | 9042 | Direct VNet routing |
| **Reads** | App Service (10.0.1.x) | Cosmos DB Private Endpoint (10.0.2.4) | 10350 | Private Link |
| **UI Access** | User browser | App Service | 443 | Public or App Service access restrictions |

---

## Step 1: Prerequisites

- An Azure Virtual Network with at least 3 subnets (or create them below)
- Azure Managed Instance for Apache Cassandra already deployed in its subnet
- Azure Cosmos DB Cassandra API account (source)
- Azure App Service plan (P1v3 or higher recommended)

---

## Step 2: Create Subnets

The MI subnet already exists. You need two additional subnets:

### App Service Subnet

This subnet will be delegated to App Service for VNet integration.

```bash
# Create the App Service subnet
az network vnet subnet create \
  --resource-group <resource-group> \
  --vnet-name <vnet-name> \
  --name app-service-subnet \
  --address-prefixes 10.0.1.0/24 \
  --delegations Microsoft.Web/serverFarms
```

> **Important:** The App Service subnet must be delegated to
> `Microsoft.Web/serverFarms`. This is required for VNet integration.

### Private Endpoint Subnet

```bash
# Create the Private Endpoint subnet
az network vnet subnet create \
  --resource-group <resource-group> \
  --vnet-name <vnet-name> \
  --name private-endpoint-subnet \
  --address-prefixes 10.0.2.0/24
```

> **Note:** Private endpoint subnets must NOT have any delegations.
> Disable network policies if needed:
> ```bash
> az network vnet subnet update \
>   --resource-group <resource-group> \
>   --vnet-name <vnet-name> \
>   --name private-endpoint-subnet \
>   --disable-private-endpoint-network-policies true
> ```

---

## Step 3: Deploy App Service with VNet Integration

### Create the App Service

```bash
# Create App Service plan
az appservice plan create \
  --name cassandra-migration-plan \
  --resource-group <resource-group> \
  --sku P1V3 \
  --is-linux false

# Create the Web App
az webapp create \
  --name <app-name> \
  --resource-group <resource-group> \
  --plan cassandra-migration-plan \
  --runtime "dotnet:8"
```

### Enable VNet Integration

```bash
# Integrate App Service with the VNet
az webapp vnet-integration add \
  --name <app-name> \
  --resource-group <resource-group> \
  --vnet <vnet-name> \
  --subnet app-service-subnet
```

### Configure Route All Traffic Through VNet

```bash
# Ensure all outbound traffic goes through the VNet
az webapp config appsettings set \
  --name <app-name> \
  --resource-group <resource-group> \
  --settings WEBSITE_VNET_ROUTE_ALL=1
```

> This ensures traffic to Cosmos DB goes through the Private
> Endpoint instead of over the public internet.

---

## Step 4: Create Cosmos DB Private Endpoint

### Create the Private Endpoint

```bash
# Get the Cosmos DB resource ID
COSMOS_ID=$(az cosmosdb show \
  --name <cosmos-account-name> \
  --resource-group <cosmos-resource-group> \
  --query id --output tsv)

# Create Private Endpoint
az network private-endpoint create \
  --name cosmos-cassandra-pe \
  --resource-group <resource-group> \
  --vnet-name <vnet-name> \
  --subnet private-endpoint-subnet \
  --private-connection-resource-id $COSMOS_ID \
  --group-id Cassandra \
  --connection-name cosmos-cassandra-connection
```

> **Note:** The `--group-id` must be `Cassandra` for Cosmos DB
> Cassandra API accounts.

### Configure Private DNS Zone

For the private endpoint to resolve correctly, you need a Private
DNS Zone linked to the VNet:

```bash
# Create Private DNS Zone
az network private-dns zone create \
  --resource-group <resource-group> \
  --name privatelink.cassandra.cosmos.azure.com

# Link DNS Zone to VNet
az network private-dns zone vnet-link create \
  --resource-group <resource-group> \
  --zone-name privatelink.cassandra.cosmos.azure.com \
  --name cosmos-dns-link \
  --virtual-network <vnet-name> \
  --registration-enabled false

# Create DNS Zone Group for auto-registration
az network private-endpoint dns-zone-group create \
  --resource-group <resource-group> \
  --endpoint-name cosmos-cassandra-pe \
  --name cosmos-dns-group \
  --private-dns-zone privatelink.cassandra.cosmos.azure.com \
  --zone-name cassandra
```

### Disable Public Access (Optional but Recommended)

```bash
# Disable public network access on Cosmos DB
az cosmosdb update \
  --name <cosmos-account-name> \
  --resource-group <cosmos-resource-group> \
  --public-network-access DISABLED
```

---

## Step 5: Verify Connectivity

### Verify from App Service

Use the App Service console (Kudu) or SSH to test connectivity:

```bash
# Test target MI connectivity (direct VNet)
tcpping <mi-ip>:9042

# Test source Cosmos DB connectivity (via Private Endpoint)
# Should resolve to private IP (10.0.2.x)
nslookup <cosmos-account>.cassandra.cosmos.azure.com
tcpping <cosmos-account>.cassandra.cosmos.azure.com:10350
```

**Expected DNS resolution:**
```
<cosmos-account>.cassandra.cosmos.azure.com
  → <cosmos-account>.privatelink.cassandra.cosmos.azure.com
  → 10.0.2.4  (private endpoint IP)
```

If it resolves to a public IP, verify:
- `WEBSITE_VNET_ROUTE_ALL=1` is set
- Private DNS zone is linked to the VNet
- DNS zone group is configured on the private endpoint

### Configure the Migration Tool

In the migration tool UI, use:

| Field | Value |
|-------|-------|
| **Source Contact Point** | `<cosmos-account>.cassandra.cosmos.azure.com` |
| **Source Port** | `10350` |
| **Target Contact Point** | `<mi-private-ip>` (e.g., `10.0.0.5`) |
| **Target Port** | `9042` |

---

## Network Security Groups (NSGs)

### App Service Subnet NSG

| Rule | Direction | Source | Destination | Port | Action |
|------|-----------|--------|-------------|------|--------|
| Allow CQL to MI | Outbound | 10.0.1.0/24 | 10.0.0.0/24 | 9042 | Allow |
| Allow CQL to Cosmos PE | Outbound | 10.0.1.0/24 | 10.0.2.0/24 | 10350 | Allow |
| Allow HTTPS (UI) | Inbound | Any / Your IP | 10.0.1.0/24 | 443 | Allow |

### Private Endpoint Subnet NSG

| Rule | Direction | Source | Destination | Port | Action |
|------|-----------|--------|-------------|------|--------|
| Allow from App Service | Inbound | 10.0.1.0/24 | 10.0.2.0/24 | 10350 | Allow |

> **Tip:** NSG rules on private endpoint subnets require
> `PrivateEndpointNetworkPolicies` to be enabled. By default
> they are disabled (NSG rules are ignored).

---

## Alternative: App Service in MI's Subnet

If you don't need subnet isolation, you can deploy the App
Service in the same subnet as Managed Instance:

```
┌──────────────────────────────────────────┐
│  MI Subnet (10.0.0.0/24)                 │
│                                           │
│  ┌──────────────┐  ┌──────────────────┐  │
│  │ App Service   │  │ Managed Instance │  │
│  │ (10.0.0.10)   │──│ (10.0.0.5)       │  │
│  └──────┬───────┘  └──────────────────┘  │
│         │                                 │
└─────────┼─────────────────────────────────┘
          │ Private Link
          ▼
   ┌────────────────┐
   │ Cosmos DB       │
   │ Private Endpoint│
   └────────────────┘
```

This simplifies networking but requires the MI subnet to allow
`Microsoft.Web/serverFarms` delegation alongside the MI resources.

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| `No host available` to MI | App Service not in same VNet | Enable VNet integration on App Service |
| Cosmos DB resolves to public IP | DNS misconfigured | Link Private DNS zone to VNet, set `WEBSITE_VNET_ROUTE_ALL=1` |
| Connection timeout to Cosmos DB | NSG blocking traffic | Allow outbound 10350 from App Service subnet to PE subnet |
| `SSL handshake failed` | Port mismatch | Use port 10350 for Cosmos DB (not 9042) |
| MI connection refused | Wrong port or IP | Verify MI contact point IP and port 9042 |
| Intermittent failures | DNS cache | Restart App Service after enabling Private DNS zone |
