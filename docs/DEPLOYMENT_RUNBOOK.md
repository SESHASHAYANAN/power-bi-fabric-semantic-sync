# Deployment Runbook - Production Sync System

## Overview
This runbook provides step-by-step instructions for deploying the bidirectional sync system to production.

**Target Audience**: DevOps Engineers, SREs, Platform Engineers  
**Estimated Time**: 2-4 hours for first-time deployment  
**Prerequisites**: Access to Azure, Snowflake, production infrastructure

---

## Pre-Deployment Checklist

### ✅ Access & Permissions
- [ ] Azure AD Global Administrator or Application Administrator role
- [ ] Snowflake ACCOUNTADMIN or equivalent
- [ ] Access to production secrets management (Azure Key Vault, etc.)
- [ ] Access to deployment environment (Azure App Service, VM, K8s)
- [ ] Monitoring system access (Prometheus, Grafana)

### ✅ Infrastructure Requirements
- [ ] Compute: 2 CPU cores, 4GB RAM minimum
- [ ] Python 3.9+ runtime
- [ ] Network: Outbound HTTPS to Fabric API, Snowflake
- [ ] Storage: 10GB for logs and temporary data

### ✅ Dependencies Ready
- [ ] Microsoft Fabric workspace created
- [ ] Snowflake database and schema created
- [ ] Service principal registered in Azure AD
- [ ] Snowflake user created with required permissions

---

## Phase 1: Azure AD Setup (30 minutes)

### 1.1 Register Application in Azure AD

```bash
# Using Azure CLI
az login

# Create app registration
az ad app create \
  --display-name "Fabric-Snowflake Sync Service" \
  --sign-in-audience AzureADMyOrg

# Save the output - you'll need appId (CLIENT_ID) and id (OBJECT_ID)
```

**Manual Steps** (Azure Portal):
1. Navigate to Azure Portal → Azure Active Directory → App Registrations
2. Click "New registration"
3. Name: `Fabric-Snowflake Sync Service`
4. Supported account types: "Accounts in this organizational directory only"
5. Click "Register"
6. Copy **Application (client) ID** → This is your `FABRIC_CLIENT_ID`
7. Copy **Directory (tenant) ID** → This is your `FABRIC_TENANT_ID`

### 1.2 Create Client Secret

```bash
# Create secret (valid for 1 year)
az ad app credential reset \
  --id <APP_ID> \
  --append \
  --display-name "Sync Service Secret" \
  --years 1

# Copy the 'password' value - this is FABRIC_CLIENT_SECRET
# WARNING: Secret is only shown once!
```

**Manual Steps**:
1. In App Registration → "Certificates & secrets"
2. Click "New client secret"
3. Description: `Sync Service Production`
4. Expires: 12 months (recommended)
5. Copy the **Value** (not Secret ID) → This is your `FABRIC_CLIENT_SECRET`
6. **CRITICAL**: Store in secure location immediately (Azure Key Vault)

### 1.3 Grant API Permissions

**Required Permissions**:
- Microsoft Graph: `User.Read` (Delegated)
- Power BI Service: `Workspace.ReadWrite.All` (Delegated)
- Fabric API: `Item.ReadWrite.All` (Application)

```bash
# Add Microsoft Graph permission
az ad app permission add \
  --id <APP_ID> \
  --api 00000003-0000-0000-c000-000000000000 \
  --api-permissions e1fe6dd8-ba31-4d61-89e7-88639da4683d=Scope

# Grant admin consent
az ad app permission admin-consent --id <APP_ID>
```

**Manual Steps**:
1. App Registration → "API permissions"
2. Click "Add a permission"
3. Select "Microsoft Graph" → "Delegated permissions" → Search and select `User.Read`
4. Click "Add permissions"
5. Repeat for Power BI Service and Fabric API
6. Click "Grant admin consent for [Your Org]"
7. Verify all permissions show green checkmarks

### 1.4 Grant Workspace Access

```bash
# Get service principal object ID
SP_OBJECT_ID=$(az ad sp show --id <APP_ID> --query id -o tsv)

# In Fabric portal:
# 1. Navigate to your workspace
# 2. Settings → Access
# 3. Add service principal with Admin or Contributor role
```

---

## Phase 2: Snowflake Setup (30 minutes)

### 2.1 Create Database Objects

**Login as ACCOUNTADMIN**:
```sql
USE ROLE ACCOUNTADMIN;

-- Create database
CREATE DATABASE IF NOT EXISTS FABRIC_SYNC_DB
  COMMENT = 'Bidirectional sync metadata and synced data';

-- Create schema
CREATE SCHEMA IF NOT EXISTS FABRIC_SYNC_DB.SYNC_METADATA;

-- Create compute warehouse
CREATE WAREHOUSE IF NOT EXISTS SYNC_COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'SMALL'
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       INITIALLY_SUSPENDED = FALSE
  COMMENT = 'Dedicated warehouse for sync operations';
```

### 2.2 Create Sync Service User

```sql
-- Create user
CREATE USER IF NOT EXISTS SYNC_SERVICE
  PASSWORD = '<GENERATE_STRONG_PASSWORD>'
  DEFAULT_ROLE = SYNC_ROLE
  DEFAULT_WAREHOUSE = SYNC_COMPUTE_WH
  COMMENT = 'Service account for Fabric-Snowflake sync';

-- Create role
CREATE ROLE IF NOT EXISTS SYNC_ROLE
  COMMENT = 'Role for sync service operations';

-- Grant role to user
GRANT ROLE SYNC_ROLE TO USER SYNC_SERVICE;
```

### 2.3 Grant Permissions

```sql
-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE SYNC_COMPUTE_WH TO ROLE SYNC_ROLE;

-- Grant database and schema access
GRANT USAGE ON DATABASE FABRIC_SYNC_DB TO ROLE SYNC_ROLE;
GRANT USAGE ON SCHEMA FABRIC_SYNC_DB.SYNC_METADATA TO ROLE SYNC_ROLE;

-- Grant table privileges
GRANT CREATE TABLE ON SCHEMA FABRIC_SYNC_DB.SYNC_METADATA TO ROLE SYNC_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FABRIC_SYNC_DB.SYNC_METADATA TO ROLE SYNC_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA FABRIC_SYNC_DB.SYNC_METADATA TO ROLE SYNC_ROLE;

-- Grant view privileges (for monitoring)
GRANT SELECT ON ALL VIEWS IN SCHEMA FABRIC_SYNC_DB.SYNC_METADATA TO ROLE SYNC_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA FABRIC_SYNC_DB.SYNC_METADATA TO ROLE SYNC_ROLE;
```

### 2.4 Execute Schema DDL

```sql
-- Switch to sync role
USE ROLE SYNC_ROLE;
USE WAREHOUSE SYNC_COMPUTE_WH;
USE DATABASE FABRIC_SYNC_DB;
USE SCHEMA SYNC_METADATA;

-- Execute the schema creation script
-- Copy contents from: infrastructure/snowflake/sf_sync_manifest.sql
-- Then execute in Snowflake worksheet
```

### 2.5 Configure Network Policy (Optional but Recommended)

```sql
USE ROLE ACCOUNTADMIN;

-- Get your sync service IP address
-- Then create network policy
CREATE NETWORK POLICY SYNC_SERVICE_POLICY
  ALLOWED_IP_LIST = ('YOUR.SYNC.SERVICE.IP', 'YOUR.BACKUP.IP');

-- Apply to sync user
ALTER USER SYNC_SERVICE SET NETWORK_POLICY = 'SYNC_SERVICE_POLICY';
```

---

## Phase 3: Application Deployment (60 minutes)

### Option A: Azure App Service (Recommended)

#### 3.1 Create App Service

```bash
# Create resource group
az group create \
  --name rg-fabric-sync-prod \
  --location eastus

# Create App Service plan (Linux, Python)
az appservice plan create \
  --name plan-fabric-sync \
  --resource-group rg-fabric-sync-prod \
  --sku B2 \
  --is-linux

# Create web app
az webapp create \
  --name app-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod \
  --plan plan-fabric-sync \
  --runtime "PYTHON|3.11"
```

#### 3.2 Configure Application Settings

```bash
# Set environment variables
az webapp config appsettings set \
  --name app-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod \
  --settings \
    FABRIC_TENANT_ID="<FROM_STEP_1.1>" \
    FABRIC_CLIENT_ID="<FROM_STEP_1.1>" \
    FABRIC_CLIENT_SECRET="<FROM_STEP_1.2>" \
    FABRIC_WORKSPACE_ID="<YOUR_WORKSPACE_ID>" \
    SNOWFLAKE_ACCOUNT="<YOUR_ACCOUNT>.snowflakecomputing.com" \
    SNOWFLAKE_USER="SYNC_SERVICE" \
    SNOWFLAKE_PASSWORD="<FROM_STEP_2.2>" \
    SNOWFLAKE_WAREHOUSE="SYNC_COMPUTE_WH" \
    SNOWFLAKE_DATABASE="FABRIC_SYNC_DB" \
    SNOWFLAKE_SCHEMA="SYNC_METADATA" \
    PORT="8000" \
    LOG_LEVEL="INFO"
```

**Better: Use Azure Key Vault**:
```bash
# Create Key Vault
az keyvault create \
  --name kv-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod \
  --location eastus

# Store secrets
az keyvault secret set --vault-name kv-fabric-sync-prod --name FabricClientSecret --value "<SECRET>"
az keyvault secret set --vault-name kv-fabric-sync-prod --name SnowflakePassword --value "<PASSWORD>"

# Grant App Service access to Key Vault
az webapp identity assign \
  --name app-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod

IDENTITY=$(az webapp identity show --name app-fabric-sync-prod --resource-group rg-fabric-sync-prod --query principalId -o tsv)

az keyvault set-policy \
  --name kv-fabric-sync-prod \
  --object-id $IDENTITY \
  --secret-permissions get list

# Reference in app settings
az webapp config appsettings set \
  --name app-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod \
  --settings \
    FABRIC_CLIENT_SECRET="@Microsoft.KeyVault(SecretUri=https://kv-fabric-sync-prod.vault.azure.net/secrets/FabricClientSecret/)" \
    SNOWFLAKE_PASSWORD="@Microsoft.KeyVault(SecretUri=https://kv-fabric-sync-prod.vault.azure.net/secrets/SnowflakePassword/)"
```

#### 3.3 Deploy Application

```bash
# From project root
cd "c:\Users\M.S.Seshashayanan\Desktop\API connector"

# Create deployment package
git init  # If not already a git repo
git add .
git commit -m "Production deployment"

# Configure deployment source
az webapp deployment source config-local-git \
  --name app-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod

# Get deployment URL
DEPLOY_URL=$(az webapp deployment source show \
  --name app-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod \
  --query url -o tsv)

# Add remote and push
git remote add azure $DEPLOY_URL
git push azure main:master
```

#### 3.4 Configure Startup Command

```bash
az webapp config set \
  --name app-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod \
  --startup-file "gunicorn --bind=0.0.0.0:8000 --workers=4 --timeout=300 production_sync_api:app"
```

### Option B: Docker Container

#### 3.1 Create Dockerfile
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Expose port
EXPOSE 5050

# Run application
CMD ["gunicorn", "--bind", "0.0.0.0:5050", "--workers", "4", "--timeout", "300", "production_sync_api:app"]
```

#### 3.2 Build and Push
```bash
# Build image
docker build -t your-registry.azurecr.io/fabric-sync:1.0.0 .

# Push to registry
az acr login --name your-registry
docker push your-registry.azurecr.io/fabric-sync:1.0.0
```

#### 3.3 Deploy to Azure Container Instances
```bash
az container create \
  --resource-group rg-fabric-sync-prod \
  --name fabric-sync-container \
  --image your-registry.azurecr.io/fabric-sync:1.0.0 \
  --cpu 2 \
  --memory 4 \
  --port 5050 \
  --environment-variables \
    FABRIC_TENANT_ID="..." \
    FABRIC_CLIENT_ID="..." \
    # ... other vars
  --secure-environment-variables \
    FABRIC_CLIENT_SECRET="..." \
    SNOWFLAKE_PASSWORD="..."
```

---

## Phase 4: Monitoring Setup (30 minutes)

### 4.1 Configure Application Insights

```bash
# Create Application Insights
az monitor app-insights component create \
  --app fabric-sync-insights \
  --location eastus \
  --resource-group rg-fabric-sync-prod

# Get instrumentation key
INSTRUMENTATION_KEY=$(az monitor app-insights component show \
  --app fabric-sync-insights \
  --resource-group rg-fabric-sync-prod \
  --query instrumentationKey -o tsv)

# Add to app settings
az webapp config appsettings set \
  --name app-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod \
  --settings APPINSIGHTS_INSTRUMENTATIONKEY="$INSTRUMENTATION_KEY"
```

### 4.2 Setup Prometheus & Grafana (if using external monitoring)

**Install Prometheus**:
```yaml
# prometheus.yml
global:
  scrape_interval: 30s

scrape_configs:
  - job_name: 'fabric-sync'
    static_configs:
      - targets: ['app-fabric-sync-prod.azurewebsites.net']
    metrics_path: '/api/metrics'
```

**Import Grafana Dashboard**:
1. Login to Grafana
2. Import `infrastructure/monitoring/grafana_dashboard.json`
3. Configure Prometheus data source

### 4.3 Configure Alerts

```bash
# Import alert rules
kubectl apply -f infrastructure/monitoring/prometheus_alerts.yml

# Or configure in Azure Monitor
az monitor metrics alert create \
  --name "Sync Failure Rate Critical" \
  --resource-group rg-fabric-sync-prod \
  --scopes /subscriptions/.../app-fabric-sync-prod \
  --condition "avg sync_failure_rate > 5" \
  --window-size 5m \
  --severity 0 \
  --action-group sync-oncall-group
```

---

## Phase 5: Validation & Testing (30 minutes)

### 5.1 Health Check

```bash
# Get app URL
APP_URL=$(az webapp show \
  --name app-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod \
  --query defaultHostName -o tsv)

# Test health endpoint
curl https://$APP_URL/api/health

# Expected response:
# {"status":"healthy","fabric":"connected","snowflake":"connected"}
```

### 5.2 Smoke Test - File Upload

```bash
# Create test file
echo "id,name,value
1,Test,100
2,Test2,200" > test_data.csv

# Upload
curl -X POST https://$APP_URL/api/files/upload \
  -F "file=@test_data.csv" \
  -F "user_id=deployment_test"

# Verify response has sync_id and success: true
```

### 5.3 Verify Data in Snowflake

```sql
-- Check sync manifest
SELECT * FROM FABRIC_SYNC_DB.SYNC_METADATA.SF_SYNC_MANIFEST 
ORDER BY CREATED_AT DESC 
LIMIT 10;

-- Check uploaded data
SHOW TABLES LIKE '%UPLOADED_TEST_DATA%';

SELECT * FROM "UPLOADED_TEST_DATA";
```

### 5.4 Verify Fabric

```bash
# List Fabric models
curl https://$APP_URL/api/fabric/models

# Should show the uploaded test data
```

### 5.5 Test Dashboard

```bash
curl https://$APP_URL/api/sync/dashboard

# Verify metrics are being tracked
```

---

## Phase 6: Production Cutover (30 minutes)

### 6.1 Historical Data Migration

**DRY RUN FIRST**:
```bash
# SSH into app service or run locally with production credentials
python scripts/fabric_to_snowflake_migration.py --dry-run

# Review output - how many tables will be migrated?
```

**Run Migration**:
```bash
# Schedule during maintenance window
python scripts/fabric_to_snowflake_migration.py > migration_fabric_to_snowflake.log 2>&1

# Monitor progress
tail -f migration_fabric_to_snowflake.log
```

### .2 Enable Auto-Sync

```bash
# This starts the periodic sync loop
# It's enabled by default in production_sync_api.py
# Verify it's running via dashboard
```

### 6.3 Update DNS / Load Balancer

```bash
# Point your production domain to the new service
az network dns record-set cname set-record \
  --resource-group rg-dns \
  --zone-name yourdomain.com \
  --record-set-name sync-api \
  --cname app-fabric-sync-prod.azurewebsites.net
```

---

## Post-Deployment

### ✅ Verification Checklist
- [ ] Health endpoint returns "healthy"
- [ ] Test file upload succeeds
- [ ] Data appears in both Fabric and Snowflake
- [ ] Dashboard shows correct metrics
- [ ] Prometheus scraping working (if applicable)
- [ ] Alerts firing correctly (test with intentional failure)
- [ ] Historical migration completed successfully
- [ ] Auto-sync running every 5 minutes
- [ ] Audit trail being written to Snowflake

### 📊 Monitor for 24 Hours
- Check success rate every 4 hours
- Verify no checksum mismatches
- Monitor retry queue size
- Review error logs for patterns

### 📝 Document
- [ ] Production URLs documented in wiki
- [ ] Rollback procedure documented
- [ ] On-call runbook updated
- [ ] Credentials stored securely

---

## Rollback Procedure

If issues occur in production:

### Quick Rollback (< 5 minutes)
```bash
# Stop the service
az webapp stop \
  --name app-fabric-sync-prod \
  --resource-group rg-fabric-sync-prod

# Point DNS back to old service
# Resume old service
```

### Data Rollback
```sql
-- In Snowflake, rollback syncs from specific time
DELETE FROM FABRIC_SYNC_DB.SYNC_METADATA.SF_SYNC_MANIFEST
WHERE CREATED_AT > 'YYYY-MM-DD HH:MM:SS';

-- Rollback synced tables (if needed)
-- Use backup reference from migration script
```

---

## Maintenance Windows

### Monthly Tasks
- [ ] Review and clear old audit logs (> 90 days)
- [ ] Rotate secrets (before expiry)
- [ ] Review and optimize retry queue
- [ ] Update dependencies (security patches)

### Quarterly Tasks
- [ ] Review and optimize Snowflake warehouse sizing
- [ ] Analyze sync patterns and adjust polling intervals
- [ ] Review and update alert thresholds
- [ ] Capacity planning review

---

## Support Contacts

| Issue Type | Contact | SLA |
|------------|---------|-----|
| CRITICAL (Checksum mismatch, >5% failure) | On-call | 15 min |
| HIGH (Service down, >1% failure) | Platform team | 1 hour |
| MEDIUM (Individual sync failures) | Support ticket | 4 hours |
| LOW (Questions, enhancements) | Backlog | Best effort |

---

**Deployment Date**: _______________  
**Deployed By**: _______________  
**Approved By**: _______________  
**Version**: 1.0.0
