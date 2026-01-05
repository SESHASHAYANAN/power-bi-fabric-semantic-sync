# Fabric-Snowflake Sync Background Automation
# Infrastructure Deployment Guide

## Overview

This guide covers the complete deployment of the background automation system for synchronizing semantic models between Microsoft Fabric and Snowflake.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     FABRIC-SNOWFLAKE SYNC ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────┐                    ┌──────────────────┐               │
│  │  Microsoft       │                    │    Snowflake     │               │
│  │  Fabric          │                    │    Database      │               │
│  │  (Semantic       │◄──────────────────►│    (Views &      │               │
│  │   Models)        │                    │     Tasks)       │               │
│  └────────┬─────────┘                    └────────┬─────────┘               │
│           │                                       │                          │
│           │  REST API                             │  SQL/Stored Procedures   │
│           ▼                                       ▼                          │
│  ┌───────────────────────────────────────────────────────────┐              │
│  │              Azure Functions (Timer Triggered)             │              │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │              │
│  │  │ Hourly Sync │  │ Health Check│  │ Change      │        │              │
│  │  │ (Bidirect.) │  │ (15 min)    │  │ Detection   │        │              │
│  │  └─────────────┘  └─────────────┘  └─────────────┘        │              │
│  └───────────────────────────┬───────────────────────────────┘              │
│                              │                                               │
│                              ▼                                               │
│  ┌───────────────────────────────────────────────────────────┐              │
│  │           Redis / Azure Cache (Shared State)               │              │
│  │  • Distributed Locks                                       │              │
│  │  • Sync State                                              │              │
│  │  • Content Hashes (Change Detection)                       │              │
│  └───────────────────────────────────────────────────────────┘              │
│                              │                                               │
│                              ▼                                               │
│  ┌───────────────────────────────────────────────────────────┐              │
│  │              Monitoring & Alerting                         │              │
│  │  • Application Insights                                    │              │
│  │  • Slack/Teams Notifications                               │              │
│  │  • Azure Storage (Audit Logs)                              │              │
│  │  • Streamlit Dashboard                                     │              │
│  └───────────────────────────────────────────────────────────┘              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Azure Subscription with:
  - Azure Functions Premium or Consumption plan
  - Azure Cache for Redis
  - Azure Storage Account
  - Application Insights
  
- Snowflake Account with:
  - ACCOUNTADMIN or equivalent privileges
  - External Network Access configured
  
- Microsoft Fabric with:
  - Service Principal with appropriate permissions
  - Workspace access

---

## Deployment Steps

### 1. Snowflake Infrastructure

Execute the SQL scripts in order:

```bash
# Connect to Snowflake and run:
snowsql -a <account> -u <user> -f infrastructure/snowflake/01_create_audit_tables.sql
snowsql -a <account> -u <user> -f infrastructure/snowflake/02_create_stored_procedures.sql
snowsql -a <account> -u <user> -f infrastructure/snowflake/03_create_scheduled_tasks.sql
```

Key configurations to update:
- Replace `<your-azure-function-app>` with your actual Azure Function URL
- Configure email notification integration
- Adjust schedule timing as needed

### 2. Azure Function Deployment

```bash
# Navigate to azure_functions directory
cd infrastructure/azure_functions

# Create Azure Function App
az functionapp create \
  --resource-group <resource-group> \
  --consumption-plan-location <location> \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4 \
  --name <function-app-name> \
  --storage-account <storage-account>

# Enable managed identity
az functionapp identity assign --name <function-app-name> --resource-group <resource-group>

# Configure app settings
az functionapp config appsettings set \
  --name <function-app-name> \
  --resource-group <resource-group> \
  --settings \
    FABRIC_TENANT_ID=<tenant-id> \
    FABRIC_CLIENT_ID=<client-id> \
    FABRIC_CLIENT_SECRET=<client-secret> \
    FABRIC_WORKSPACE_ID=<workspace-id> \
    SNOWFLAKE_ACCOUNT=<snowflake-account> \
    SNOWFLAKE_USER=<snowflake-user> \
    SNOWFLAKE_PASSWORD=<snowflake-password> \
    SNOWFLAKE_WAREHOUSE=SEMANTIC_SYNC_WH \
    SNOWFLAKE_DATABASE=ANALYTICS_DB \
    SNOWFLAKE_SCHEMA=SEMANTIC_LAYER \
    REDIS_HOST=<redis-host>.redis.cache.windows.net \
    REDIS_PORT=6380 \
    REDIS_PASSWORD=<redis-password> \
    REDIS_SSL=true \
    SLACK_WEBHOOK_URL=<slack-webhook>

# Deploy function
func azure functionapp publish <function-app-name>
```

### 3. Redis Cache Setup

```bash
# Create Azure Cache for Redis
az redis create \
  --name <redis-name> \
  --resource-group <resource-group> \
  --location <location> \
  --sku Basic \
  --vm-size c0

# Get connection details
az redis show --name <redis-name> --resource-group <resource-group>
az redis list-keys --name <redis-name> --resource-group <resource-group>
```

### 4. Application Insights

```bash
# Create Application Insights
az monitor app-insights component create \
  --app <app-insights-name> \
  --location <location> \
  --resource-group <resource-group> \
  --application-type web

# Get instrumentation key
az monitor app-insights component show \
  --app <app-insights-name> \
  --resource-group <resource-group> \
  --query instrumentationKey
```

---

## Configuration Reference

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `FABRIC_TENANT_ID` | Azure AD tenant ID | Yes |
| `FABRIC_CLIENT_ID` | Service principal client ID | Yes |
| `FABRIC_CLIENT_SECRET` | Service principal secret | Yes |
| `FABRIC_WORKSPACE_ID` | Target Fabric workspace | Yes |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | Yes |
| `SNOWFLAKE_USER` | Snowflake username | Yes |
| `SNOWFLAKE_PASSWORD` | Snowflake password | Yes |
| `SNOWFLAKE_WAREHOUSE` | Dedicated sync warehouse | Yes |
| `SNOWFLAKE_DATABASE` | Target database | Yes |
| `SNOWFLAKE_SCHEMA` | Target schema | Yes |
| `REDIS_HOST` | Redis hostname | Yes |
| `REDIS_PORT` | Redis port (default: 6380) | Yes |
| `REDIS_PASSWORD` | Redis access key | Yes |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook | No |
| `ALERT_EMAIL_RECIPIENTS` | Comma-separated emails | No |

### Sync Schedules

| Task | Schedule | Description |
|------|----------|-------------|
| Bidirectional Sync | Every 1 hour | Main sync at :00 |
| Fabric to SF Full | 2 AM UTC daily | Off-peak full sync |
| SF to Fabric Full | 3 AM UTC daily | Off-peak full sync |
| Health Check | Every 15 min | System health monitoring |
| Change Detection | Every 1 hour | Incremental at :30 |
| Cleanup | 4 AM UTC daily | Remove old records |
| Auto-Retry | Every 15 min | Retry failed syncs |

---

## Monitoring & Alerting

### Alert Conditions

| Condition | Severity | Action |
|-----------|----------|--------|
| 3+ consecutive failures | CRITICAL | Email + Slack + PagerDuty |
| Sync duration > 10 min | HIGH | Slack notification |
| No sync in 2+ hours | MEDIUM | Slack notification |
| Connection timeout | MEDIUM | Log + retry |
| API rate limited | LOW | Wait and retry |

### Dashboard Access

Run the monitoring dashboard:

```bash
cd infrastructure/monitoring
pip install streamlit plotly pandas
streamlit run monitoring_dashboard.py
```

Access at: http://localhost:8501

---

## Troubleshooting

### Common Issues

1. **Authentication Failures**
   - Verify service principal permissions
   - Check client secret expiration
   - Ensure tenant ID is correct

2. **Snowflake Connection Timeout**
   - Verify network access rules
   - Check warehouse is not suspended
   - Confirm account identifier format

3. **Redis Lock Stuck**
   - Health check will auto-release expired locks
   - Manual release: `DELETE sync_lock:*` in Redis

4. **Rate Limiting**
   - Built-in exponential backoff handles this
   - Check API usage in Application Insights

### Log Locations

- **Azure Function Logs**: Application Insights > Logs
- **Snowflake Audit**: `SYNC_OPERATIONS.SYNC_AUDIT_LOG`
- **Error Details**: `SYNC_OPERATIONS.SYNC_ERRORS`
- **Blob Storage**: `sync-logs` container

---

## Security Considerations

1. Use Azure Key Vault for secrets
2. Enable managed identity where possible
3. Implement VNet integration for Azure Functions
4. Configure Snowflake network policies
5. Enable audit logging for all operations
6. Implement least-privilege access

---

## Support

For issues or questions:
- Check monitoring dashboard for real-time status
- Review error logs in Application Insights
- Contact: data-engineering-team@company.com
