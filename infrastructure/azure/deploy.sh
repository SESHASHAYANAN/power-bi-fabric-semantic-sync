#!/bin/bash
# =============================================================================
# Fabric-Snowflake Sync - Azure Infrastructure Deployment Script
# =============================================================================
# This script deploys all Azure resources required for the sync automation
# =============================================================================

set -e

# Configuration
RESOURCE_GROUP="${RESOURCE_GROUP:-fabric-snowflake-sync-rg}"
LOCATION="${LOCATION:-eastus}"
DEPLOYMENT_NAME="fabric-sync-$(date +%Y%m%d%H%M%S)"

echo "============================================================"
echo "Fabric-Snowflake Sync - Azure Infrastructure Deployment"
echo "============================================================"
echo ""
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo "Deployment: $DEPLOYMENT_NAME"
echo ""

# Check if logged in to Azure
echo "Checking Azure CLI login status..."
if ! az account show > /dev/null 2>&1; then
    echo "Not logged in to Azure. Please run 'az login' first."
    exit 1
fi

echo "Logged in as: $(az account show --query user.name -o tsv)"
echo "Subscription: $(az account show --query name -o tsv)"
echo ""

# Create resource group if it doesn't exist
echo "Creating resource group if not exists..."
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --output none

echo "Resource group ready."
echo ""

# Deploy ARM template
echo "Deploying Azure resources..."
echo "This may take 5-10 minutes..."
echo ""

DEPLOYMENT_OUTPUT=$(az deployment group create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$DEPLOYMENT_NAME" \
    --template-file azuredeploy.json \
    --parameters @azuredeploy.parameters.json \
    --output json)

echo ""
echo "Deployment completed successfully!"
echo ""

# Extract outputs
FUNCTION_APP_URL=$(echo $DEPLOYMENT_OUTPUT | jq -r '.properties.outputs.functionAppUrl.value')
FUNCTION_PRINCIPAL_ID=$(echo $DEPLOYMENT_OUTPUT | jq -r '.properties.outputs.functionAppPrincipalId.value')
REDIS_HOST=$(echo $DEPLOYMENT_OUTPUT | jq -r '.properties.outputs.redisHostName.value')
APP_INSIGHTS_KEY=$(echo $DEPLOYMENT_OUTPUT | jq -r '.properties.outputs.appInsightsInstrumentationKey.value')
STORAGE_ACCOUNT=$(echo $DEPLOYMENT_OUTPUT | jq -r '.properties.outputs.storageAccountName.value')

echo "============================================================"
echo "Deployment Outputs"
echo "============================================================"
echo ""
echo "Function App URL: $FUNCTION_APP_URL"
echo "Function Principal ID: $FUNCTION_PRINCIPAL_ID"
echo "Redis Host: $REDIS_HOST"
echo "App Insights Key: $APP_INSIGHTS_KEY"
echo "Storage Account: $STORAGE_ACCOUNT"
echo ""

# Deploy function code
echo "============================================================"
echo "Deploying Function App Code"
echo "============================================================"
echo ""

FUNCTION_APP_NAME=$(echo $FUNCTION_APP_URL | sed 's|https://||' | sed 's|.azurewebsites.net||')

echo "Function App Name: $FUNCTION_APP_NAME"
echo ""

# Get publish profile
echo "Getting publish profile..."
PUBLISH_PROFILE=$(az functionapp deployment list-publishing-profiles \
    --name "$FUNCTION_APP_NAME" \
    --resource-group "$RESOURCE_GROUP" \
    --output json)

echo "Deploying function code (using Azure Functions Core Tools)..."
cd ../azure_functions

# Install dependencies
pip install -r requirements.txt --target=".python_packages/lib/site-packages"

# Package and deploy
func azure functionapp publish "$FUNCTION_APP_NAME" --python

echo ""
echo "Function code deployed successfully!"
echo ""

# Verify deployment
echo "============================================================"
echo "Verifying Deployment"
echo "============================================================"
echo ""

echo "Checking function app health..."
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$FUNCTION_APP_URL/api/health")

if [ "$HTTP_STATUS" == "200" ]; then
    echo "✅ Function app is healthy!"
else
    echo "⚠️  Function app returned status: $HTTP_STATUS"
    echo "    The function may still be warming up. Try again in a few minutes."
fi

echo ""
echo "============================================================"
echo "Deployment Complete!"
echo "============================================================"
echo ""
echo "Next Steps:"
echo ""
echo "1. Deploy Snowflake infrastructure:"
echo "   snowsql -f ../snowflake/01_create_audit_tables.sql"
echo "   snowsql -f ../snowflake/02_create_stored_procedures.sql"
echo "   snowsql -f ../snowflake/03_create_scheduled_tasks.sql"
echo ""
echo "2. Update Snowflake tasks with Azure Function URL:"
echo "   Replace 'https://your-azure-function-app.azurewebsites.net'"
echo "   with: $FUNCTION_APP_URL"
echo ""
echo "3. Start the monitoring dashboard:"
echo "   cd ../monitoring && streamlit run monitoring_dashboard.py"
echo ""
echo "4. Verify sync by triggering manually:"
echo "   curl -X POST '$FUNCTION_APP_URL/api/fabric-snowflake-sync'"
echo ""
echo "============================================================"
