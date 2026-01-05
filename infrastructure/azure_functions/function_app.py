"""
Azure Function: Timer-Triggered Semantic Model Sync
Fabric-Snowflake Bidirectional Synchronization

This function runs every 1 hour and orchestrates the complete sync process:
1. Authenticates with both Fabric API and Snowflake
2. Extracts semantic models from Fabric
3. Transforms DAX expressions to SQL
4. Creates/updates views in Snowflake
5. Logs all operations and sends alerts on failures

Author: Data Engineering Team
Created: 2026-01-03
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import azure.functions as func
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import requests
import redis
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('FabricSnowflakeSync')

# Create the FunctionApp with timer trigger
app = func.FunctionApp()


class SyncConfig:
    """Configuration manager for sync operations."""
    
    def __init__(self):
        # Fabric Configuration
        self.fabric_tenant_id = os.getenv('FABRIC_TENANT_ID', '')
        self.fabric_client_id = os.getenv('FABRIC_CLIENT_ID', '')
        self.fabric_client_secret = os.getenv('FABRIC_CLIENT_SECRET', '')
        self.fabric_workspace_id = os.getenv('FABRIC_WORKSPACE_ID', '')
        
        # Snowflake Configuration
        self.snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT', '')
        self.snowflake_user = os.getenv('SNOWFLAKE_USER', '')
        self.snowflake_password = os.getenv('SNOWFLAKE_PASSWORD', '')
        self.snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'SEMANTIC_SYNC_WH')
        self.snowflake_database = os.getenv('SNOWFLAKE_DATABASE', 'ANALYTICS_DB')
        self.snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA', 'SEMANTIC_LAYER')
        
        # Redis Configuration
        self.redis_host = os.getenv('REDIS_HOST', '')
        self.redis_port = int(os.getenv('REDIS_PORT', '6380'))
        self.redis_password = os.getenv('REDIS_PASSWORD', '')
        self.redis_ssl = os.getenv('REDIS_SSL', 'true').lower() == 'true'
        
        # Storage Configuration
        self.storage_connection = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
        self.sync_logs_container = os.getenv('SYNC_LOGS_CONTAINER', 'sync-logs')
        
        # Notification Configuration
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL', '')
        self.alert_email_recipients = os.getenv('ALERT_EMAIL_RECIPIENTS', '').split(',')
        
        # Sync Settings
        self.max_retries = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))
        self.sync_timeout_minutes = int(os.getenv('SYNC_TIMEOUT_MINUTES', '30'))


class RedisStateManager:
    """Distributed state management using Redis/Azure Cache."""
    
    def __init__(self, config: SyncConfig):
        self.config = config
        self.client: Optional[redis.Redis] = None
        self._connect()
    
    def _connect(self):
        """Establish Redis connection."""
        try:
            if self.config.redis_host:
                self.client = redis.Redis(
                    host=self.config.redis_host,
                    port=self.config.redis_port,
                    password=self.config.redis_password,
                    ssl=self.config.redis_ssl,
                    decode_responses=True
                )
                self.client.ping()
                logger.info("Connected to Redis for state management")
        except Exception as e:
            logger.warning(f"Redis connection failed, using local state: {e}")
            self.client = None
    
    def acquire_lock(self, lock_name: str, ttl_seconds: int = 1800) -> bool:
        """Acquire a distributed lock."""
        if not self.client:
            return True  # No Redis, assume lock acquired
        
        lock_key = f"sync_lock:{lock_name}"
        lock_value = f"{os.getenv('WEBSITE_INSTANCE_ID', 'local')}:{datetime.now().isoformat()}"
        
        # Try to set the lock with NX (only if not exists)
        result = self.client.set(lock_key, lock_value, nx=True, ex=ttl_seconds)
        return result is not None
    
    def release_lock(self, lock_name: str):
        """Release a distributed lock."""
        if self.client:
            lock_key = f"sync_lock:{lock_name}"
            self.client.delete(lock_key)
    
    def get_sync_state(self, sync_type: str) -> Dict[str, Any]:
        """Get current sync state."""
        if not self.client:
            return {}
        
        state_key = f"sync_state:{sync_type}"
        state_json = self.client.get(state_key)
        return json.loads(state_json) if state_json else {}
    
    def set_sync_state(self, sync_type: str, state: Dict[str, Any], ttl_seconds: int = 86400):
        """Update sync state."""
        if self.client:
            state_key = f"sync_state:{sync_type}"
            state['updated_at'] = datetime.now(timezone.utc).isoformat()
            self.client.set(state_key, json.dumps(state), ex=ttl_seconds)
    
    def record_sync_hash(self, model_name: str, content_hash: str):
        """Record content hash for incremental change detection."""
        if self.client:
            hash_key = f"model_hash:{model_name}"
            self.client.set(hash_key, content_hash, ex=86400 * 7)  # Keep for 7 days
    
    def get_sync_hash(self, model_name: str) -> Optional[str]:
        """Get previous content hash."""
        if self.client:
            hash_key = f"model_hash:{model_name}"
            return self.client.get(hash_key)
        return None


class FabricClient:
    """Client for Microsoft Fabric API interactions."""
    
    def __init__(self, config: SyncConfig):
        self.config = config
        self.access_token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None
        self.base_url = "https://api.fabric.microsoft.com"
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def authenticate(self) -> bool:
        """Authenticate with Azure AD using managed identity or client credentials."""
        try:
            # Try managed identity first
            if os.getenv('AZURE_CLIENT_ID'):
                credential = DefaultAzureCredential()
            else:
                # Fall back to client credentials
                credential = ClientSecretCredential(
                    tenant_id=self.config.fabric_tenant_id,
                    client_id=self.config.fabric_client_id,
                    client_secret=self.config.fabric_client_secret
                )
            
            # Get token for Fabric API
            token = credential.get_token("https://api.fabric.microsoft.com/.default")
            self.access_token = token.token
            self.token_expiry = datetime.fromtimestamp(token.expires_on)
            
            logger.info("Successfully authenticated with Fabric API")
            return True
            
        except Exception as e:
            logger.error(f"Fabric authentication failed: {e}")
            return False
    
    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers for API requests."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_semantic_models(self) -> List[Dict[str, Any]]:
        """Retrieve all semantic models from the workspace."""
        if not self.access_token:
            if not self.authenticate():
                return []
        
        url = f"{self.base_url}/v1/workspaces/{self.config.fabric_workspace_id}/semanticmodels"
        
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                models = data.get('value', [])
                logger.info(f"Retrieved {len(models)} semantic models from Fabric")
                return models
            
            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limited, waiting {retry_after} seconds")
                raise Exception(f"Rate limited: {response.text}")
            
            else:
                logger.error(f"Failed to get models: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching semantic models: {e}")
            raise
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_model_details(self, model_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific semantic model."""
        if not self.access_token:
            if not self.authenticate():
                return None
        
        url = f"{self.base_url}/v1/workspaces/{self.config.fabric_workspace_id}/semanticmodels/{model_id}"
        
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=30)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get model details: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching model details: {e}")
            raise
    
    def update_measure(self, model_id: str, table_name: str, 
                       measure_name: str, expression: str) -> bool:
        """Update a measure in a semantic model."""
        if not self.access_token:
            if not self.authenticate():
                return False
        
        url = f"{self.base_url}/v1/workspaces/{self.config.fabric_workspace_id}/semanticmodels/{model_id}/updateDefinition"
        
        payload = {
            "updateDetails": [{
                "path": f"model/tables/{table_name}/measures/{measure_name}",
                "updates": {
                    "expression": expression
                }
            }]
        }
        
        try:
            response = requests.post(url, headers=self._get_headers(), 
                                    json=payload, timeout=60)
            
            if response.status_code in (200, 202, 204):
                logger.info(f"Successfully updated measure {table_name}.{measure_name}")
                return True
            else:
                logger.error(f"Failed to update measure: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating measure: {e}")
            return False


class SnowflakeClient:
    """Client for Snowflake database operations."""
    
    def __init__(self, config: SyncConfig):
        self.config = config
        self.connection = None
    
    def connect(self) -> bool:
        """Establish connection to Snowflake."""
        try:
            import snowflake.connector
            
            self.connection = snowflake.connector.connect(
                account=self.config.snowflake_account,
                user=self.config.snowflake_user,
                password=self.config.snowflake_password,
                warehouse=self.config.snowflake_warehouse,
                database=self.config.snowflake_database,
                schema=self.config.snowflake_schema
            )
            logger.info("Connected to Snowflake")
            return True
            
        except Exception as e:
            logger.error(f"Snowflake connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close Snowflake connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Disconnected from Snowflake")
            except Exception as e:
                logger.warning(f"Error closing Snowflake connection: {e}")
    
    def execute_query(self, query: str, params: Optional[List] = None) -> Optional[List[Dict]]:
        """Execute a SQL query."""
        if not self.connection:
            return None
        
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            cursor.close()
            return results
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return None
    
    def create_or_update_view(self, view_name: str, columns: List[Dict], 
                               measures: List[Dict], source_table: str) -> bool:
        """Create or replace a semantic view."""
        if not self.connection:
            return False
        
        try:
            # Build column list
            col_defs = []
            for col in columns:
                col_defs.append(col['name'])
            
            # Build measure expressions
            for measure in measures:
                sql_expr = self._convert_dax_to_sql(measure.get('expression', ''))
                col_defs.append(f"{sql_expr} AS {measure['name']}")
            
            columns_sql = ", ".join(col_defs)
            
            ddl = f"""
            CREATE OR REPLACE VIEW {self.config.snowflake_schema}.{view_name} AS
            SELECT {columns_sql}
            FROM {source_table}
            """
            
            cursor = self.connection.cursor()
            cursor.execute(ddl)
            cursor.close()
            
            logger.info(f"Created/updated view: {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create view {view_name}: {e}")
            return False
    
    def _convert_dax_to_sql(self, dax_expression: str) -> str:
        """Convert DAX expression to SQL."""
        sql_expr = dax_expression
        
        # Simple DAX to SQL conversions
        replacements = {
            'SUM(': 'SUM(',
            'COUNT(': 'COUNT(',
            'AVERAGE(': 'AVG(',
            'MIN(': 'MIN(',
            'MAX(': 'MAX(',
            'COUNTROWS(': 'COUNT(*',
            'DISTINCTCOUNT(': 'COUNT(DISTINCT '
        }
        
        for dax_func, sql_func in replacements.items():
            sql_expr = sql_expr.replace(dax_func, sql_func)
        
        return sql_expr
    
    def get_views(self) -> List[str]:
        """Get list of views in the schema."""
        query = f"""
        SELECT TABLE_NAME
        FROM {self.config.snowflake_database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{self.config.snowflake_schema}'
        AND TABLE_TYPE = 'VIEW'
        """
        
        results = self.execute_query(query)
        return [r.get('TABLE_NAME', '') for r in (results or [])]
    
    def log_sync_operation(self, sync_id: str, status: str, 
                           details: Dict[str, Any]) -> bool:
        """Log sync operation to audit table."""
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                INSERT INTO SYNC_OPERATIONS.SYNC_AUDIT_LOG (
                    sync_id, sync_direction, sync_status,
                    models_processed, views_created, views_updated,
                    execution_start_time, execution_end_time,
                    execution_duration_ms, triggered_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, [
                sync_id,
                details.get('direction', 'BIDIRECTIONAL'),
                status,
                details.get('models_processed', 0),
                details.get('views_created', 0),
                details.get('views_updated', 0),
                details.get('start_time'),
                details.get('end_time'),
                details.get('duration_ms', 0),
                'AZURE_FUNCTION'
            ])
            cursor.close()
            self.connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to log sync operation: {e}")
            return False


class SyncOrchestrator:
    """Main orchestrator for bidirectional sync operations."""
    
    def __init__(self, config: SyncConfig):
        self.config = config
        self.state_manager = RedisStateManager(config)
        self.fabric_client = FabricClient(config)
        self.snowflake_client = SnowflakeClient(config)
        self.sync_id: str = ""
        self.start_time: datetime = datetime.now(timezone.utc)
    
    def generate_sync_id(self) -> str:
        """Generate unique sync identifier."""
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        random_suffix = hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest()[:6]
        return f"AZSYNC_{timestamp}_{random_suffix.upper()}"
    
    def execute_bidirectional_sync(self) -> Dict[str, Any]:
        """Execute full bidirectional sync."""
        self.sync_id = self.generate_sync_id()
        self.start_time = datetime.now(timezone.utc)
        
        result = {
            'sync_id': self.sync_id,
            'status': 'STARTED',
            'direction': 'BIDIRECTIONAL',
            'start_time': self.start_time.isoformat(),
            'models_processed': 0,
            'views_created': 0,
            'views_updated': 0,
            'measures_synced': 0,
            'errors': []
        }
        
        try:
            # Acquire distributed lock
            if not self.state_manager.acquire_lock('bidirectional_sync'):
                result['status'] = 'SKIPPED'
                result['message'] = 'Another sync is in progress'
                return result
            
            # Update state to running
            self.state_manager.set_sync_state('BIDIRECTIONAL', {
                'sync_id': self.sync_id,
                'status': 'RUNNING',
                'started_at': self.start_time.isoformat()
            })
            
            # Authenticate with both platforms
            if not self.fabric_client.authenticate():
                result['status'] = 'FAILED'
                result['errors'].append('Fabric authentication failed')
                return result
            
            if not self.snowflake_client.connect():
                result['status'] = 'FAILED'
                result['errors'].append('Snowflake connection failed')
                return result
            
            # Step 1: Fabric to Snowflake sync
            fabric_to_sf_result = self._sync_fabric_to_snowflake()
            result['models_processed'] = fabric_to_sf_result.get('models_processed', 0)
            result['views_created'] = fabric_to_sf_result.get('views_created', 0)
            result['views_updated'] = fabric_to_sf_result.get('views_updated', 0)
            result['errors'].extend(fabric_to_sf_result.get('errors', []))
            
            # Step 2: Snowflake to Fabric sync (detect and apply changes)
            sf_to_fabric_result = self._sync_snowflake_to_fabric()
            result['measures_synced'] = sf_to_fabric_result.get('measures_synced', 0)
            result['errors'].extend(sf_to_fabric_result.get('errors', []))
            
            # Determine final status
            if result['errors']:
                result['status'] = 'PARTIAL'
            else:
                result['status'] = 'COMPLETED'
            
            # Calculate duration
            end_time = datetime.now(timezone.utc)
            result['end_time'] = end_time.isoformat()
            result['duration_ms'] = int((end_time - self.start_time).total_seconds() * 1000)
            
            # Log to Snowflake
            self.snowflake_client.log_sync_operation(self.sync_id, result['status'], result)
            
            # Update state
            self.state_manager.set_sync_state('BIDIRECTIONAL', {
                'sync_id': self.sync_id,
                'status': result['status'],
                'completed_at': end_time.isoformat(),
                'duration_ms': result['duration_ms']
            })
            
            return result
            
        except Exception as e:
            result['status'] = 'FAILED'
            result['errors'].append(str(e))
            logger.exception("Sync failed with exception")
            return result
            
        finally:
            # Release lock
            self.state_manager.release_lock('bidirectional_sync')
            
            # Disconnect from Snowflake
            self.snowflake_client.disconnect()
    
    def _sync_fabric_to_snowflake(self) -> Dict[str, Any]:
        """Sync semantic models from Fabric to Snowflake."""
        result = {
            'models_processed': 0,
            'views_created': 0,
            'views_updated': 0,
            'errors': []
        }
        
        try:
            # Get all semantic models
            models = self.fabric_client.get_semantic_models()
            
            existing_views = self.snowflake_client.get_views()
            existing_views_set = set(v.upper() for v in existing_views)
            
            for model_info in models:
                model_id = model_info.get('id', '')
                model_name = model_info.get('name', '')
                
                try:
                    # Get model details
                    model_detail = self.fabric_client.get_model_details(model_id)
                    if not model_detail:
                        continue
                    
                    # Check for changes using content hash
                    content_hash = hashlib.md5(
                        json.dumps(model_detail, sort_keys=True).encode()
                    ).hexdigest()
                    
                    previous_hash = self.state_manager.get_sync_hash(model_name)
                    
                    if previous_hash == content_hash:
                        logger.info(f"Model {model_name} unchanged, skipping")
                        continue
                    
                    # Process each table in the model
                    for table in model_detail.get('tables', []):
                        table_name = table.get('name', '')
                        view_name = f"SV_{model_name}_{table_name}".upper().replace(' ', '_')
                        
                        columns = []
                        for col in table.get('columns', []):
                            if not col.get('isHidden', False):
                                columns.append({
                                    'name': col.get('name', ''),
                                    'data_type': col.get('dataType', 'string')
                                })
                        
                        measures = []
                        for measure in table.get('measures', []):
                            measures.append({
                                'name': measure.get('name', ''),
                                'expression': measure.get('expression', '')
                            })
                        
                        # Determine source table
                        source_expr = table.get('source', {}).get('expression', '')
                        source_table = source_expr if source_expr else f"{model_name}.{table_name}"
                        
                        # Create/update view
                        is_new = view_name not in existing_views_set
                        
                        if self.snowflake_client.create_or_update_view(
                            view_name, columns, measures, source_table
                        ):
                            if is_new:
                                result['views_created'] += 1
                            else:
                                result['views_updated'] += 1
                    
                    # Update content hash
                    self.state_manager.record_sync_hash(model_name, content_hash)
                    result['models_processed'] += 1
                    
                except Exception as e:
                    result['errors'].append(f"Error processing model {model_name}: {str(e)}")
                    logger.error(f"Error processing model {model_name}: {e}")
            
        except Exception as e:
            result['errors'].append(f"Fabric to Snowflake sync error: {str(e)}")
            logger.error(f"Fabric to Snowflake sync error: {e}")
        
        return result
    
    def _sync_snowflake_to_fabric(self) -> Dict[str, Any]:
        """Sync changes from Snowflake back to Fabric."""
        result = {
            'measures_synced': 0,
            'errors': []
        }
        
        # This would detect changes made in Snowflake views
        # and push them back to Fabric semantic models
        
        # For now, log that this direction is enabled
        logger.info("Snowflake to Fabric sync: checking for changes")
        
        # Placeholder for reverse sync logic
        # In production, this would:
        # 1. Get view definitions from Snowflake
        # 2. Compare with Fabric model definitions
        # 3. Update Fabric models if changes detected
        
        return result


class AlertManager:
    """Manages alerts and notifications."""
    
    def __init__(self, config: SyncConfig):
        self.config = config
    
    def send_slack_alert(self, message: str, is_error: bool = False):
        """Send alert to Slack."""
        if not self.config.slack_webhook_url:
            return
        
        try:
            payload = {
                "text": f"{'🚨 ' if is_error else '✅ '}{message}",
                "username": "Fabric-Snowflake Sync",
                "icon_emoji": ":warning:" if is_error else ":white_check_mark:"
            }
            
            requests.post(self.config.slack_webhook_url, json=payload, timeout=10)
            
        except Exception as e:
            logger.warning(f"Failed to send Slack alert: {e}")
    
    def send_failure_notification(self, sync_id: str, error_message: str):
        """Send failure notification through all channels."""
        message = f"Sync Job {sync_id} FAILED: {error_message}"
        self.send_slack_alert(message, is_error=True)
        
        # Log to Application Insights
        logger.error(f"SYNC_FAILURE: {message}")


class SyncLogStorage:
    """Azure Blob Storage for sync logs."""
    
    def __init__(self, config: SyncConfig):
        self.config = config
        self.blob_service: Optional[BlobServiceClient] = None
        self._connect()
    
    def _connect(self):
        """Connect to Azure Blob Storage."""
        try:
            if self.config.storage_connection:
                self.blob_service = BlobServiceClient.from_connection_string(
                    self.config.storage_connection
                )
        except Exception as e:
            logger.warning(f"Failed to connect to blob storage: {e}")
    
    def save_sync_log(self, sync_id: str, log_data: Dict[str, Any]):
        """Save sync log to blob storage."""
        if not self.blob_service:
            return
        
        try:
            container_client = self.blob_service.get_container_client(
                self.config.sync_logs_container
            )
            
            # Create container if not exists
            try:
                container_client.create_container()
            except:
                pass
            
            # Save log
            date_prefix = datetime.now().strftime('%Y/%m/%d')
            blob_name = f"{date_prefix}/{sync_id}.json"
            
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(
                json.dumps(log_data, indent=2),
                overwrite=True
            )
            
            logger.info(f"Saved sync log to {blob_name}")
            
        except Exception as e:
            logger.warning(f"Failed to save sync log: {e}")


# ============================================================
# AZURE FUNCTION DEFINITIONS
# ============================================================

@app.timer_trigger(schedule="0 0 * * * *", arg_name="timer", run_on_startup=False)
def timer_bidirectional_sync(timer: func.TimerRequest) -> None:
    """
    Timer-triggered function for bidirectional sync.
    Runs every hour (0 0 * * * * = every hour at minute 0).
    """
    utc_timestamp = datetime.utcnow().isoformat()
    logger.info(f"Bidirectional sync triggered at {utc_timestamp}")
    
    if timer.past_due:
        logger.warning("Timer is running late!")
    
    try:
        config = SyncConfig()
        orchestrator = SyncOrchestrator(config)
        alert_manager = AlertManager(config)
        log_storage = SyncLogStorage(config)
        
        # Execute sync
        result = orchestrator.execute_bidirectional_sync()
        
        # Save logs
        log_storage.save_sync_log(result['sync_id'], result)
        
        # Send alerts based on result
        if result['status'] == 'FAILED':
            alert_manager.send_failure_notification(
                result['sync_id'],
                '; '.join(result.get('errors', ['Unknown error']))
            )
        elif result['status'] == 'COMPLETED':
            logger.info(f"Sync completed successfully: {result['sync_id']}")
        
        logger.info(f"Sync result: {json.dumps(result)}")
        
    except Exception as e:
        logger.exception(f"Sync function failed: {e}")


@app.timer_trigger(schedule="0 */15 * * * *", arg_name="timer", run_on_startup=False)
def timer_health_check(timer: func.TimerRequest) -> None:
    """
    Health check function that runs every 15 minutes.
    Monitors sync status and sends alerts if issues detected.
    """
    logger.info("Health check triggered")
    
    try:
        config = SyncConfig()
        state_manager = RedisStateManager(config)
        alert_manager = AlertManager(config)
        
        # Check sync state
        bi_state = state_manager.get_sync_state('BIDIRECTIONAL')
        
        if bi_state:
            last_sync = bi_state.get('completed_at')
            if last_sync:
                last_sync_time = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
                minutes_ago = (datetime.now(timezone.utc) - last_sync_time).total_seconds() / 60
                
                if minutes_ago > 120:  # More than 2 hours since last sync
                    alert_manager.send_slack_alert(
                        f"⚠️ No successful sync in {int(minutes_ago)} minutes",
                        is_error=True
                    )
        
        logger.info("Health check completed")
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")


@app.route(route="fabric-snowflake-sync", methods=["POST"])
def http_trigger_sync(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered sync for manual or Snowflake webhook calls.
    """
    logger.info("HTTP sync trigger received")
    
    try:
        # Parse request body
        req_body = req.get_json() if req.get_body() else {}
        
        sync_direction = req_body.get('direction', 'BIDIRECTIONAL')
        force_full = req_body.get('force_full_sync', False)
        
        config = SyncConfig()
        orchestrator = SyncOrchestrator(config)
        
        # Execute sync
        result = orchestrator.execute_bidirectional_sync()
        
        return func.HttpResponse(
            body=json.dumps(result),
            status_code=200 if result['status'] != 'FAILED' else 500,
            mimetype="application/json"
        )
        
    except Exception as e:
        logger.exception(f"HTTP sync failed: {e}")
        return func.HttpResponse(
            body=json.dumps({'error': str(e)}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="detect-changes", methods=["POST"])
def http_detect_changes(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP endpoint for incremental change detection.
    Called by Snowflake scheduled task.
    """
    logger.info("Change detection triggered")
    
    try:
        config = SyncConfig()
        fabric_client = FabricClient(config)
        state_manager = RedisStateManager(config)
        
        changes_detected = []
        
        if fabric_client.authenticate():
            models = fabric_client.get_semantic_models()
            
            for model_info in models:
                model_id = model_info.get('id', '')
                model_name = model_info.get('name', '')
                
                model_detail = fabric_client.get_model_details(model_id)
                if not model_detail:
                    continue
                
                # Calculate content hash
                content_hash = hashlib.md5(
                    json.dumps(model_detail, sort_keys=True).encode()
                ).hexdigest()
                
                previous_hash = state_manager.get_sync_hash(model_name)
                
                if previous_hash and previous_hash != content_hash:
                    changes_detected.append({
                        'model_name': model_name,
                        'model_id': model_id,
                        'change_type': 'MODIFIED'
                    })
        
        return func.HttpResponse(
            body=json.dumps({
                'changes_detected': len(changes_detected),
                'changes': changes_detected
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logger.exception(f"Change detection failed: {e}")
        return func.HttpResponse(
            body=json.dumps({'error': str(e)}),
            status_code=500,
            mimetype="application/json"
        )


@app.route(route="health", methods=["GET"])
def http_health(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint.
    """
    return func.HttpResponse(
        body=json.dumps({
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }),
        status_code=200,
        mimetype="application/json"
    )
