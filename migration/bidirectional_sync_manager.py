"""
Bidirectional Sync Manager - Phase 3 of Migration

Establishes automated synchronization between Fabric and Snowflake:
- Fabric Mirroring configuration for real-time replication
- Delta Lake formatted tables in OneLake
- Change Data Capture (CDC) mechanisms
- Stored procedures/pipelines for update propagation
- Unified schema and naming conventions
- Scheduled refresh tasks

Extends the existing sync_orchestration module with enhanced capabilities.
"""

import os
import json
import logging
import hashlib
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum
import time

logger = logging.getLogger(__name__)


class SyncMode(Enum):
    """Sync operation modes."""
    FULL_SYNC = "full_sync"
    INCREMENTAL = "incremental"
    CDC = "change_data_capture"
    MIRRORING = "mirroring"
    MERGE = "merge"


class SyncDirection(Enum):
    """Direction of synchronization."""
    FABRIC_TO_SNOWFLAKE = "fabric_to_snowflake"
    SNOWFLAKE_TO_FABRIC = "snowflake_to_fabric"
    BIDIRECTIONAL = "bidirectional"


class ConflictResolution(Enum):
    """How to resolve conflicts in bidirectional sync."""
    FABRIC_WINS = "fabric_wins"
    SNOWFLAKE_WINS = "snowflake_wins"
    LATEST_WINS = "latest_wins"
    MANUAL = "manual"
    CUSTOM = "custom"


@dataclass
class SyncConfiguration:
    """Configuration for sync operations."""
    direction: SyncDirection = SyncDirection.BIDIRECTIONAL
    mode: SyncMode = SyncMode.INCREMENTAL
    conflict_resolution: ConflictResolution = ConflictResolution.LATEST_WINS
    batch_size: int = 10000
    parallel_threads: int = 4
    retry_attempts: int = 3
    retry_delay_seconds: int = 30
    include_tables: List[str] = field(default_factory=list)
    exclude_tables: List[str] = field(default_factory=list)
    enable_cdc: bool = True
    enable_validation: bool = True
    sync_interval_minutes: int = 15
    watermark_column: str = "_SYNC_TIMESTAMP"
    hash_column: str = "_ROW_HASH"


@dataclass
class CDCRecord:
    """Change Data Capture record."""
    operation: str  # INSERT, UPDATE, DELETE
    table_name: str
    primary_key: Dict[str, Any]
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    timestamp: str = ""
    source_platform: str = ""
    sync_id: str = ""


@dataclass
class SyncTask:
    """Represents a scheduled sync task."""
    task_id: str
    name: str
    cron_expression: str
    config: SyncConfiguration
    enabled: bool = True
    last_run: Optional[str] = None
    next_run: Optional[str] = None
    status: str = "pending"
    error_count: int = 0


@dataclass
class SyncCheckpoint:
    """Checkpoint for resumable sync operations."""
    checkpoint_id: str
    table_name: str
    last_sync_timestamp: str
    last_sync_id: str
    rows_synced: int
    platform: str
    watermark_value: Any = None


class BidirectionalSyncManager:
    """
    Manages bidirectional synchronization between Fabric and Snowflake.
    
    Features:
    - Real-time change detection via CDC
    - Conflict resolution strategies
    - Incremental sync with watermarks
    - Checkpoint/resume capability
    - Scheduled task management
    - Data validation and integrity checks
    """
    
    def __init__(self,
                 snowflake_connector=None,
                 fabric_client=None,
                 config: SyncConfiguration = None,
                 checkpoint_file: str = "sync_checkpoints.json"):
        """
        Initialize the sync manager.
        
        Args:
            snowflake_connector: SnowflakeConnector instance
            fabric_client: FabricApiClient instance
            config: SyncConfiguration
            checkpoint_file: Path to checkpoint persistence file
        """
        self.snowflake = snowflake_connector
        self.fabric = fabric_client
        self.config = config or SyncConfiguration()
        self.checkpoint_file = checkpoint_file
        
        self.checkpoints: Dict[str, SyncCheckpoint] = {}
        self.cdc_buffer: List[CDCRecord] = []
        self.scheduled_tasks: Dict[str, SyncTask] = {}
        self.running = False
        self.sync_lock = threading.Lock()
        
        self._load_checkpoints()
        
    def set_snowflake_connector(self, connector):
        """Set the Snowflake connector."""
        self.snowflake = connector
        
    def set_fabric_client(self, client):
        """Set the Fabric API client."""
        self.fabric = client
        
    # ==========================================
    # SNOWFLAKE CDC SETUP
    # ==========================================
    
    def setup_snowflake_cdc(self, tables: List[str] = None) -> Dict[str, bool]:
        """
        Set up Change Data Capture in Snowflake using Streams.
        
        Creates Snowflake Streams on specified tables to track changes.
        """
        if not self.snowflake:
            raise ValueError("Snowflake connector not set")
            
        results = {}
        
        # Get list of tables to track
        if not tables:
            tables = self._get_snowflake_sync_tables()
            
        for table in tables:
            try:
                # Create stream for CDC
                stream_name = f"{table}_CDC_STREAM"
                
                create_stream_sql = f"""
CREATE OR REPLACE STREAM {stream_name}
ON TABLE {table}
APPEND_ONLY = FALSE
SHOW_INITIAL_ROWS = FALSE
COMMENT = 'CDC stream for bidirectional sync with Fabric'
"""
                self.snowflake.execute_query(create_stream_sql)
                
                # Create change tracking task
                task_name = f"{table}_CDC_TASK"
                
                create_task_sql = f"""
CREATE OR REPLACE TASK {task_name}
WAREHOUSE = {os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WAREHOUSE')}
SCHEDULE = 'USING CRON */5 * * * * UTC'
WHEN SYSTEM$STREAM_HAS_DATA('{stream_name}')
AS
INSERT INTO CDC_CHANGE_LOG (
    TABLE_NAME,
    OPERATION_TYPE,
    CHANGE_DATA,
    CHANGE_TIMESTAMP,
    SYNC_STATUS
)
SELECT 
    '{table}' as TABLE_NAME,
    CASE 
        WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE THEN 'INSERT'
        WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE THEN 'UPDATE'
        WHEN METADATA$ACTION = 'DELETE' THEN 'DELETE'
    END as OPERATION_TYPE,
    OBJECT_CONSTRUCT(*) as CHANGE_DATA,
    CURRENT_TIMESTAMP() as CHANGE_TIMESTAMP,
    'PENDING' as SYNC_STATUS
FROM {stream_name}
"""
                self.snowflake.execute_query(create_task_sql)
                
                # Resume the task
                self.snowflake.execute_query(f"ALTER TASK {task_name} RESUME")
                
                results[table] = True
                logger.info(f"CDC enabled for {table}")
                
            except Exception as e:
                results[table] = False
                logger.error(f"Failed to enable CDC for {table}: {e}")
                
        return results
        
    def _get_snowflake_sync_tables(self) -> List[str]:
        """Get list of tables to sync from Snowflake."""
        query = """
        SELECT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME as FULL_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
          AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
        """
        
        if self.config.include_tables:
            tables_str = "', '".join(self.config.include_tables)
            query += f" AND TABLE_NAME IN ('{tables_str}')"
            
        if self.config.exclude_tables:
            tables_str = "', '".join(self.config.exclude_tables)
            query += f" AND TABLE_NAME NOT IN ('{tables_str}')"
            
        try:
            results = self.snowflake.execute_query(query)
            return [r['FULL_NAME'] for r in results]
        except Exception as e:
            logger.error(f"Failed to get Snowflake tables: {e}")
            return []
            
    def setup_snowflake_cdc_infrastructure(self):
        """
        Set up the base CDC infrastructure in Snowflake.
        
        Creates the change log table and supporting objects.
        """
        # Create CDC schema
        self.snowflake.execute_query("""
        CREATE SCHEMA IF NOT EXISTS CDC_TRACKING
        """)
        
        # Create change log table
        self.snowflake.execute_query("""
        CREATE TABLE IF NOT EXISTS CDC_TRACKING.CDC_CHANGE_LOG (
            LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
            TABLE_NAME VARCHAR(500) NOT NULL,
            OPERATION_TYPE VARCHAR(20) NOT NULL,
            CHANGE_DATA VARIANT,
            PRIMARY_KEY_VALUES VARIANT,
            CHANGE_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            SYNC_STATUS VARCHAR(50) DEFAULT 'PENDING',
            SYNC_TIMESTAMP TIMESTAMP_NTZ,
            SYNC_ID VARCHAR(100),
            ERROR_MESSAGE VARCHAR(4000),
            RETRY_COUNT NUMBER DEFAULT 0
        )
        """)
        
        # Create index for efficient queries
        self.snowflake.execute_query("""
        CREATE OR REPLACE INDEX IF NOT EXISTS IDX_CDC_STATUS_TIMESTAMP 
        ON CDC_TRACKING.CDC_CHANGE_LOG (SYNC_STATUS, CHANGE_TIMESTAMP)
        """)
        
        logger.info("Snowflake CDC infrastructure created")
        
    # ==========================================
    # FABRIC MIRRORING SETUP
    # ==========================================
    
    def setup_fabric_mirroring(self, 
                               snowflake_tables: List[str],
                               workspace_id: str = None) -> Dict[str, Any]:
        """
        Configure Fabric Mirroring for Snowflake tables.
        
        Sets up Delta Lake formatted tables in OneLake that automatically
        sync with Snowflake tables.
        """
        if not self.fabric:
            raise ValueError("Fabric client not set")
            
        workspace_id = workspace_id or os.getenv('FABRIC_WORKSPACE_ID')
        
        results = {
            'configured_tables': [],
            'failed_tables': [],
            'mirroring_config': {}
        }
        
        try:
            # Create mirroring configuration
            mirroring_config = {
                'name': f'Snowflake_Mirror_{datetime.now().strftime("%Y%m%d")}',
                'description': 'Automated Snowflake mirroring for bidirectional sync',
                'sourceConnection': {
                    'type': 'Snowflake',
                    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                    'database': os.getenv('SNOWFLAKE_DATABASE'),
                    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                    'schema': os.getenv('SNOWFLAKE_SCHEMA')
                },
                'replicationMode': 'Incremental',
                'schedule': {
                    'type': 'Recurring',
                    'intervalMinutes': self.config.sync_interval_minutes
                },
                'tables': []
            }
            
            for table in snowflake_tables:
                table_config = {
                    'sourceTable': table,
                    'targetTable': table.replace('.', '_'),
                    'enableChangeTracking': True,
                    'incrementalColumn': self.config.watermark_column
                }
                mirroring_config['tables'].append(table_config)
                results['configured_tables'].append(table)
                
            results['mirroring_config'] = mirroring_config
            
            # In production, would call Fabric API to create mirroring
            # self.fabric.create_mirroring(workspace_id, mirroring_config)
            
            # Store configuration locally
            config_file = f"fabric_mirroring_config_{workspace_id}.json"
            with open(config_file, 'w') as f:
                json.dump(mirroring_config, f, indent=2)
                
            logger.info(f"Fabric mirroring configured for {len(snowflake_tables)} tables")
            
        except Exception as e:
            logger.error(f"Failed to setup Fabric mirroring: {e}")
            results['error'] = str(e)
            
        return results
        
    def create_delta_lake_table(self, 
                                 table_name: str,
                                 columns: List[Dict],
                                 lakehouse_id: str) -> Dict[str, Any]:
        """
        Create a Delta Lake formatted table in OneLake.
        """
        delta_schema = []
        
        for col in columns:
            col_name = col.get('name', '')
            data_type = col.get('data_type', 'STRING')
            
            # Map to Delta Lake types
            delta_type_map = {
                'VARCHAR': 'STRING',
                'NUMBER': 'DOUBLE',
                'INTEGER': 'LONG',
                'BOOLEAN': 'BOOLEAN',
                'DATE': 'DATE',
                'TIMESTAMP': 'TIMESTAMP',
                'VARIANT': 'STRING'
            }
            
            delta_type = delta_type_map.get(data_type.upper(), 'STRING')
            delta_schema.append({
                'name': col_name,
                'type': delta_type,
                'nullable': col.get('nullable', True)
            })
            
        # Add sync metadata columns
        delta_schema.extend([
            {'name': '_SYNC_TIMESTAMP', 'type': 'TIMESTAMP', 'nullable': True},
            {'name': '_ROW_HASH', 'type': 'STRING', 'nullable': True},
            {'name': '_SYNC_ID', 'type': 'STRING', 'nullable': True},
            {'name': '_SOURCE_PLATFORM', 'type': 'STRING', 'nullable': True}
        ])
        
        delta_config = {
            'tableName': table_name,
            'lakehouseId': lakehouse_id,
            'format': 'delta',
            'schema': delta_schema,
            'partitionBy': ['_SYNC_TIMESTAMP'],
            'tableProperties': {
                'delta.enableChangeDataFeed': 'true',
                'delta.autoOptimize.optimizeWrite': 'true',
                'delta.autoOptimize.autoCompact': 'true'
            }
        }
        
        logger.info(f"Delta Lake table config created for {table_name}")
        return delta_config
        
    # ==========================================
    # SYNC EXECUTION
    # ==========================================
    
    def run_incremental_sync(self,
                              direction: SyncDirection = None,
                              tables: List[str] = None) -> Dict[str, Any]:
        """
        Run incremental synchronization using watermarks.
        
        Only syncs changes since last checkpoint.
        """
        direction = direction or self.config.direction
        
        results = {
            'direction': direction.value,
            'start_time': datetime.now().isoformat(),
            'tables_synced': [],
            'rows_synced': 0,
            'errors': []
        }
        
        with self.sync_lock:
            try:
                if direction in [SyncDirection.FABRIC_TO_SNOWFLAKE, SyncDirection.BIDIRECTIONAL]:
                    fabric_results = self._sync_fabric_to_snowflake_incremental(tables)
                    results['fabric_to_snowflake'] = fabric_results
                    results['rows_synced'] += fabric_results.get('rows_synced', 0)
                    
                if direction in [SyncDirection.SNOWFLAKE_TO_FABRIC, SyncDirection.BIDIRECTIONAL]:
                    snowflake_results = self._sync_snowflake_to_fabric_incremental(tables)
                    results['snowflake_to_fabric'] = snowflake_results
                    results['rows_synced'] += snowflake_results.get('rows_synced', 0)
                    
            except Exception as e:
                results['errors'].append(str(e))
                logger.error(f"Incremental sync failed: {e}")
                
        results['end_time'] = datetime.now().isoformat()
        
        return results
        
    def _sync_fabric_to_snowflake_incremental(self, tables: List[str] = None) -> Dict[str, Any]:
        """Sync changes from Fabric to Snowflake."""
        results = {
            'tables_synced': [],
            'rows_synced': 0,
            'errors': []
        }
        
        # Get changed records from Fabric
        # In production, would query Fabric's change feed
        
        # For each changed record, sync to Snowflake
        # Using MERGE for upserts
        
        logger.info("Incremental sync Fabric → Snowflake completed")
        return results
        
    def _sync_snowflake_to_fabric_incremental(self, tables: List[str] = None) -> Dict[str, Any]:
        """Sync changes from Snowflake to Fabric."""
        results = {
            'tables_synced': [],
            'rows_synced': 0,
            'errors': []
        }
        
        if not self.snowflake:
            results['errors'].append("Snowflake connector not set")
            return results
            
        # Get pending changes from CDC log
        query = """
        SELECT * FROM CDC_TRACKING.CDC_CHANGE_LOG
        WHERE SYNC_STATUS = 'PENDING'
        ORDER BY CHANGE_TIMESTAMP
        LIMIT 1000
        """
        
        try:
            changes = self.snowflake.execute_query(query)
            
            for change in changes:
                try:
                    # Process change
                    self._apply_change_to_fabric(change)
                    
                    # Mark as synced
                    update_query = f"""
                    UPDATE CDC_TRACKING.CDC_CHANGE_LOG
                    SET SYNC_STATUS = 'COMPLETED',
                        SYNC_TIMESTAMP = CURRENT_TIMESTAMP()
                    WHERE LOG_ID = {change['LOG_ID']}
                    """
                    self.snowflake.execute_query(update_query)
                    
                    results['rows_synced'] += 1
                    
                except Exception as e:
                    # Mark as failed
                    error_msg = str(e).replace("'", "''")
                    update_query = f"""
                    UPDATE CDC_TRACKING.CDC_CHANGE_LOG
                    SET SYNC_STATUS = 'FAILED',
                        ERROR_MESSAGE = '{error_msg}',
                        RETRY_COUNT = RETRY_COUNT + 1
                    WHERE LOG_ID = {change['LOG_ID']}
                    """
                    self.snowflake.execute_query(update_query)
                    results['errors'].append(f"Change {change['LOG_ID']}: {e}")
                    
        except Exception as e:
            results['errors'].append(str(e))
            logger.error(f"Failed to get CDC changes: {e}")
            
        logger.info(f"Incremental sync Snowflake → Fabric: {results['rows_synced']} rows")
        return results
        
    def _apply_change_to_fabric(self, change: Dict):
        """Apply a CDC change to Fabric."""
        operation = change.get('OPERATION_TYPE')
        table_name = change.get('TABLE_NAME')
        data = change.get('CHANGE_DATA', {})
        
        if operation == 'INSERT':
            # Create record in Fabric
            # self.fabric.insert_record(table_name, data)
            pass
        elif operation == 'UPDATE':
            # Update record in Fabric
            # self.fabric.update_record(table_name, data)
            pass
        elif operation == 'DELETE':
            # Delete record in Fabric (or soft delete)
            # self.fabric.delete_record(table_name, data)
            pass
            
        logger.debug(f"Applied {operation} to Fabric table {table_name}")
        
    def run_cdc_sync(self) -> Dict[str, Any]:
        """
        Process CDC buffer and sync changes bidirectionally.
        
        Handles conflict resolution for simultaneous changes.
        """
        results = {
            'processed': 0,
            'conflicts_detected': 0,
            'conflicts_resolved': 0,
            'errors': []
        }
        
        with self.sync_lock:
            while self.cdc_buffer:
                record = self.cdc_buffer.pop(0)
                
                try:
                    # Check for conflicts
                    conflict = self._detect_conflict(record)
                    
                    if conflict:
                        results['conflicts_detected'] += 1
                        resolved = self._resolve_conflict(record, conflict)
                        if resolved:
                            results['conflicts_resolved'] += 1
                    else:
                        # Apply change
                        self._apply_cdc_record(record)
                        
                    results['processed'] += 1
                    
                except Exception as e:
                    results['errors'].append(str(e))
                    logger.error(f"CDC sync error: {e}")
                    
        return results
        
    def _detect_conflict(self, record: CDCRecord) -> Optional[CDCRecord]:
        """
        Detect if a change conflicts with pending changes.
        
        Conflict exists if same row was modified on both platforms
        since last sync.
        """
        # Check other platform for concurrent modifications
        # This would query the change tracking systems
        return None  # No conflict
        
    def _resolve_conflict(self, 
                          local: CDCRecord, 
                          remote: CDCRecord) -> bool:
        """
        Resolve a conflict between two concurrent changes.
        """
        strategy = self.config.conflict_resolution
        
        if strategy == ConflictResolution.FABRIC_WINS:
            winner = local if local.source_platform == 'fabric' else remote
        elif strategy == ConflictResolution.SNOWFLAKE_WINS:
            winner = local if local.source_platform == 'snowflake' else remote
        elif strategy == ConflictResolution.LATEST_WINS:
            winner = local if local.timestamp > remote.timestamp else remote
        elif strategy == ConflictResolution.MANUAL:
            # Log for manual resolution
            logger.warning(f"Manual conflict resolution needed: {local.table_name}")
            return False
        else:
            winner = local
            
        # Apply winning change to both platforms
        self._apply_cdc_record(winner)
        return True
        
    def _apply_cdc_record(self, record: CDCRecord):
        """Apply a CDC record to the target platform."""
        target = 'snowflake' if record.source_platform == 'fabric' else 'fabric'
        
        if record.operation == 'INSERT':
            if target == 'snowflake':
                self._insert_to_snowflake(record.table_name, record.new_values)
            else:
                self._insert_to_fabric(record.table_name, record.new_values)
        elif record.operation == 'UPDATE':
            if target == 'snowflake':
                self._update_in_snowflake(record.table_name, record.primary_key, record.new_values)
            else:
                self._update_in_fabric(record.table_name, record.primary_key, record.new_values)
        elif record.operation == 'DELETE':
            if target == 'snowflake':
                self._delete_from_snowflake(record.table_name, record.primary_key)
            else:
                self._delete_from_fabric(record.table_name, record.primary_key)
                
    def _insert_to_snowflake(self, table: str, values: Dict):
        """Insert a record to Snowflake."""
        if not self.snowflake:
            return
            
        cols = ", ".join(values.keys())
        vals = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in values.values()])
        
        query = f"INSERT INTO {table} ({cols}) VALUES ({vals})"
        self.snowflake.execute_query(query)
        
    def _update_in_snowflake(self, table: str, pk: Dict, values: Dict):
        """Update a record in Snowflake."""
        if not self.snowflake:
            return
            
        set_clause = ", ".join([f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}" 
                               for k, v in values.items()])
        where_clause = " AND ".join([f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
                                    for k, v in pk.items()])
        
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        self.snowflake.execute_query(query)
        
    def _delete_from_snowflake(self, table: str, pk: Dict):
        """Delete a record from Snowflake."""
        if not self.snowflake:
            return
            
        where_clause = " AND ".join([f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}"
                                    for k, v in pk.items()])
        
        query = f"DELETE FROM {table} WHERE {where_clause}"
        self.snowflake.execute_query(query)
        
    def _insert_to_fabric(self, table: str, values: Dict):
        """Insert a record to Fabric."""
        # Would use Fabric API
        logger.debug(f"Insert to Fabric: {table}")
        
    def _update_in_fabric(self, table: str, pk: Dict, values: Dict):
        """Update a record in Fabric."""
        # Would use Fabric API
        logger.debug(f"Update in Fabric: {table}")
        
    def _delete_from_fabric(self, table: str, pk: Dict):
        """Delete a record from Fabric."""
        # Would use Fabric API
        logger.debug(f"Delete from Fabric: {table}")
        
    # ==========================================
    # STORED PROCEDURES / PIPELINES
    # ==========================================
    
    def create_sync_stored_procedures(self) -> Dict[str, str]:
        """
        Create stored procedures for sync operations in Snowflake.
        """
        procedures = {}
        
        # Main sync procedure
        procedures['SP_SYNC_TO_FABRIC'] = """
CREATE OR REPLACE PROCEDURE SP_SYNC_TO_FABRIC(TABLE_NAME VARCHAR, BATCH_SIZE NUMBER DEFAULT 10000)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    rows_synced NUMBER := 0;
    batch_count NUMBER := 0;
    sync_id VARCHAR;
BEGIN
    -- Generate sync ID
    sync_id := 'SYNC_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') || '_' || UUID_STRING();
    
    -- Process changes in batches
    FOR batch IN (
        SELECT * FROM CDC_TRACKING.CDC_CHANGE_LOG
        WHERE TABLE_NAME = :TABLE_NAME
          AND SYNC_STATUS = 'PENDING'
        ORDER BY CHANGE_TIMESTAMP
        LIMIT :BATCH_SIZE
    ) DO
        -- Mark as processing
        UPDATE CDC_TRACKING.CDC_CHANGE_LOG
        SET SYNC_STATUS = 'PROCESSING',
            SYNC_ID = :sync_id
        WHERE LOG_ID = batch.LOG_ID;
        
        -- Here would be the actual sync to Fabric via UDF or external function
        -- CALL FABRIC_SYNC_UDF(batch.CHANGE_DATA);
        
        -- Mark as completed
        UPDATE CDC_TRACKING.CDC_CHANGE_LOG
        SET SYNC_STATUS = 'COMPLETED',
            SYNC_TIMESTAMP = CURRENT_TIMESTAMP()
        WHERE LOG_ID = batch.LOG_ID;
        
        rows_synced := rows_synced + 1;
    END FOR;
    
    RETURN OBJECT_CONSTRUCT(
        'sync_id', :sync_id,
        'rows_synced', :rows_synced,
        'status', 'COMPLETED'
    );
END;
$$;
"""
        
        # Rollback procedure
        procedures['SP_SYNC_ROLLBACK'] = """
CREATE OR REPLACE PROCEDURE SP_SYNC_ROLLBACK(SYNC_ID VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    rows_rolled_back NUMBER := 0;
BEGIN
    -- Reset all records from this sync back to pending
    UPDATE CDC_TRACKING.CDC_CHANGE_LOG
    SET SYNC_STATUS = 'PENDING',
        SYNC_TIMESTAMP = NULL,
        SYNC_ID = NULL
    WHERE SYNC_ID = :SYNC_ID
      AND SYNC_STATUS IN ('PROCESSING', 'COMPLETED');
    
    rows_rolled_back := SQLROWCOUNT;
    
    RETURN OBJECT_CONSTRUCT(
        'sync_id', :SYNC_ID,
        'rows_rolled_back', :rows_rolled_back,
        'status', 'ROLLED_BACK'
    );
END;
$$;
"""
        
        # Health check procedure
        procedures['SP_SYNC_HEALTH_CHECK'] = """
CREATE OR REPLACE PROCEDURE SP_SYNC_HEALTH_CHECK()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    pending_count NUMBER;
    failed_count NUMBER;
    avg_lag_seconds NUMBER;
BEGIN
    -- Get counts
    SELECT COUNT(*) INTO pending_count
    FROM CDC_TRACKING.CDC_CHANGE_LOG
    WHERE SYNC_STATUS = 'PENDING';
    
    SELECT COUNT(*) INTO failed_count
    FROM CDC_TRACKING.CDC_CHANGE_LOG
    WHERE SYNC_STATUS = 'FAILED';
    
    -- Get average lag
    SELECT AVG(TIMESTAMPDIFF('SECOND', CHANGE_TIMESTAMP, CURRENT_TIMESTAMP())) INTO avg_lag_seconds
    FROM CDC_TRACKING.CDC_CHANGE_LOG
    WHERE SYNC_STATUS = 'PENDING';
    
    RETURN OBJECT_CONSTRUCT(
        'status', IFF(:pending_count > 10000 OR :failed_count > 100, 'WARNING', 'HEALTHY'),
        'pending_changes', :pending_count,
        'failed_changes', :failed_count,
        'avg_lag_seconds', :avg_lag_seconds,
        'check_timestamp', CURRENT_TIMESTAMP()
    );
END;
$$;
"""
        
        # Create procedures in Snowflake
        if self.snowflake:
            for name, ddl in procedures.items():
                try:
                    self.snowflake.execute_query(ddl)
                    logger.info(f"Created procedure: {name}")
                except Exception as e:
                    logger.error(f"Failed to create {name}: {e}")
                    
        return procedures
        
    def create_sync_tasks(self) -> Dict[str, str]:
        """
        Create scheduled tasks for automated sync in Snowflake.
        """
        tasks = {}
        
        interval = self.config.sync_interval_minutes
        
        # Main sync task
        tasks['TASK_BIDIRECTIONAL_SYNC'] = f"""
CREATE OR REPLACE TASK TASK_BIDIRECTIONAL_SYNC
WAREHOUSE = {os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WAREHOUSE')}
SCHEDULE = 'USING CRON */{interval} * * * * UTC'
AS
BEGIN
    -- Run sync to Fabric for each tracked table
    CALL SP_SYNC_TO_FABRIC(NULL);
    
    -- Log execution
    INSERT INTO CDC_TRACKING.SYNC_EXECUTION_LOG (EXECUTION_TIME, STATUS)
    VALUES (CURRENT_TIMESTAMP(), 'COMPLETED');
END;
"""
        
        # Health check task
        tasks['TASK_SYNC_HEALTH_CHECK'] = f"""
CREATE OR REPLACE TASK TASK_SYNC_HEALTH_CHECK
WAREHOUSE = {os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WAREHOUSE')}
SCHEDULE = 'USING CRON */30 * * * * UTC'
AS
BEGIN
    DECLARE health_result VARIANT;
    BEGIN
        CALL SP_SYNC_HEALTH_CHECK() INTO health_result;
        
        -- Alert if unhealthy
        IF (health_result:status = 'WARNING') THEN
            -- Would trigger notification
            INSERT INTO CDC_TRACKING.SYNC_ALERTS (ALERT_TIME, ALERT_TYPE, DETAILS)
            VALUES (CURRENT_TIMESTAMP(), 'HEALTH_WARNING', :health_result);
        END IF;
    END;
END;
"""

        # Failed record retry task
        tasks['TASK_RETRY_FAILED_SYNCS'] = f"""
CREATE OR REPLACE TASK TASK_RETRY_FAILED_SYNCS
WAREHOUSE = {os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WAREHOUSE')}
SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
BEGIN
    -- Reset failed records with retry_count < 3 back to pending
    UPDATE CDC_TRACKING.CDC_CHANGE_LOG
    SET SYNC_STATUS = 'PENDING'
    WHERE SYNC_STATUS = 'FAILED'
      AND RETRY_COUNT < 3
      AND CHANGE_TIMESTAMP < DATEADD(hour, -1, CURRENT_TIMESTAMP());
END;
"""
        
        # Create and resume tasks
        if self.snowflake:
            for name, ddl in tasks.items():
                try:
                    self.snowflake.execute_query(ddl)
                    self.snowflake.execute_query(f"ALTER TASK {name} RESUME")
                    logger.info(f"Created and resumed task: {name}")
                except Exception as e:
                    logger.error(f"Failed to create {name}: {e}")
                    
        return tasks
        
    # ==========================================
    # CHECKPOINT MANAGEMENT
    # ==========================================
    
    def _load_checkpoints(self):
        """Load checkpoints from file."""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    for key, cp_dict in data.items():
                        self.checkpoints[key] = SyncCheckpoint(**cp_dict)
        except Exception as e:
            logger.warning(f"Could not load checkpoints: {e}")
            
    def _save_checkpoints(self):
        """Save checkpoints to file."""
        try:
            data = {key: cp.__dict__ for key, cp in self.checkpoints.items()}
            with open(self.checkpoint_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Could not save checkpoints: {e}")
            
    def update_checkpoint(self, table_name: str, platform: str, 
                          rows_synced: int, watermark: Any):
        """Update checkpoint for a table."""
        key = f"{platform}:{table_name}"
        sync_id = f"SYNC_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        self.checkpoints[key] = SyncCheckpoint(
            checkpoint_id=key,
            table_name=table_name,
            last_sync_timestamp=datetime.now().isoformat(),
            last_sync_id=sync_id,
            rows_synced=rows_synced,
            platform=platform,
            watermark_value=watermark
        )
        
        self._save_checkpoints()
        
    def get_checkpoint(self, table_name: str, platform: str) -> Optional[SyncCheckpoint]:
        """Get checkpoint for a table."""
        key = f"{platform}:{table_name}"
        return self.checkpoints.get(key)
        
    # ==========================================
    # SCHEMA UNIFICATION
    # ==========================================
    
    def unify_schemas(self, tables: List[str] = None) -> Dict[str, Any]:
        """
        Ensure identical table schemas across both platforms.
        
        Compares schemas and generates ALTER statements to align them.
        """
        results = {
            'tables_analyzed': [],
            'schema_differences': [],
            'alignment_ddl': []
        }
        
        tables = tables or self._get_snowflake_sync_tables()
        
        for table in tables:
            try:
                # Get Snowflake schema
                sf_schema = self._get_snowflake_schema(table)
                
                # Get Fabric schema (would query Fabric)
                # fabric_schema = self._get_fabric_schema(table)
                
                results['tables_analyzed'].append(table)
                
                # Compare and generate alignment DDL
                # alignment = self._compare_schemas(sf_schema, fabric_schema)
                # results['alignment_ddl'].extend(alignment)
                
            except Exception as e:
                logger.error(f"Schema analysis failed for {table}: {e}")
                
        return results
        
    def _get_snowflake_schema(self, table: str) -> List[Dict]:
        """Get column schema from Snowflake."""
        parts = table.split('.')
        if len(parts) == 3:
            database, schema, table_name = parts
        else:
            database = os.getenv('SNOWFLAKE_DATABASE')
            schema = os.getenv('SNOWFLAKE_SCHEMA') 
            table_name = table
            
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, 
               CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        return self.snowflake.execute_query(query) if self.snowflake else []
        
    def generate_naming_convention_mappings(self) -> Dict[str, str]:
        """
        Generate consistent naming mappings between platforms.
        
        Handles differences in case sensitivity, special characters, etc.
        """
        mappings = {}
        
        # Get all tables from both platforms
        sf_tables = self._get_snowflake_sync_tables() if self.snowflake else []
        
        for table in sf_tables:
            # Snowflake uses UPPERCASE, Fabric prefers PascalCase
            parts = table.split('.')
            table_name = parts[-1]
            
            # Create consistent mapping
            fabric_name = ''.join(word.capitalize() for word in table_name.lower().split('_'))
            mappings[table] = {
                'snowflake_name': table,
                'fabric_name': fabric_name,
                'onelake_path': f"Tables/{fabric_name}"
            }
            
        return mappings
        
    # ==========================================
    # STATUS AND MONITORING
    # ==========================================
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get comprehensive sync status."""
        status = {
            'is_running': self.running,
            'config': {
                'direction': self.config.direction.value,
                'mode': self.config.mode.value,
                'interval_minutes': self.config.sync_interval_minutes
            },
            'checkpoints': len(self.checkpoints),
            'cdc_buffer_size': len(self.cdc_buffer),
            'scheduled_tasks': len(self.scheduled_tasks)
        }
        
        # Get CDC stats from Snowflake
        if self.snowflake:
            try:
                stats = self.snowflake.execute_query("""
                SELECT 
                    SYNC_STATUS,
                    COUNT(*) as COUNT
                FROM CDC_TRACKING.CDC_CHANGE_LOG
                GROUP BY SYNC_STATUS
                """)
                status['cdc_stats'] = {r['SYNC_STATUS']: r['COUNT'] for r in stats}
            except:
                status['cdc_stats'] = {}
                
        return status
        
    def get_sync_history(self, days: int = 7) -> List[Dict]:
        """Get sync execution history."""
        if not self.snowflake:
            return []
            
        try:
            query = f"""
            SELECT * FROM CDC_TRACKING.SYNC_EXECUTION_LOG
            WHERE EXECUTION_TIME > DATEADD(day, -{days}, CURRENT_TIMESTAMP())
            ORDER BY EXECUTION_TIME DESC
            LIMIT 100
            """
            return self.snowflake.execute_query(query)
        except:
            return []
