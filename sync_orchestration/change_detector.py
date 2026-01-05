"""
Change Detectors - Fabric and Snowflake CDC

Implements change detection for bidirectional sync:
- Fabric: Polling with modification timestamp tracking
- Snowflake: Streams (CDC) or polling fallback
"""

import os
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ChangeEvent:
    """Represents a detected change."""
    table_name: str
    change_type: str  # INSERT, UPDATE, DELETE, SCHEMA_CHANGE
    detected_at: datetime = field(default_factory=datetime.now)
    modified_at: Optional[datetime] = None
    row_count: Optional[int] = None
    affected_columns: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "change_type": self.change_type,
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
            "row_count": self.row_count,
            "affected_columns": self.affected_columns,
            "metadata": self.metadata
        }


class BaseChangeDetector(ABC):
    """Abstract base class for change detection."""
    
    def __init__(self, checkpoint_file: str = None):
        self.checkpoint_file = checkpoint_file
        self.last_checkpoint: Optional[datetime] = None
        self._load_checkpoint()
    
    def _load_checkpoint(self):
        """Load last sync checkpoint from file."""
        if self.checkpoint_file and os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    if "last_checkpoint" in data:
                        self.last_checkpoint = datetime.fromisoformat(data["last_checkpoint"])
            except Exception as e:
                logger.warning(f"Error loading checkpoint: {e}")
    
    def _save_checkpoint(self, checkpoint: datetime = None):
        """Save sync checkpoint to file."""
        checkpoint = checkpoint or datetime.now()
        self.last_checkpoint = checkpoint
        
        if self.checkpoint_file:
            try:
                data = {"last_checkpoint": checkpoint.isoformat()}
                os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
                with open(self.checkpoint_file, 'w') as f:
                    json.dump(data, f)
            except Exception as e:
                logger.warning(f"Error saving checkpoint: {e}")
    
    @abstractmethod
    def detect_changes(self, since: datetime = None) -> List[ChangeEvent]:
        """Detect changes since last checkpoint."""
        pass
    
    @abstractmethod
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Get all tables/models from the platform."""
        pass


class FabricChangeDetector(BaseChangeDetector):
    """
    Change detector for Microsoft Fabric.
    
    Uses polling against the Fabric API to detect:
    - New semantic models
    - Modified tables
    - Schema changes
    """
    
    def __init__(self, 
                 fabric_client=None,
                 checkpoint_file: str = None,
                 poll_interval_seconds: int = 300):
        """
        Initialize Fabric change detector.
        
        Args:
            fabric_client: FabricApiClient instance
            checkpoint_file: Path to store checkpoint
            poll_interval_seconds: Polling interval (default 5 min)
        """
        super().__init__(checkpoint_file)
        self.fabric_client = fabric_client
        self.poll_interval = poll_interval_seconds
        self._last_known_state: Dict[str, Dict] = {}
    
    def set_client(self, client):
        """Set the Fabric API client."""
        self.fabric_client = client
    
    def detect_changes(self, since: datetime = None) -> List[ChangeEvent]:
        """
        Detect changes in Fabric semantic models.
        
        Args:
            since: Detect changes after this timestamp
            
        Returns:
            List of ChangeEvent objects
        """
        if since is None:
            since = self.last_checkpoint or datetime.now() - timedelta(hours=24)
        
        changes: List[ChangeEvent] = []
        
        if not self.fabric_client:
            logger.error("No Fabric client configured")
            return changes
        
        try:
            # Authenticate
            if not self.fabric_client.authenticate():
                logger.error("Failed to authenticate with Fabric")
                return changes
            
            # Get all semantic models
            models = self.fabric_client.get_semantic_models() or []
            current_state = {}
            
            for model in models:
                model_id = model.get("id", "")
                model_name = model.get("displayName", model.get("name", "Unknown"))
                
                try:
                    # Get model details including tables
                    detail = self.fabric_client.get_semantic_model_detail(model_id)
                    if not detail:
                        continue
                    
                    tables = detail.get("tables", [])
                    
                    for table in tables:
                        table_name = table.get("name", "")
                        if not table_name:
                            continue
                        
                        # Build state key
                        state_key = f"{model_name}.{table_name}"
                        columns = table.get("columns", [])
                        measures = table.get("measures", [])
                        
                        # Calculate schema hash
                        schema_hash = self._calculate_schema_hash(columns, measures)
                        
                        current_state[state_key] = {
                            "model_id": model_id,
                            "model_name": model_name,
                            "table_name": table_name,
                            "columns": columns,
                            "measures": measures,
                            "schema_hash": schema_hash
                        }
                        
                        # Compare with previous state
                        if state_key in self._last_known_state:
                            old_state = self._last_known_state[state_key]
                            
                            if old_state.get("schema_hash") != schema_hash:
                                # Schema changed
                                changes.append(ChangeEvent(
                                    table_name=table_name,
                                    change_type="SCHEMA_CHANGE",
                                    modified_at=datetime.now(),
                                    metadata={
                                        "model_id": model_id,
                                        "model_name": model_name,
                                        "old_hash": old_state.get("schema_hash"),
                                        "new_hash": schema_hash
                                    }
                                ))
                        else:
                            # New table
                            changes.append(ChangeEvent(
                                table_name=table_name,
                                change_type="INSERT",
                                modified_at=datetime.now(),
                                row_count=len(columns),
                                metadata={
                                    "model_id": model_id,
                                    "model_name": model_name,
                                    "columns_count": len(columns),
                                    "measures_count": len(measures)
                                }
                            ))
                
                except Exception as e:
                    logger.warning(f"Error getting detail for model {model_name}: {e}")
            
            # Detect deleted tables
            for state_key in self._last_known_state:
                if state_key not in current_state:
                    old_state = self._last_known_state[state_key]
                    changes.append(ChangeEvent(
                        table_name=old_state.get("table_name", state_key),
                        change_type="DELETE",
                        modified_at=datetime.now(),
                        metadata={
                            "model_name": old_state.get("model_name")
                        }
                    ))
            
            # Update state
            self._last_known_state = current_state
            self._save_checkpoint()
            
        except Exception as e:
            logger.error(f"Error detecting Fabric changes: {e}")
        
        return changes
    
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Get all tables from Fabric semantic models."""
        tables = []
        
        if not self.fabric_client:
            return tables
        
        try:
            if not self.fabric_client.authenticate():
                return tables
            
            models = self.fabric_client.get_semantic_models() or []
            
            for model in models:
                model_id = model.get("id", "")
                model_name = model.get("displayName", model.get("name", "Unknown"))
                
                try:
                    detail = self.fabric_client.get_semantic_model_detail(model_id)
                    if detail:
                        for table in detail.get("tables", []):
                            tables.append({
                                "table_name": table.get("name", ""),
                                "model_name": model_name,
                                "model_id": model_id,
                                "columns": table.get("columns", []),
                                "measures": table.get("measures", []),
                                "source": "fabric"
                            })
                except Exception as e:
                    logger.warning(f"Error getting tables from model {model_name}: {e}")
        
        except Exception as e:
            logger.error(f"Error getting Fabric tables: {e}")
        
        return tables
    
    def _calculate_schema_hash(self, columns: List[Dict], measures: List[Dict]) -> str:
        """Calculate a hash of the schema for change detection."""
        import hashlib
        
        schema_str = json.dumps({
            "columns": [(c.get("name"), c.get("dataType")) for c in columns],
            "measures": [(m.get("name"), m.get("expression", "")[:50]) for m in measures]
        }, sort_keys=True)
        
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]


class SnowflakeChangeDetector(BaseChangeDetector):
    """
    Change detector for Snowflake.
    
    Supports two modes:
    1. Stream-based (CDC) - Real-time using Snowflake Streams
    2. Polling - Fallback using timestamp comparison
    """
    
    def __init__(self,
                 snowflake_connector=None,
                 checkpoint_file: str = None,
                 use_streams: bool = True,
                 poll_interval_seconds: int = 300):
        """
        Initialize Snowflake change detector.
        
        Args:
            snowflake_connector: SnowflakeConnector instance
            checkpoint_file: Path to store checkpoint
            use_streams: Whether to use Snowflake Streams for CDC
            poll_interval_seconds: Polling interval for fallback mode
        """
        super().__init__(checkpoint_file)
        self.snowflake_connector = snowflake_connector
        self.use_streams = use_streams
        self.poll_interval = poll_interval_seconds
        self._tracked_tables: Set[str] = set()
        self._last_row_counts: Dict[str, int] = {}
    
    def set_connector(self, connector):
        """Set the Snowflake connector."""
        self.snowflake_connector = connector
    
    def detect_changes(self, since: datetime = None) -> List[ChangeEvent]:
        """
        Detect changes in Snowflake tables.
        
        Args:
            since: Detect changes after this timestamp
            
        Returns:
            List of ChangeEvent objects
        """
        if since is None:
            since = self.last_checkpoint or datetime.now() - timedelta(hours=24)
        
        changes: List[ChangeEvent] = []
        
        if not self.snowflake_connector:
            logger.error("No Snowflake connector configured")
            return changes
        
        try:
            if not self.snowflake_connector.connect():
                logger.error("Failed to connect to Snowflake")
                return changes
            
            cursor = self.snowflake_connector.connection.cursor()
            
            if self.use_streams:
                # Try stream-based CDC first
                changes.extend(self._detect_via_streams(cursor))
            
            if not changes:
                # Fall back to polling
                changes.extend(self._detect_via_polling(cursor, since))
            
            cursor.close()
            self.snowflake_connector.disconnect()
            
            self._save_checkpoint()
            
        except Exception as e:
            logger.error(f"Error detecting Snowflake changes: {e}")
        
        return changes
    
    def _detect_via_streams(self, cursor) -> List[ChangeEvent]:
        """Detect changes using Snowflake Streams."""
        changes = []
        
        try:
            # Get all streams
            cursor.execute("SHOW STREAMS")
            streams = cursor.fetchall()
            
            for stream_row in streams:
                stream_name = stream_row[1]
                table_name = stream_row[5] if len(stream_row) > 5 else None
                
                if not table_name:
                    continue
                
                try:
                    # Query stream for changes
                    cursor.execute(f'SELECT * FROM "{stream_name}" LIMIT 1000')
                    stream_data = cursor.fetchall()
                    
                    if stream_data:
                        # Group by action type
                        inserts = 0
                        updates = 0
                        deletes = 0
                        
                        for row in stream_data:
                            # Stream metadata columns: METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID
                            # This depends on stream structure
                            pass
                        
                        if inserts > 0:
                            changes.append(ChangeEvent(
                                table_name=table_name,
                                change_type="INSERT",
                                row_count=inserts,
                                metadata={"source": "stream", "stream_name": stream_name}
                            ))
                        
                        if updates > 0:
                            changes.append(ChangeEvent(
                                table_name=table_name,
                                change_type="UPDATE",
                                row_count=updates,
                                metadata={"source": "stream", "stream_name": stream_name}
                            ))
                        
                        if deletes > 0:
                            changes.append(ChangeEvent(
                                table_name=table_name,
                                change_type="DELETE",
                                row_count=deletes,
                                metadata={"source": "stream", "stream_name": stream_name}
                            ))
                
                except Exception as e:
                    logger.warning(f"Error reading stream {stream_name}: {e}")
        
        except Exception as e:
            logger.warning(f"Streams not available, falling back to polling: {e}")
        
        return changes
    
    def _detect_via_polling(self, cursor, since: datetime) -> List[ChangeEvent]:
        """Detect changes via row count comparison (polling)."""
        changes = []
        
        try:
            # Get all tables
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            for table_row in tables:
                table_name = table_row[1]
                
                # Skip system tables
                if table_name.startswith("_") or table_name.startswith("SYS"):
                    continue
                
                try:
                    # Get current row count
                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                    current_count = cursor.fetchone()[0]
                    
                    # Compare with previous count
                    previous_count = self._last_row_counts.get(table_name)
                    
                    if previous_count is None:
                        # New table
                        changes.append(ChangeEvent(
                            table_name=table_name,
                            change_type="INSERT",
                            row_count=current_count,
                            metadata={"source": "polling", "is_new_table": True}
                        ))
                    elif current_count > previous_count:
                        # Rows added
                        changes.append(ChangeEvent(
                            table_name=table_name,
                            change_type="INSERT",
                            row_count=current_count - previous_count,
                            metadata={
                                "source": "polling",
                                "previous_count": previous_count,
                                "current_count": current_count
                            }
                        ))
                    elif current_count < previous_count:
                        # Rows deleted
                        changes.append(ChangeEvent(
                            table_name=table_name,
                            change_type="DELETE",
                            row_count=previous_count - current_count,
                            metadata={
                                "source": "polling",
                                "previous_count": previous_count,
                                "current_count": current_count
                            }
                        ))
                    
                    # Update tracked count
                    self._last_row_counts[table_name] = current_count
                    self._tracked_tables.add(table_name)
                
                except Exception as e:
                    logger.warning(f"Error polling table {table_name}: {e}")
            
            # Detect deleted tables
            current_table_names = {row[1] for row in tables}
            for tracked in list(self._tracked_tables):
                if tracked not in current_table_names:
                    changes.append(ChangeEvent(
                        table_name=tracked,
                        change_type="DELETE",
                        metadata={"source": "polling", "table_dropped": True}
                    ))
                    self._tracked_tables.discard(tracked)
                    self._last_row_counts.pop(tracked, None)
        
        except Exception as e:
            logger.error(f"Error in polling detection: {e}")
        
        return changes
    
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Get all tables from Snowflake."""
        tables = []
        
        if not self.snowflake_connector:
            return tables
        
        try:
            if not self.snowflake_connector.connect():
                return tables
            
            cursor = self.snowflake_connector.connection.cursor()
            
            # Get all tables
            cursor.execute("SHOW TABLES")
            table_list = cursor.fetchall()
            
            for table_row in table_list:
                table_name = table_row[1]
                
                if table_name.startswith("_") or table_name.startswith("SYS"):
                    continue
                
                try:
                    # Get column info
                    cursor.execute(f'DESCRIBE TABLE "{table_name}"')
                    columns = [
                        {"name": col[0], "dataType": col[1]}
                        for col in cursor.fetchall()
                    ]
                    
                    # Get row count
                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                    row_count = cursor.fetchone()[0]
                    
                    tables.append({
                        "table_name": table_name,
                        "columns": columns,
                        "row_count": row_count,
                        "source": "snowflake"
                    })
                
                except Exception as e:
                    logger.warning(f"Error describing table {table_name}: {e}")
            
            cursor.close()
            self.snowflake_connector.disconnect()
        
        except Exception as e:
            logger.error(f"Error getting Snowflake tables: {e}")
        
        return tables
    
    def create_stream(self, table_name: str) -> bool:
        """
        Create a Snowflake stream for CDC on a table.
        
        Args:
            table_name: Name of the table to track
            
        Returns:
            True if stream created successfully
        """
        if not self.snowflake_connector:
            return False
        
        try:
            if not self.snowflake_connector.connect():
                return False
            
            cursor = self.snowflake_connector.connection.cursor()
            stream_name = f"{table_name}_SYNC_STREAM"
            
            cursor.execute(f'''
                CREATE STREAM IF NOT EXISTS "{stream_name}" 
                ON TABLE "{table_name}"
                SHOW_INITIAL_ROWS = FALSE
            ''')
            
            cursor.close()
            self.snowflake_connector.disconnect()
            
            logger.info(f"Created stream {stream_name} for table {table_name}")
            return True
        
        except Exception as e:
            logger.error(f"Error creating stream for {table_name}: {e}")
            return False
    
    def consume_stream(self, table_name: str) -> List[Dict]:
        """
        Consume changes from a Snowflake stream.
        
        Args:
            table_name: Name of the source table
            
        Returns:
            List of changed records with metadata
        """
        changes = []
        
        if not self.snowflake_connector:
            return changes
        
        try:
            if not self.snowflake_connector.connect():
                return changes
            
            cursor = self.snowflake_connector.connection.cursor()
            stream_name = f"{table_name}_SYNC_STREAM"
            
            # Query stream
            cursor.execute(f'''
                SELECT *, METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID
                FROM "{stream_name}"
            ''')
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            for row in rows:
                record = dict(zip(columns, row))
                
                action = record.pop("METADATA$ACTION", "INSERT")
                is_update = record.pop("METADATA$ISUPDATE", False)
                row_id = record.pop("METADATA$ROW_ID", None)
                
                changes.append({
                    "data": record,
                    "action": "UPDATE" if is_update else action,
                    "row_id": row_id
                })
            
            cursor.close()
            self.snowflake_connector.disconnect()
        
        except Exception as e:
            logger.error(f"Error consuming stream for {table_name}: {e}")
        
        return changes
