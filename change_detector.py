"""
Change Detector Module for Fabric-Snowflake Semantic Sync.

Captures snapshots and detects changes between Fabric and Snowflake.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
import json


@dataclass
class ColumnSnapshot:
    """Snapshot of a column."""
    name: str
    data_type: str
    is_nullable: bool = True
    description: str = ""


@dataclass
class MeasureSnapshot:
    """Snapshot of a measure."""
    name: str
    expression: str
    data_type: str = "DECIMAL"
    description: str = ""


@dataclass
class TableSnapshot:
    """Snapshot of a table/view."""
    name: str
    columns: List[ColumnSnapshot] = field(default_factory=list)
    measures: List[MeasureSnapshot] = field(default_factory=list)
    row_count: int = 0


@dataclass
class ModelSnapshot:
    """Snapshot of a semantic model."""
    name: str
    source: str  # 'fabric' or 'snowflake'
    tables: List[TableSnapshot] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)


class ChangeDetector:
    """Detects changes between Fabric and Snowflake semantic models."""
    
    def __init__(self, fabric_client, snowflake_connector):
        self.fabric_client = fabric_client
        self.snowflake_connector = snowflake_connector
        self.snapshots: Dict[str, ModelSnapshot] = {}
    
    def capture_fabric_snapshot(self, model_id: str) -> Optional[ModelSnapshot]:
        """Capture a snapshot of a Fabric semantic model."""
        try:
            # Get model details from Fabric
            model_detail = self.fabric_client.get_semantic_model_detail(model_id)
            
            if not model_detail:
                return None
            
            tables = []
            
            # Extract tables from model definition
            model_def = model_detail.get("model", {})
            for table_data in model_def.get("tables", []):
                columns = []
                for col in table_data.get("columns", []):
                    columns.append(ColumnSnapshot(
                        name=col.get("name", ""),
                        data_type=col.get("dataType", "STRING"),
                        is_nullable=col.get("isNullable", True),
                        description=col.get("description", "")
                    ))
                
                measures = []
                for measure in table_data.get("measures", []):
                    measures.append(MeasureSnapshot(
                        name=measure.get("name", ""),
                        expression=measure.get("expression", ""),
                        data_type=measure.get("dataType", "DECIMAL"),
                        description=measure.get("description", "")
                    ))
                
                tables.append(TableSnapshot(
                    name=table_data.get("name", ""),
                    columns=columns,
                    measures=measures
                ))
            
            snapshot = ModelSnapshot(
                name=model_detail.get("displayName", model_detail.get("name", "")),
                source="fabric",
                tables=tables,
                metadata={"id": model_id}
            )
            
            self.snapshots[f"fabric_{model_id}"] = snapshot
            return snapshot
            
        except Exception as e:
            print(f"Error capturing Fabric snapshot: {e}")
            return None
    
    def capture_snowflake_snapshot(self, view_name: str) -> Optional[ModelSnapshot]:
        """Capture a snapshot of a Snowflake view."""
        try:
            # Get view columns from Snowflake
            columns_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{view_name}'
                ORDER BY ORDINAL_POSITION
            """
            
            columns_data = self.snowflake_connector.execute_query(columns_query, fetch_all=True)
            
            columns = []
            if columns_data:
                for col in columns_data:
                    columns.append(ColumnSnapshot(
                        name=col.get("COLUMN_NAME", ""),
                        data_type=col.get("DATA_TYPE", "VARCHAR"),
                        is_nullable=col.get("IS_NULLABLE", "YES") == "YES"
                    ))
            
            # Get row count
            count_query = f"SELECT COUNT(*) as cnt FROM {view_name}"
            count_result = self.snowflake_connector.execute_query(count_query, fetch_all=True)
            row_count = count_result[0].get("CNT", 0) if count_result else 0
            
            table_snapshot = TableSnapshot(
                name=view_name,
                columns=columns,
                measures=[],
                row_count=row_count
            )
            
            snapshot = ModelSnapshot(
                name=view_name,
                source="snowflake",
                tables=[table_snapshot],
                metadata={"view_name": view_name}
            )
            
            self.snapshots[f"snowflake_{view_name}"] = snapshot
            return snapshot
            
        except Exception as e:
            print(f"Error capturing Snowflake snapshot: {e}")
            return None
    
    def compare_snapshots(self, old_snapshot: ModelSnapshot, new_snapshot: ModelSnapshot) -> Dict[str, Any]:
        """Compare two snapshots and return the differences."""
        changes = {
            "added_tables": [],
            "removed_tables": [],
            "modified_tables": [],
            "summary": ""
        }
        
        old_tables = {t.name: t for t in old_snapshot.tables}
        new_tables = {t.name: t for t in new_snapshot.tables}
        
        # Find added tables
        for name in new_tables:
            if name not in old_tables:
                changes["added_tables"].append(name)
        
        # Find removed tables
        for name in old_tables:
            if name not in new_tables:
                changes["removed_tables"].append(name)
        
        # Find modified tables
        for name in new_tables:
            if name in old_tables:
                old_cols = {c.name: c for c in old_tables[name].columns}
                new_cols = {c.name: c for c in new_tables[name].columns}
                
                if old_cols != new_cols:
                    changes["modified_tables"].append({
                        "name": name,
                        "added_columns": [c for c in new_cols if c not in old_cols],
                        "removed_columns": [c for c in old_cols if c not in new_cols]
                    })
        
        # Create summary
        parts = []
        if changes["added_tables"]:
            parts.append(f"{len(changes['added_tables'])} tables added")
        if changes["removed_tables"]:
            parts.append(f"{len(changes['removed_tables'])} tables removed")
        if changes["modified_tables"]:
            parts.append(f"{len(changes['modified_tables'])} tables modified")
        
        changes["summary"] = ", ".join(parts) if parts else "No changes detected"
        
        return changes
    
    def get_all_snapshots(self) -> List[ModelSnapshot]:
        """Get all captured snapshots."""
        return list(self.snapshots.values())
