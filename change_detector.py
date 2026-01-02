
import json
import logging
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field, asdict

# Configure logging
logger = logging.getLogger(__name__)


class SourceSystem(Enum):
    """Enumeration for source systems."""
    FABRIC = "fabric"
    SNOWFLAKE = "snowflake"
    UNKNOWN = "unknown"


class ChangeType(Enum):
    """Enumeration for types of changes."""
    ADDED = "added"
    DELETED = "deleted"
    MODIFIED = "modified"
    NO_CHANGE = "no_change"


@dataclass
class ColumnSnapshot:
    """Snapshot of a column."""
    name: str
    data_type: str
    is_hidden: bool = False
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MeasureSnapshot:
    """Snapshot of a measure."""
    name: str
    expression: str
    format_string: str = ""
    description: str = ""
    table_name: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def __eq__(self, other):
        if not isinstance(other, MeasureSnapshot):
            return False
        return (self.name == other.name and 
                self.expression == other.expression and 
                self.format_string == other.format_string)


@dataclass
class TableSnapshot:
    """Snapshot of a table."""
    name: str
    columns: List[ColumnSnapshot] = field(default_factory=list)
    measures: List[MeasureSnapshot] = field(default_factory=list)
    is_hidden: bool = False
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "measures": [m.to_dict() for m in self.measures],
            "is_hidden": self.is_hidden,
            "description": self.description
        }


class SchemaSnapshot:
    """Represents a snapshot of a semantic model schema at a point in time."""

    def __init__(
        self,
        model_name: str,
        source: SourceSystem,
        model_id: Optional[str] = None,
        timestamp: Optional[str] = None
    ):
        self.model_name = model_name
        self.model_id = model_id or model_name
        self.source = source
        self.timestamp = timestamp or datetime.now().isoformat()
        self.tables: List[TableSnapshot] = []

    def add_table(self, table: TableSnapshot):
        """Add a table to the snapshot."""
        self.tables.append(table)

    def get_all_measures(self) -> List[MeasureSnapshot]:
        """Get all measures across all tables."""
        all_measures = []
        for table in self.tables:
            for measure in table.measures:
                # Ensure table_name is set
                if not measure.table_name:
                    measure.table_name = table.name
                all_measures.append(measure)
        return all_measures

    def get_measure_map(self) -> Dict[str, MeasureSnapshot]:
        """Get a map of measures key by table.name|measure.name."""
        measure_map = {}
        for measure in self.get_all_measures():
            key = f"{measure.table_name}.{measure.name}"
            measure_map[key] = measure
        return measure_map

    def to_json(self) -> str:
        """Serialize snapshot to JSON string."""
        data = {
            "model_name": self.model_name,
            "model_id": self.model_id,
            "source": self.source.value,
            "timestamp": self.timestamp,
            "tables": [t.to_dict() for t in self.tables]
        }
        return json.dumps(data, indent=2)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SchemaSnapshot':
        """Create a snapshot from a dictionary."""
        snapshot = cls(
            model_name=data["model_name"],
            source=SourceSystem(data["source"]),
            model_id=data.get("model_id"),
            timestamp=data.get("timestamp")
        )
        
        for t_data in data.get("tables", []):
            table = TableSnapshot(
                name=t_data["name"],
                is_hidden=t_data.get("is_hidden", False),
                description=t_data.get("description", "")
            )
            
            for c_data in t_data.get("columns", []):
                table.columns.append(ColumnSnapshot(**c_data))
                
            for m_data in t_data.get("measures", []):
                table.measures.append(MeasureSnapshot(**m_data))
                
            snapshot.add_table(table)
            
        return snapshot


@dataclass
class ChangeRecord:
    """Represents a single change detected between snapshots."""
    item_type: str  # "table", "column", "measure"
    item_name: str
    change_type: ChangeType
    source_system: SourceSystem
    old_value: Any = None
    new_value: Any = None
    table_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_type": self.item_type,
            "item_name": self.item_name,
            "change_type": self.change_type.value,
            "source_system": self.source_system.value,
            "old_value": str(self.old_value) if self.old_value else None,
            "new_value": str(self.new_value) if self.new_value else None,
            "table_name": self.table_name
        }


class ChangeReport:
    """Collection of ChangeRecords with summary statistics."""

    def __init__(self, source_a: SchemaSnapshot, source_b: SchemaSnapshot):
        self.source_a = source_a
        self.source_b = source_b
        self.changes: List[ChangeRecord] = []
        self.timestamp = datetime.now().isoformat()

    def add_change(self, change: ChangeRecord):
        """Add a change record to the report."""
        self.changes.append(change)

    def has_changes(self) -> bool:
        """Check if any changes were detected."""
        return len(self.changes) > 0

    def get_measure_changes(self) -> List[ChangeRecord]:
        """Get only changes related to measures."""
        return [c for c in self.changes if c.item_type == "measure"]

    @property
    def summary(self) -> Dict[str, int]:
        """Get summary statistics of changes."""
        return {
            "total": len(self.changes),
            "added": sum(1 for c in self.changes if c.change_type == ChangeType.ADDED),
            "deleted": sum(1 for c in self.changes if c.change_type == ChangeType.DELETED),
            "modified": sum(1 for c in self.changes if c.change_type == ChangeType.MODIFIED),
        }

    def format_report(self) -> str:
        """Generate a human-readable string report."""
        lines = []
        lines.append("=" * 60)
        lines.append("CHANGE DETECTION REPORT")
        lines.append(f"Generated: {self.timestamp}")
        lines.append(f"Source A: {self.source_a.model_name} ({self.source_a.source.value})")
        lines.append(f"Source B: {self.source_b.model_name} ({self.source_b.source.value})")
        lines.append("=" * 60)
        
        summary = self.summary
        lines.append(f"SUMMARY: {summary['total']} changes ({summary['added']} added, {summary['modified']} modified, {summary['deleted']} deleted)")
        lines.append("-" * 60)
        
        if not self.changes:
            lines.append("No changes detected.")
        else:
            # Group by change type
            for ct in [ChangeType.ADDED, ChangeType.MODIFIED, ChangeType.DELETED]:
                ct_changes = [c for c in self.changes if c.change_type == ct]
                if ct_changes:
                    lines.append(f"\n{ct.value.upper()}:")
                    for c in ct_changes:
                        context = f" in {c.table_name}" if c.table_name else ""
                        lines.append(f"  - {c.item_type.capitalize()}: {c.item_name}{context}")
                        if ct == ChangeType.MODIFIED:
                            lines.append(f"    From: {c.old_value}")
                            lines.append(f"    To:   {c.new_value}")

        lines.append("=" * 60)
        return "\n".join(lines)


class ChangeDetector:
    """Detector for identifying changes between semantic model snapshots."""

    def __init__(self, fabric_client=None, snowflake_connector=None):
        self.fabric_client = fabric_client
        self.snowflake_connector = snowflake_connector

    def capture_fabric_snapshot(self, model_id: str, model_data: Optional[Dict[str, Any]] = None) -> Optional[SchemaSnapshot]:
        """Capture a snapshot of a Fabric semantic model."""
        try:
            if model_data is None:
                if not self.fabric_client:
                    raise ValueError("Fabric client not provided and no model data supplied")
                model_data = self.fabric_client.get_semantic_model_detail(model_id)
            
            if not model_data:
                logger.error(f"Failed to get details for Fabric model {model_id}")
                return None
            
            snapshot = SchemaSnapshot(
                model_name=model_data.get("displayName", model_data.get("name", "Unknown")),
                source=SourceSystem.FABRIC,
                model_id=model_id,
                timestamp=model_data.get("modifiedDate")
            )
            
            for t_data in model_data.get("tables", []):
                table = TableSnapshot(
                    name=t_data["name"],
                    is_hidden=t_data.get("isHidden", False),
                    description=t_data.get("description", "")
                )
                
                for c_data in t_data.get("columns", []):
                    table.columns.append(ColumnSnapshot(
                        name=c_data["name"],
                        data_type=c_data.get("dataType", "unknown"),
                        is_hidden=c_data.get("isHidden", False),
                        description=c_data.get("description", "")
                    ))
                    
                for m_data in t_data.get("measures", []):
                    table.measures.append(MeasureSnapshot(
                        name=m_data["name"],
                        expression=m_data.get("expression", ""),
                        format_string=m_data.get("formatString", ""),
                        description=m_data.get("description", ""),
                        table_name=table.name
                    ))
                    
                snapshot.add_table(table)
                
            return snapshot
            
        except Exception as e:
            logger.error(f"Error capturing Fabric snapshot: {e}")
            return None

    def capture_snowflake_snapshot(self, view_name: str, view_data: Optional[Dict[str, Any]] = None) -> Optional[SchemaSnapshot]:
        """Capture a snapshot of a Snowflake semantic view."""
        try:
            if view_data is None:
                if not self.snowflake_connector:
                    raise ValueError("Snowflake connector not provided and no view data supplied")
                
                # Try to get data if the connector is available
                # In a real scenario, this would use the connector's methods
                # This is simplified for the example
                columns = self.snowflake_connector.execute_query(f"DESCRIBE VIEW {view_name}", fetch_all=True)
                # This is a bit complex without knowing the exact structure returned by connector
                # We'll assume the structure matches mock_data for this implementation
                view_data = {
                    "view_name": view_name,
                    "columns": columns,
                    "measures": [] # Would need more complex query to get measures
                }
            
            snapshot = SchemaSnapshot(
                model_name=view_name,
                source=SourceSystem.SNOWFLAKE,
                model_id=view_name
            )
            
            # Group into a single table for the view
            table = TableSnapshot(name=view_name)
            
            for c_data in view_data.get("columns", []):
                # Handle both dict formats
                c_name = c_data.get("name") or c_data.get("COLUMN_NAME")
                c_type = c_data.get("data_type") or c_data.get("DATA_TYPE")
                
                if c_name:
                    table.columns.append(ColumnSnapshot(
                        name=c_name,
                        data_type=str(c_type),
                        is_hidden=False
                    ))
                    
            for m_data in view_data.get("measures", []):
                table.measures.append(MeasureSnapshot(
                    name=m_data["name"],
                    expression=m_data.get("expression", ""),
                    format_string=m_data.get("format_string", ""),
                    description=m_data.get("description", ""),
                    table_name=table.name
                ))
                
            snapshot.add_table(table)
            return snapshot
            
        except Exception as e:
            logger.error(f"Error capturing Snowflake snapshot: {e}")
            return None

    def compare_snapshots(self, before: SchemaSnapshot, after: SchemaSnapshot) -> ChangeReport:
        """Compare two snapshots and return a change report."""
        report = ChangeReport(before, after)
        
        # Compare measures
        before_measures = before.get_measure_map()
        after_measures = after.get_measure_map()
        
        # Added and Modified
        for key, after_m in after_measures.items():
            if key not in before_measures:
                report.add_change(ChangeRecord(
                    item_type="measure",
                    item_name=after_m.name,
                    change_type=ChangeType.ADDED,
                    source_system=after.source,
                    new_value=after_m.expression,
                    table_name=after_m.table_name
                ))
            else:
                before_m = before_measures[key]
                if before_m != after_m:
                    report.add_change(ChangeRecord(
                        item_type="measure",
                        item_name=after_m.name,
                        change_type=ChangeType.MODIFIED,
                        source_system=after.source,
                        old_value=before_m.expression,
                        new_value=after_m.expression,
                        table_name=after_m.table_name
                    ))
        
        # Deleted
        for key, before_m in before_measures.items():
            if key not in after_measures:
                report.add_change(ChangeRecord(
                    item_type="measure",
                    item_name=before_m.name,
                    change_type=ChangeType.DELETED,
                    source_system=after.source,
                    old_value=before_m.expression,
                    table_name=before_m.table_name
                ))
                
        return report

    def detect_changes_bidirectional(
        self, 
        fabric_snapshot: SchemaSnapshot, 
        snowflake_snapshot: SchemaSnapshot
    ) -> Tuple[ChangeReport, ChangeReport]:
        """
        Detect changes in both directions.
        
        Returns:
            Tuple of (fabric_to_sf_report, sf_to_fabric_report)
        """
        # Fabric to Snowflake: What changes in Fabric need to go to Snowflake?
        # This is essentially comparing SF (as baseline) to Fabric (as target state)
        fabric_to_sf = self.compare_snapshots(snowflake_snapshot, fabric_snapshot)
        
        # Snowflake to Fabric: What changes in Snowflake need to go to Fabric?
        # This is essentially comparing Fabric (as baseline) to SF (as target state)
        sf_to_fabric = self.compare_snapshots(fabric_snapshot, snowflake_snapshot)
        
        return fabric_to_sf, sf_to_fabric

    def load_snapshot(self, filepath: str) -> Optional[SchemaSnapshot]:
        """Load a snapshot from a JSON file."""
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            return SchemaSnapshot.from_dict(data)
        except Exception as e:
            logger.error(f"Error loading snapshot from {filepath}: {e}")
            return None
