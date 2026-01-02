"""
Change Detection Module for Fabric-Snowflake Semantic Sync.

This module provides functionality to:
- Capture point-in-time snapshots of semantic models/views
- Compare snapshots to detect changes
- Generate human-readable change reports
- Support bidirectional change detection
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("fabric_snowflake_sync.change_detector")


# =============================================================================
# ENUMS
# =============================================================================


class ChangeType(Enum):
    """Types of changes that can be detected."""
    
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"


class SourceSystem(Enum):
    """Source system for snapshots."""
    
    FABRIC = "fabric"
    SNOWFLAKE = "snowflake"


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class MeasureSnapshot:
    """
    Snapshot of a single measure definition.
    
    Attributes:
        name: Measure name.
        expression: DAX or SQL expression.
        format_string: Display format.
        description: Optional description.
        table_name: Parent table name.
    """
    
    name: str
    expression: str
    format_string: str = ""
    description: str = ""
    table_name: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MeasureSnapshot":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class ColumnSnapshot:
    """
    Snapshot of a single column definition.
    
    Attributes:
        name: Column name.
        data_type: Data type string.
        is_hidden: Whether column is hidden.
        description: Optional description.
        table_name: Parent table name.
    """
    
    name: str
    data_type: str
    is_hidden: bool = False
    description: str = ""
    table_name: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnSnapshot":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class TableSnapshot:
    """
    Snapshot of a single table definition.
    
    Attributes:
        name: Table name.
        columns: List of column snapshots.
        measures: List of measure snapshots.
        description: Optional description.
    """
    
    name: str
    columns: List[ColumnSnapshot] = field(default_factory=list)
    measures: List[MeasureSnapshot] = field(default_factory=list)
    description: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "measures": [m.to_dict() for m in self.measures],
            "description": self.description,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableSnapshot":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            columns=[ColumnSnapshot.from_dict(c) for c in data.get("columns", [])],
            measures=[MeasureSnapshot.from_dict(m) for m in data.get("measures", [])],
            description=data.get("description", ""),
        )


@dataclass
class SchemaSnapshot:
    """
    Complete snapshot of a semantic model/view schema.
    
    Attributes:
        source: Source system (FABRIC or SNOWFLAKE).
        model_name: Name of the semantic model or view.
        model_id: Unique identifier.
        tables: List of table snapshots.
        timestamp: When snapshot was captured.
        metadata: Additional metadata.
    """
    
    source: SourceSystem
    model_name: str
    model_id: str
    tables: List[TableSnapshot] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "source": self.source.value,
            "model_name": self.model_name,
            "model_id": self.model_id,
            "tables": [t.to_dict() for t in self.tables],
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SchemaSnapshot":
        """Create from dictionary."""
        return cls(
            source=SourceSystem(data["source"]),
            model_name=data["model_name"],
            model_id=data["model_id"],
            tables=[TableSnapshot.from_dict(t) for t in data.get("tables", [])],
            timestamp=data.get("timestamp", datetime.now().isoformat()),
            metadata=data.get("metadata", {}),
        )
    
    @classmethod
    def from_json(cls, json_str: str) -> "SchemaSnapshot":
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    def get_all_measures(self) -> List[MeasureSnapshot]:
        """Get all measures across all tables."""
        measures = []
        for table in self.tables:
            for measure in table.measures:
                measure.table_name = table.name
                measures.append(measure)
        return measures
    
    def get_measure(self, table_name: str, measure_name: str) -> Optional[MeasureSnapshot]:
        """Get a specific measure by table and name."""
        for table in self.tables:
            if table.name == table_name:
                for measure in table.measures:
                    if measure.name == measure_name:
                        return measure
        return None


@dataclass
class ChangeRecord:
    """
    Record of a single change detected.
    
    Attributes:
        change_type: Type of change (ADDED, REMOVED, MODIFIED).
        item_type: Type of item changed (measure, column, table).
        item_name: Name of the changed item.
        table_name: Parent table name.
        before_value: Value before change (for MODIFIED/REMOVED).
        after_value: Value after change (for MODIFIED/ADDED).
        source: Source system where change originated.
    """
    
    change_type: ChangeType
    item_type: str
    item_name: str
    table_name: str
    before_value: Optional[Any] = None
    after_value: Optional[Any] = None
    source: Optional[SourceSystem] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "change_type": self.change_type.value,
            "item_type": self.item_type,
            "item_name": self.item_name,
            "table_name": self.table_name,
            "before_value": self.before_value,
            "after_value": self.after_value,
            "source": self.source.value if self.source else None,
        }


@dataclass
class ChangeReport:
    """
    Complete report of changes detected between snapshots.
    
    Attributes:
        source_snapshot: The source snapshot.
        target_snapshot: The target snapshot.
        changes: List of change records.
        generated_at: When report was generated.
        summary: Summary statistics.
    """
    
    source_snapshot: SchemaSnapshot
    target_snapshot: SchemaSnapshot
    changes: List[ChangeRecord] = field(default_factory=list)
    generated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    summary: Dict[str, int] = field(default_factory=dict)
    
    def __post_init__(self):
        """Calculate summary after initialization."""
        self._calculate_summary()
    
    def _calculate_summary(self) -> None:
        """Calculate summary statistics from changes."""
        self.summary = {
            "total": len(self.changes),
            "added": sum(1 for c in self.changes if c.change_type == ChangeType.ADDED),
            "removed": sum(1 for c in self.changes if c.change_type == ChangeType.REMOVED),
            "modified": sum(1 for c in self.changes if c.change_type == ChangeType.MODIFIED),
        }
    
    def has_changes(self) -> bool:
        """Check if any changes were detected."""
        return len(self.changes) > 0
    
    def get_measure_changes(self) -> List[ChangeRecord]:
        """Get only measure-related changes."""
        return [c for c in self.changes if c.item_type == "measure"]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "source_snapshot": self.source_snapshot.to_dict(),
            "target_snapshot": self.target_snapshot.to_dict(),
            "changes": [c.to_dict() for c in self.changes],
            "generated_at": self.generated_at,
            "summary": self.summary,
        }
    
    def format_report(self) -> str:
        """Generate a human-readable report string."""
        lines = [
            "=" * 80,
            "CHANGE DETECTION REPORT",
            "=" * 80,
            f"Generated At: {self.generated_at}",
            f"Source: {self.source_snapshot.source.value.upper()} - {self.source_snapshot.model_name}",
            f"Target: {self.target_snapshot.source.value.upper()} - {self.target_snapshot.model_name}",
            "",
        ]
        
        if not self.has_changes():
            lines.append("No changes detected.")
        else:
            # Group changes by type
            added = [c for c in self.changes if c.change_type == ChangeType.ADDED]
            modified = [c for c in self.changes if c.change_type == ChangeType.MODIFIED]
            removed = [c for c in self.changes if c.change_type == ChangeType.REMOVED]
            
            if modified:
                lines.append("MODIFIED:")
                for change in modified:
                    lines.append(f"  - [{change.item_type.upper()}] {change.table_name}.{change.item_name}")
                    if change.item_type == "measure":
                        lines.append(f"    Before: {change.before_value}")
                        lines.append(f"    After:  {change.after_value}")
                lines.append("")
            
            if added:
                lines.append("ADDED:")
                for change in added:
                    lines.append(f"  - [{change.item_type.upper()}] {change.table_name}.{change.item_name}")
                    if change.after_value:
                        lines.append(f"    Value: {change.after_value}")
                lines.append("")
            
            if removed:
                lines.append("REMOVED:")
                for change in removed:
                    lines.append(f"  - [{change.item_type.upper()}] {change.table_name}.{change.item_name}")
                lines.append("")
        
        lines.extend([
            "-" * 80,
            "SUMMARY:",
            f"  Total Changes: {self.summary['total']}",
            f"  Additions:     {self.summary['added']}",
            f"  Modifications: {self.summary['modified']}",
            f"  Deletions:     {self.summary['removed']}",
            "=" * 80,
        ])
        
        return "\n".join(lines)


# =============================================================================
# CHANGE DETECTOR CLASS
# =============================================================================


class ChangeDetector:
    """
    Detects changes between Fabric semantic models and Snowflake views.
    
    Provides functionality to:
    - Capture snapshots from both systems
    - Compare snapshots to detect changes
    - Generate detailed change reports
    """
    
    def __init__(
        self,
        fabric_client: Optional[Any] = None,
        snowflake_connector: Optional[Any] = None,
    ) -> None:
        """
        Initialize the change detector.
        
        Args:
            fabric_client: Optional FabricApiClient instance.
            snowflake_connector: Optional SnowflakeConnector instance.
        """
        self.fabric_client = fabric_client
        self.snowflake_connector = snowflake_connector
        self._snapshots: Dict[str, SchemaSnapshot] = {}
        
        logger.info("ChangeDetector initialized")
    
    def capture_fabric_snapshot(
        self,
        model_id: str,
        model_data: Optional[Dict[str, Any]] = None,
    ) -> Optional[SchemaSnapshot]:
        """
        Capture a snapshot of a Fabric semantic model.
        
        Args:
            model_id: The semantic model ID.
            model_data: Optional pre-fetched model data.
        
        Returns:
            SchemaSnapshot if successful, None otherwise.
        """
        logger.info(f"📸 Capturing Fabric snapshot for model: {model_id}")
        
        try:
            # Get model data if not provided
            if model_data is None and self.fabric_client:
                model_data = self.fabric_client.get_semantic_model_detail(model_id)
            
            if model_data is None:
                logger.error("Failed to get model data")
                return None
            
            # Parse tables
            tables: List[TableSnapshot] = []
            for table_data in model_data.get("tables", []):
                # Parse columns
                columns = [
                    ColumnSnapshot(
                        name=col.get("name", ""),
                        data_type=col.get("dataType", "string"),
                        is_hidden=col.get("isHidden", False),
                        description=col.get("description", ""),
                        table_name=table_data.get("name", ""),
                    )
                    for col in table_data.get("columns", [])
                ]
                
                # Parse measures
                measures = [
                    MeasureSnapshot(
                        name=m.get("name", ""),
                        expression=m.get("expression", ""),
                        format_string=m.get("formatString", ""),
                        description=m.get("description", ""),
                        table_name=table_data.get("name", ""),
                    )
                    for m in table_data.get("measures", [])
                ]
                
                table = TableSnapshot(
                    name=table_data.get("name", ""),
                    columns=columns,
                    measures=measures,
                    description=table_data.get("description", ""),
                )
                tables.append(table)
            
            snapshot = SchemaSnapshot(
                source=SourceSystem.FABRIC,
                model_name=model_data.get("name", ""),
                model_id=model_id,
                tables=tables,
                metadata={
                    "workspace_id": model_data.get("workspaceId", ""),
                    "modified_date": model_data.get("modifiedDate", ""),
                },
            )
            
            # Store snapshot
            self._snapshots[f"fabric_{model_id}"] = snapshot
            
            logger.info(f"✅ Captured Fabric snapshot: {len(tables)} table(s)")
            return snapshot
            
        except Exception as e:
            logger.error(f"❌ Failed to capture Fabric snapshot: {e}")
            return None
    
    def capture_snowflake_snapshot(
        self,
        view_name: str,
        view_data: Optional[Dict[str, Any]] = None,
    ) -> Optional[SchemaSnapshot]:
        """
        Capture a snapshot of a Snowflake semantic view.
        
        Args:
            view_name: The semantic view name.
            view_data: Optional pre-fetched view data.
        
        Returns:
            SchemaSnapshot if successful, None otherwise.
        """
        logger.info(f"📸 Capturing Snowflake snapshot for view: {view_name}")
        
        try:
            columns: List[ColumnSnapshot] = []
            measures: List[MeasureSnapshot] = []
            
            # Get view definition from Snowflake if connector available
            if view_data is None and self.snowflake_connector:
                # Get column info from INFORMATION_SCHEMA
                query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{view_name.upper()}'
                ORDER BY ORDINAL_POSITION
                """
                results = self.snowflake_connector.execute_query(query, fetch_all=True)
                
                if results:
                    for row in results:
                        columns.append(ColumnSnapshot(
                            name=row.get("COLUMN_NAME", ""),
                            data_type=row.get("DATA_TYPE", ""),
                            description=row.get("COMMENT", "") or "",
                            table_name=view_name,
                        ))
                
                # Get view DDL for measure definitions
                ddl_query = f"SELECT GET_DDL('VIEW', '{view_name}') AS DDL"
                ddl_result = self.snowflake_connector.execute_query(ddl_query)
                
                if ddl_result and ddl_result[0]:
                    ddl = ddl_result[0].get("DDL", "")
                    measures = self._parse_measures_from_ddl(ddl, view_name)
            
            elif view_data:
                # Use provided data
                columns = [
                    ColumnSnapshot.from_dict(c)
                    for c in view_data.get("columns", [])
                ]
                measures = [
                    MeasureSnapshot.from_dict(m)
                    for m in view_data.get("measures", [])
                ]
            
            table = TableSnapshot(
                name=view_name,
                columns=columns,
                measures=measures,
            )
            
            snapshot = SchemaSnapshot(
                source=SourceSystem.SNOWFLAKE,
                model_name=view_name,
                model_id=view_name,
                tables=[table],
            )
            
            # Store snapshot
            self._snapshots[f"snowflake_{view_name}"] = snapshot
            
            logger.info(f"✅ Captured Snowflake snapshot: {len(columns)} column(s), {len(measures)} measure(s)")
            return snapshot
            
        except Exception as e:
            logger.error(f"❌ Failed to capture Snowflake snapshot: {e}")
            return None
    
    def _parse_measures_from_ddl(
        self,
        ddl: str,
        view_name: str,
    ) -> List[MeasureSnapshot]:
        """
        Parse measure definitions from view DDL.
        
        Args:
            ddl: The view DDL statement.
            view_name: Name of the view.
        
        Returns:
            List of MeasureSnapshot objects.
        """
        measures = []
        
        # Look for common aggregate patterns
        import re
        
        # Pattern: expression AS alias
        pattern = r"(SUM|COUNT|AVG|MIN|MAX|COUNT\s*\(\s*DISTINCT)\s*\([^)]+\)\s+AS\s+(\w+)"
        matches = re.findall(pattern, ddl, re.IGNORECASE)
        
        for expr_type, alias in matches:
            # Find the full expression
            full_pattern = rf"({expr_type}\s*\([^)]+\))\s+AS\s+{alias}"
            full_match = re.search(full_pattern, ddl, re.IGNORECASE)
            if full_match:
                measures.append(MeasureSnapshot(
                    name=alias,
                    expression=full_match.group(1),
                    table_name=view_name,
                ))
        
        return measures
    
    def compare_snapshots(
        self,
        source: SchemaSnapshot,
        target: SchemaSnapshot,
    ) -> ChangeReport:
        """
        Compare two snapshots and generate a change report.
        
        Args:
            source: The source snapshot (before state or origin system).
            target: The target snapshot (after state or destination system).
        
        Returns:
            ChangeReport with detected changes.
        """
        logger.info(f"🔍 Comparing snapshots: {source.model_name} vs {target.model_name}")
        
        changes: List[ChangeRecord] = []
        
        # Compare measures
        measure_changes = self._compare_measures(source, target)
        changes.extend(measure_changes)
        
        # Compare columns
        column_changes = self._compare_columns(source, target)
        changes.extend(column_changes)
        
        report = ChangeReport(
            source_snapshot=source,
            target_snapshot=target,
            changes=changes,
        )
        
        logger.info(f"✅ Comparison complete: {len(changes)} change(s) detected")
        return report
    
    def _compare_measures(
        self,
        source: SchemaSnapshot,
        target: SchemaSnapshot,
    ) -> List[ChangeRecord]:
        """
        Compare measures between two snapshots.
        
        Args:
            source: Source snapshot.
            target: Target snapshot.
        
        Returns:
            List of ChangeRecord for measure differences.
        """
        changes = []
        
        source_measures = {
            f"{m.table_name}.{m.name}": m for m in source.get_all_measures()
        }
        target_measures = {
            f"{m.table_name}.{m.name}": m for m in target.get_all_measures()
        }
        
        # Find added measures
        for key, measure in target_measures.items():
            if key not in source_measures:
                changes.append(ChangeRecord(
                    change_type=ChangeType.ADDED,
                    item_type="measure",
                    item_name=measure.name,
                    table_name=measure.table_name,
                    after_value=measure.expression,
                    source=target.source,
                ))
        
        # Find removed measures
        for key, measure in source_measures.items():
            if key not in target_measures:
                changes.append(ChangeRecord(
                    change_type=ChangeType.REMOVED,
                    item_type="measure",
                    item_name=measure.name,
                    table_name=measure.table_name,
                    before_value=measure.expression,
                    source=source.source,
                ))
        
        # Find modified measures
        for key, source_measure in source_measures.items():
            if key in target_measures:
                target_measure = target_measures[key]
                
                # Normalize expressions for comparison
                source_expr = self._normalize_expression(source_measure.expression)
                target_expr = self._normalize_expression(target_measure.expression)
                
                if source_expr != target_expr:
                    changes.append(ChangeRecord(
                        change_type=ChangeType.MODIFIED,
                        item_type="measure",
                        item_name=source_measure.name,
                        table_name=source_measure.table_name,
                        before_value=source_measure.expression,
                        after_value=target_measure.expression,
                        source=target.source,
                    ))
        
        return changes
    
    def _compare_columns(
        self,
        source: SchemaSnapshot,
        target: SchemaSnapshot,
    ) -> List[ChangeRecord]:
        """
        Compare columns between two snapshots.
        
        Args:
            source: Source snapshot.
            target: Target snapshot.
        
        Returns:
            List of ChangeRecord for column differences.
        """
        changes = []
        
        # Build column maps
        source_columns = {}
        target_columns = {}
        
        for table in source.tables:
            for col in table.columns:
                source_columns[f"{table.name}.{col.name}"] = col
        
        for table in target.tables:
            for col in table.columns:
                target_columns[f"{table.name}.{col.name}"] = col
        
        # Find added columns
        for key, col in target_columns.items():
            if key not in source_columns:
                table_name = key.split(".")[0]
                changes.append(ChangeRecord(
                    change_type=ChangeType.ADDED,
                    item_type="column",
                    item_name=col.name,
                    table_name=table_name,
                    after_value=col.data_type,
                    source=target.source,
                ))
        
        # Find removed columns
        for key, col in source_columns.items():
            if key not in target_columns:
                table_name = key.split(".")[0]
                changes.append(ChangeRecord(
                    change_type=ChangeType.REMOVED,
                    item_type="column",
                    item_name=col.name,
                    table_name=table_name,
                    before_value=col.data_type,
                    source=source.source,
                ))
        
        return changes
    
    def _normalize_expression(self, expression: str) -> str:
        """
        Normalize an expression for comparison.
        
        Removes whitespace differences and normalizes case.
        
        Args:
            expression: The expression string.
        
        Returns:
            Normalized expression string.
        """
        import re
        # Remove extra whitespace
        normalized = re.sub(r"\s+", " ", expression.strip())
        # Normalize case for common keywords
        keywords = ["SUM", "COUNT", "AVG", "MIN", "MAX", "CALCULATE", "FILTER"]
        for kw in keywords:
            normalized = re.sub(rf"\b{kw}\b", kw, normalized, flags=re.IGNORECASE)
        return normalized
    
    def detect_changes_bidirectional(
        self,
        fabric_snapshot: SchemaSnapshot,
        snowflake_snapshot: SchemaSnapshot,
    ) -> Tuple[ChangeReport, ChangeReport]:
        """
        Detect changes in both directions.
        
        Args:
            fabric_snapshot: Fabric semantic model snapshot.
            snowflake_snapshot: Snowflake semantic view snapshot.
        
        Returns:
            Tuple of (fabric_to_snowflake_report, snowflake_to_fabric_report).
        """
        logger.info("🔄 Running bidirectional change detection")
        
        # Fabric → Snowflake: What's in Fabric that differs from Snowflake
        fabric_to_snowflake = self.compare_snapshots(snowflake_snapshot, fabric_snapshot)
        
        # Snowflake → Fabric: What's in Snowflake that differs from Fabric
        snowflake_to_fabric = self.compare_snapshots(fabric_snapshot, snowflake_snapshot)
        
        return fabric_to_snowflake, snowflake_to_fabric
    
    def get_stored_snapshot(self, key: str) -> Optional[SchemaSnapshot]:
        """
        Get a previously captured snapshot.
        
        Args:
            key: Snapshot key (e.g., 'fabric_model_id' or 'snowflake_view_name').
        
        Returns:
            SchemaSnapshot if found, None otherwise.
        """
        return self._snapshots.get(key)
    
    def save_snapshot(self, snapshot: SchemaSnapshot, filepath: str) -> bool:
        """
        Save a snapshot to a JSON file.
        
        Args:
            snapshot: The snapshot to save.
            filepath: Path to save the JSON file.
        
        Returns:
            True if successful, False otherwise.
        """
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(snapshot.to_json())
            logger.info(f"✅ Saved snapshot to {filepath}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to save snapshot: {e}")
            return False
    
    def load_snapshot(self, filepath: str) -> Optional[SchemaSnapshot]:
        """
        Load a snapshot from a JSON file.
        
        Args:
            filepath: Path to the JSON file.
        
        Returns:
            SchemaSnapshot if successful, None otherwise.
        """
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return SchemaSnapshot.from_json(f.read())
        except Exception as e:
            logger.error(f"❌ Failed to load snapshot: {e}")
            return None
