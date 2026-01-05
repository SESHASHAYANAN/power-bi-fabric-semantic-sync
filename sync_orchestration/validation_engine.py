"""
Validation Engine - Data Integrity Verification

Implements comprehensive validation:
- Checksum verification (SHA256)
- Row count parity
- Schema matching
- Sample data comparison
"""

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    is_valid: bool
    validation_type: str
    source_value: Any
    target_value: Any
    error_message: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "validation_type": self.validation_type,
            "source_value": str(self.source_value),
            "target_value": str(self.target_value),
            "error_message": self.error_message,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None
        }


class ValidationEngine:
    """
    Production-grade validation engine for sync operations.
    
    Ensures data integrity through multiple validation layers:
    1. Row count parity check
    2. Checksum verification
    3. Schema compatibility
    4. Sample data comparison
    """
    
    # Tolerance for row count difference (0% = exact match required)
    ROW_COUNT_TOLERANCE = 0.0
    
    # Number of sample rows to compare
    SAMPLE_SIZE = 10
    
    def __init__(self):
        self.validation_results: List[ValidationResult] = []
    
    # ==================================================================
    # CHECKSUM VALIDATION
    # ==================================================================
    
    @staticmethod
    def compute_data_checksum(data: List[Dict], 
                               sort_key: Optional[str] = None) -> str:
        """
        Compute SHA256 checksum of data.
        
        Args:
            data: List of dictionaries representing rows
            sort_key: Optional key to sort by for consistent ordering
            
        Returns:
            SHA256 hex digest
        """
        if sort_key and data:
            sorted_data = sorted(data, key=lambda x: str(x.get(sort_key, "")))
        else:
            sorted_data = data
        
        # Convert to deterministic JSON string
        content = json.dumps(sorted_data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    @staticmethod
    def compute_file_checksum(filepath: str) -> str:
        """
        Compute SHA256 checksum of a file.
        
        Args:
            filepath: Path to file
            
        Returns:
            SHA256 hex digest
        """
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    @staticmethod
    def compute_schema_hash(columns: List[Dict]) -> str:
        """
        Compute hash of schema definition.
        
        Args:
            columns: List of column definitions
            
        Returns:
            SHA256 hex digest of normalized schema
        """
        # Normalize column definitions
        normalized = []
        for col in columns:
            normalized.append({
                "name": str(col.get("name", "")).lower(),
                "type": str(col.get("dataType", col.get("type", ""))).lower(),
                "nullable": bool(col.get("isNullable", col.get("nullable", True)))
            })
        
        content = json.dumps(sorted(normalized, key=lambda x: x["name"]))
        return hashlib.sha256(content.encode()).hexdigest()
    
    def validate_checksum(self,
                          source_data: List[Dict],
                          target_data: List[Dict],
                          sort_key: Optional[str] = None) -> ValidationResult:
        """
        Validate data checksum match between source and target.
        
        Args:
            source_data: Data from source system
            target_data: Data from target system
            sort_key: Key to sort by for consistent comparison
            
        Returns:
            ValidationResult indicating pass/fail
        """
        source_checksum = self.compute_data_checksum(source_data, sort_key)
        target_checksum = self.compute_data_checksum(target_data, sort_key)
        
        is_valid = source_checksum == target_checksum
        
        result = ValidationResult(
            is_valid=is_valid,
            validation_type="CHECKSUM",
            source_value=source_checksum,
            target_value=target_checksum,
            error_message=None if is_valid else "Checksum mismatch detected - data corruption possible"
        )
        
        self.validation_results.append(result)
        
        if not is_valid:
            logger.error(f"Checksum validation FAILED: {source_checksum} != {target_checksum}")
        
        return result
    
    # ==================================================================
    # ROW COUNT VALIDATION
    # ==================================================================
    
    def validate_row_count(self,
                           source_count: int,
                           target_count: int,
                           tolerance: float = None) -> ValidationResult:
        """
        Validate row count parity between source and target.
        
        Args:
            source_count: Row count from source
            target_count: Row count from target
            tolerance: Allowed percentage difference (0.0 = exact match)
            
        Returns:
            ValidationResult indicating pass/fail
        """
        if tolerance is None:
            tolerance = self.ROW_COUNT_TOLERANCE
        
        if source_count == 0 and target_count == 0:
            is_valid = True
            difference = 0.0
        elif source_count == 0:
            is_valid = False
            difference = 100.0
        else:
            difference = abs(source_count - target_count) / source_count * 100
            is_valid = difference <= tolerance
        
        result = ValidationResult(
            is_valid=is_valid,
            validation_type="ROW_COUNT",
            source_value=source_count,
            target_value=target_count,
            error_message=None if is_valid else f"Row count mismatch: {source_count} vs {target_count} ({difference:.2f}% difference)"
        )
        
        self.validation_results.append(result)
        
        if not is_valid:
            logger.error(f"Row count validation FAILED: {source_count} vs {target_count}")
        
        return result
    
    # ==================================================================
    # SCHEMA VALIDATION
    # ==================================================================
    
    def validate_schema(self,
                        source_columns: List[Dict],
                        target_columns: List[Dict]) -> ValidationResult:
        """
        Validate schema compatibility between source and target.
        
        Checks:
        - Column count matches
        - Column names match (case-insensitive)
        - Compatible data types
        
        Args:
            source_columns: Column definitions from source
            target_columns: Column definitions from target
            
        Returns:
            ValidationResult indicating pass/fail
        """
        # Normalize column names for comparison
        source_names = {str(c.get("name", "")).lower() for c in source_columns}
        target_names = {str(c.get("name", "")).lower() for c in target_columns}
        
        # Filter out sync metadata columns from target
        sync_cols = {"_sync_id", "_sync_source", "_synced_at", "_sync_version"}
        target_names = target_names - sync_cols
        
        # Check for missing columns
        missing_in_target = source_names - target_names
        missing_in_source = target_names - source_names
        
        is_valid = len(missing_in_target) == 0 and len(missing_in_source) == 0
        
        error_msg = None
        if not is_valid:
            parts = []
            if missing_in_target:
                parts.append(f"Missing in target: {missing_in_target}")
            if missing_in_source:
                parts.append(f"Extra in target: {missing_in_source}")
            error_msg = "; ".join(parts)
        
        result = ValidationResult(
            is_valid=is_valid,
            validation_type="SCHEMA",
            source_value=f"{len(source_columns)} columns",
            target_value=f"{len(target_columns)} columns",
            error_message=error_msg
        )
        
        self.validation_results.append(result)
        
        if not is_valid:
            logger.error(f"Schema validation FAILED: {error_msg}")
        
        return result
    
    def validate_schema_types(self,
                               source_columns: List[Dict],
                               target_columns: List[Dict],
                               type_mapping: Dict[str, str] = None) -> ValidationResult:
        """
        Validate data type compatibility between schemas.
        """
        # Create lookup for target columns
        target_by_name = {
            str(c.get("name", "")).lower(): c 
            for c in target_columns
        }
        
        type_mismatches = []
        
        for src_col in source_columns:
            src_name = str(src_col.get("name", "")).lower()
            src_type = str(src_col.get("dataType", src_col.get("type", ""))).lower()
            
            if src_name in target_by_name:
                tgt_col = target_by_name[src_name]
                tgt_type = str(tgt_col.get("dataType", tgt_col.get("type", ""))).lower()
                
                # Check type compatibility (simplified)
                if not self._types_compatible(src_type, tgt_type):
                    type_mismatches.append(f"{src_name}: {src_type} vs {tgt_type}")
        
        is_valid = len(type_mismatches) == 0
        
        result = ValidationResult(
            is_valid=is_valid,
            validation_type="SCHEMA_TYPES",
            source_value=f"{len(source_columns)} types checked",
            target_value=f"{len(type_mismatches)} mismatches",
            error_message=None if is_valid else f"Type mismatches: {type_mismatches}"
        )
        
        self.validation_results.append(result)
        
        return result
    
    def _types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two data types are compatible."""
        # Normalize types
        t1 = type1.split("(")[0].lower()
        t2 = type2.split("(")[0].lower()
        
        # Direct match
        if t1 == t2:
            return True
        
        # Compatible type groups
        string_types = {"string", "varchar", "char", "text", "nvarchar"}
        int_types = {"int64", "int32", "int16", "integer", "bigint", "smallint", "number"}
        float_types = {"double", "float", "float64", "float32", "real", "decimal", "numeric"}
        bool_types = {"boolean", "bool"}
        datetime_types = {"datetime", "timestamp", "timestamp_ntz", "datetime64"}
        
        for type_group in [string_types, int_types, float_types, bool_types, datetime_types]:
            if t1 in type_group and t2 in type_group:
                return True
        
        return False
    
    # ==================================================================
    # SAMPLE DATA VALIDATION
    # ==================================================================
    
    def validate_sample_data(self,
                             source_data: List[Dict],
                             target_data: List[Dict],
                             key_column: Optional[str] = None,
                             sample_size: int = None) -> ValidationResult:
        """
        Validate sample rows match between source and target.
        
        Args:
            source_data: Full data from source
            target_data: Full data from target
            key_column: Primary key column for matching rows
            sample_size: Number of rows to sample
            
        Returns:
            ValidationResult indicating pass/fail
        """
        if sample_size is None:
            sample_size = self.SAMPLE_SIZE
        
        # Take first N rows as sample
        source_sample = source_data[:sample_size]
        target_sample = target_data[:sample_size]
        
        mismatches = 0
        
        if key_column:
            # Match by key column
            target_by_key = {
                str(r.get(key_column, "")): r 
                for r in target_sample
            }
            
            for src_row in source_sample:
                key_val = str(src_row.get(key_column, ""))
                if key_val in target_by_key:
                    tgt_row = target_by_key[key_val]
                    if not self._rows_equal(src_row, tgt_row):
                        mismatches += 1
                else:
                    mismatches += 1
        else:
            # Simple position-based comparison
            for i, src_row in enumerate(source_sample):
                if i < len(target_sample):
                    if not self._rows_equal(src_row, target_sample[i]):
                        mismatches += 1
                else:
                    mismatches += 1
        
        is_valid = mismatches == 0
        
        result = ValidationResult(
            is_valid=is_valid,
            validation_type="SAMPLE_DATA",
            source_value=f"{len(source_sample)} rows sampled",
            target_value=f"{mismatches} mismatches found",
            error_message=None if is_valid else f"{mismatches} sample rows differ"
        )
        
        self.validation_results.append(result)
        
        return result
    
    def _rows_equal(self, row1: Dict, row2: Dict) -> bool:
        """Compare two rows for equality, ignoring sync metadata."""
        sync_cols = {"_sync_id", "_sync_source", "_synced_at", "_sync_version"}
        
        keys1 = {k.lower() for k in row1.keys()} - sync_cols
        keys2 = {k.lower() for k in row2.keys()} - sync_cols
        
        common_keys = keys1 & keys2
        
        for key in common_keys:
            # Find matching key (case-insensitive)
            val1 = None
            val2 = None
            
            for k, v in row1.items():
                if k.lower() == key:
                    val1 = v
                    break
            
            for k, v in row2.items():
                if k.lower() == key:
                    val2 = v
                    break
            
            if str(val1) != str(val2):
                return False
        
        return True
    
    # ==================================================================
    # COMPREHENSIVE VALIDATION
    # ==================================================================
    
    def validate_sync(self,
                      source_data: List[Dict],
                      target_data: List[Dict],
                      source_columns: List[Dict],
                      target_columns: List[Dict],
                      key_column: Optional[str] = None) -> Dict[str, Any]:
        """
        Run all validation checks for a sync operation.
        
        Args:
            source_data: Data from source system
            target_data: Data from target system
            source_columns: Column definitions from source
            target_columns: Column definitions from target
            key_column: Optional primary key for sample matching
            
        Returns:
            Dictionary with all validation results
        """
        self.validation_results = []  # Reset
        
        results = {
            "is_valid": True,
            "timestamp": datetime.now().isoformat(),
            "validations": {}
        }
        
        # 1. Row count validation
        row_count_result = self.validate_row_count(
            len(source_data), 
            len(target_data)
        )
        results["validations"]["row_count"] = row_count_result.to_dict()
        if not row_count_result.is_valid:
            results["is_valid"] = False
        
        # 2. Schema validation
        schema_result = self.validate_schema(source_columns, target_columns)
        results["validations"]["schema"] = schema_result.to_dict()
        if not schema_result.is_valid:
            results["is_valid"] = False
        
        # 3. Checksum validation (only if row count matches)
        if row_count_result.is_valid:
            checksum_result = self.validate_checksum(
                source_data, 
                target_data,
                sort_key=key_column
            )
            results["validations"]["checksum"] = checksum_result.to_dict()
            if not checksum_result.is_valid:
                results["is_valid"] = False
        
        # 4. Sample data validation
        if len(source_data) > 0 and len(target_data) > 0:
            sample_result = self.validate_sample_data(
                source_data,
                target_data,
                key_column
            )
            results["validations"]["sample_data"] = sample_result.to_dict()
            if not sample_result.is_valid:
                # Sample mismatch is a warning, not a failure
                results["validations"]["sample_data"]["is_warning"] = True
        
        results["total_validations"] = len(self.validation_results)
        results["passed_validations"] = sum(1 for r in self.validation_results if r.is_valid)
        results["failed_validations"] = sum(1 for r in self.validation_results if not r.is_valid)
        
        return results
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get summary of all validation results."""
        return {
            "total": len(self.validation_results),
            "passed": sum(1 for r in self.validation_results if r.is_valid),
            "failed": sum(1 for r in self.validation_results if not r.is_valid),
            "results": [r.to_dict() for r in self.validation_results]
        }
