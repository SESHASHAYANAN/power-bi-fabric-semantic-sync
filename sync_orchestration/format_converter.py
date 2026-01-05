"""
Format Converter - Schema Transformation Between Fabric and Snowflake

Handles bidirectional type mapping and DDL generation with proper
handling of edge cases and complex types.
"""

import re
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class FormatConverter:
    """
    Bidirectional format converter between Fabric and Snowflake.
    
    Handles:
    - Data type mapping (both directions)
    - Schema transformation
    - DDL generation
    - Data serialization for transfer
    """
    
    # ==================================================================
    # TYPE MAPPINGS
    # ==================================================================
    
    # Fabric TMSL Types -> Snowflake Types
    FABRIC_TO_SNOWFLAKE: Dict[str, str] = {
        # String types
        "String": "VARCHAR(16777216)",
        "string": "VARCHAR(16777216)",
        "Text": "TEXT",
        
        # Numeric types
        "Int64": "BIGINT",
        "Int32": "INTEGER",
        "Int16": "SMALLINT",
        "int64": "BIGINT",
        "int32": "INTEGER",
        "int16": "SMALLINT",
        
        # Floating point
        "Double": "DOUBLE",
        "Float": "FLOAT",
        "float64": "DOUBLE",
        "float32": "FLOAT",
        
        # Decimal/Currency
        "Decimal": "DECIMAL(38,10)",
        "Currency": "DECIMAL(19,4)",
        "Percentage": "DECIMAL(10,4)",
        
        # Boolean
        "Boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        
        # DateTime types
        "DateTime": "TIMESTAMP_NTZ",
        "Date": "DATE",
        "Time": "TIME",
        "datetime64": "TIMESTAMP_NTZ",
        "datetime64[ns]": "TIMESTAMP_NTZ",
        
        # Binary
        "Binary": "BINARY",
        
        # Complex types - serialize to JSON
        "Array": "VARIANT",
        "Object": "VARIANT",
        "Variant": "VARIANT",
        
        # Pandas types
        "object": "VARCHAR(16777216)",
        "category": "VARCHAR(16777216)",
    }
    
    # Snowflake Types -> Fabric Types
    SNOWFLAKE_TO_FABRIC: Dict[str, str] = {
        # String types
        "VARCHAR": "String",
        "CHAR": "String",
        "STRING": "String",
        "TEXT": "String",
        
        # Numeric types
        "NUMBER": "Decimal",
        "NUMERIC": "Decimal",
        "INTEGER": "Int64",
        "INT": "Int64",
        "BIGINT": "Int64",
        "SMALLINT": "Int16",
        "TINYINT": "Int16",
        
        # Floating point
        "FLOAT": "Double",
        "FLOAT4": "Double",
        "FLOAT8": "Double",
        "DOUBLE": "Double",
        "DOUBLE PRECISION": "Double",
        "REAL": "Double",
        
        # Boolean
        "BOOLEAN": "Boolean",
        "BOOL": "Boolean",
        
        # DateTime types
        "TIMESTAMP": "DateTime",
        "TIMESTAMP_NTZ": "DateTime",
        "TIMESTAMP_LTZ": "DateTime",
        "TIMESTAMP_TZ": "DateTime",
        "DATE": "Date",
        "TIME": "Time",
        
        # Binary
        "BINARY": "Binary",
        "VARBINARY": "Binary",
        
        # Semi-structured
        "VARIANT": "String",  # Serialize as JSON string
        "OBJECT": "String",
        "ARRAY": "String",
    }
    
    # ==================================================================
    # TYPE CONVERSION METHODS
    # ==================================================================
    
    @classmethod
    def fabric_to_snowflake_type(cls, fabric_type: str) -> str:
        """
        Convert Fabric data type to Snowflake equivalent.
        
        Args:
            fabric_type: Fabric data type string
            
        Returns:
            Snowflake data type string
        """
        fabric_type = str(fabric_type).strip()
        
        # Exact match
        if fabric_type in cls.FABRIC_TO_SNOWFLAKE:
            return cls.FABRIC_TO_SNOWFLAKE[fabric_type]
        
        # Handle parameterized types like decimal(10,2)
        base_type = fabric_type.split("(")[0].strip()
        if base_type.lower() in ["decimal", "numeric"]:
            # Preserve precision/scale if specified
            if "(" in fabric_type:
                params = fabric_type.split("(")[1].rstrip(")")
                return f"DECIMAL({params})"
            return "DECIMAL(38,10)"
        
        # Handle array types
        if base_type.lower().startswith("array"):
            return "VARIANT"
        
        # Default to VARCHAR for unknown types
        logger.warning(f"Unknown Fabric type '{fabric_type}', defaulting to VARCHAR")
        return "VARCHAR(16777216)"
    
    @classmethod
    def snowflake_to_fabric_type(cls, snowflake_type: str) -> str:
        """
        Convert Snowflake data type to Fabric equivalent.
        
        Args:
            snowflake_type: Snowflake data type string
            
        Returns:
            Fabric data type string
        """
        snowflake_type = str(snowflake_type).strip().upper()
        
        # Extract base type (remove parameters)
        base_type = snowflake_type.split("(")[0].strip()
        
        # Exact match
        if base_type in cls.SNOWFLAKE_TO_FABRIC:
            return cls.SNOWFLAKE_TO_FABRIC[base_type]
        
        # Handle NUMBER with precision/scale -> map to appropriate type
        if base_type == "NUMBER":
            if "(" in snowflake_type:
                params = snowflake_type.split("(")[1].rstrip(")")
                parts = params.split(",")
                if len(parts) == 2:
                    precision, scale = int(parts[0]), int(parts[1])
                    if scale == 0:
                        # Integer type
                        if precision <= 5:
                            return "Int16"
                        elif precision <= 10:
                            return "Int32"
                        else:
                            return "Int64"
                    else:
                        return "Decimal"
            return "Decimal"
        
        # Default to String for unknown types
        logger.warning(f"Unknown Snowflake type '{snowflake_type}', defaulting to String")
        return "String"
    
    # ==================================================================
    # SCHEMA TRANSFORMATION
    # ==================================================================
    
    @classmethod
    def transform_schema_fabric_to_snowflake(cls, 
                                             columns: List[Dict[str, Any]],
                                             table_name: str) -> Tuple[str, List[Dict]]:
        """
        Transform Fabric schema to Snowflake DDL.
        
        Args:
            columns: List of Fabric column definitions
            table_name: Target table name
            
        Returns:
            Tuple of (DDL string, transformed column list)
        """
        transformed_columns = []
        col_defs = []
        
        for col in columns:
            # Get column name, sanitize for Snowflake
            col_name = cls._sanitize_identifier(col.get("name", "column"))
            fabric_type = col.get("dataType", col.get("data_type", "String"))
            sf_type = cls.fabric_to_snowflake_type(fabric_type)
            
            # Check for nullable
            nullable = col.get("isNullable", col.get("is_nullable", True))
            null_constraint = "" if nullable else " NOT NULL"
            
            col_defs.append(f'"{col_name}" {sf_type}{null_constraint}')
            
            transformed_columns.append({
                "name": col_name,
                "original_name": col.get("name", "column"),
                "snowflake_type": sf_type,
                "fabric_type": fabric_type,
                "is_nullable": nullable
            })
        
        # Add sync metadata columns
        col_defs.append('"_SYNC_ID" VARCHAR(36)')
        col_defs.append('"_SYNC_SOURCE" VARCHAR(50) DEFAULT \'fabric\'')
        col_defs.append('"_SYNCED_AT" TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()')
        col_defs.append('"_SYNC_VERSION" INTEGER DEFAULT 1')
        
        # Generate CREATE TABLE DDL
        safe_table_name = cls._sanitize_identifier(table_name)
        ddl = f'CREATE OR REPLACE TABLE "{safe_table_name}" (\n  '
        ddl += ",\n  ".join(col_defs)
        ddl += "\n)"
        
        return ddl, transformed_columns
    
    @classmethod
    def transform_schema_snowflake_to_fabric(cls,
                                            columns: List[Dict[str, Any]],
                                            table_name: str) -> Dict[str, Any]:
        """
        Transform Snowflake schema to Fabric semantic model definition.
        
        Args:
            columns: List of Snowflake column definitions
            table_name: Source table name
            
        Returns:
            Fabric semantic model definition dictionary
        """
        fabric_columns = []
        
        for col in columns:
            col_name = col.get("name", "column")
            sf_type = col.get("type", col.get("dataType", "VARCHAR"))
            fabric_type = cls.snowflake_to_fabric_type(sf_type)
            
            fabric_columns.append({
                "name": col_name,
                "displayName": col_name.replace("_", " ").title(),
                "dataType": fabric_type,
                "isHidden": col.get("isHidden", False),
                "description": col.get("description", f"Column synced from Snowflake")
            })
        
        # Build semantic model definition
        display_name = table_name.replace("_", " ").title()
        
        model = {
            "name": table_name,
            "displayName": display_name,
            "description": f"Synced from Snowflake on {datetime.now().isoformat()}",
            "tables": [{
                "name": table_name,
                "displayName": display_name,
                "columns": fabric_columns,
                "measures": [],
                "partitions": [{
                    "name": "Partition1",
                    "mode": "import"
                }]
            }],
            "relationships": [],
            "annotations": [{
                "name": "SyncSource",
                "value": "snowflake"
            }, {
                "name": "SyncedAt",
                "value": datetime.now().isoformat()
            }]
        }
        
        return model
    
    # ==================================================================
    # DATA CONVERSION
    # ==================================================================
    
    @classmethod
    def convert_value_for_snowflake(cls, value: Any, fabric_type: str) -> Any:
        """
        Convert a Fabric value to Snowflake-compatible format.
        """
        import pandas as pd
        
        if value is None or (pd and pd.isna(value)):
            return None
        
        fabric_type_lower = str(fabric_type).lower()
        
        # Handle boolean
        if fabric_type_lower == "boolean":
            return bool(value)
        
        # Handle datetime
        if fabric_type_lower in ["datetime", "datetime64", "datetime64[ns]"]:
            if isinstance(value, datetime):
                return value.strftime("%Y-%m-%d %H:%M:%S.%f")
            return str(value)
        
        # Handle complex types - serialize to JSON
        if fabric_type_lower in ["array", "object", "variant"]:
            return json.dumps(value, default=str)
        
        return value
    
    @classmethod
    def convert_value_for_fabric(cls, value: Any, sf_type: str) -> Any:
        """
        Convert a Snowflake value to Fabric-compatible format.
        """
        if value is None:
            return None
        
        sf_type_upper = str(sf_type).upper()
        base_type = sf_type_upper.split("(")[0]
        
        # Handle VARIANT/OBJECT/ARRAY - parse JSON
        if base_type in ["VARIANT", "OBJECT", "ARRAY"]:
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return value
        
        return value
    
    # ==================================================================
    # HELPER METHODS
    # ==================================================================
    
    @classmethod
    def _sanitize_identifier(cls, name: str) -> str:
        """
        Sanitize an identifier for SQL use.
        
        Removes special characters and ensures valid naming.
        """
        # Replace non-alphanumeric with underscore
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
        
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = '_' + sanitized
        
        # Uppercase for Snowflake convention
        return sanitized.upper()
    
    @classmethod
    def generate_copy_into_statement(cls, 
                                      table_name: str,
                                      stage_path: str,
                                      file_format: str = "PARQUET") -> str:
        """
        Generate Snowflake COPY INTO statement for bulk data load.
        """
        safe_table = cls._sanitize_identifier(table_name)
        
        return f"""
COPY INTO "{safe_table}"
FROM @{stage_path}
FILE_FORMAT = (TYPE = {file_format})
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT
FORCE = FALSE
"""
    
    @classmethod
    def generate_merge_statement(cls,
                                  target_table: str,
                                  source_table: str,
                                  key_columns: List[str],
                                  update_columns: List[str]) -> str:
        """
        Generate Snowflake MERGE statement for incremental sync.
        """
        safe_target = cls._sanitize_identifier(target_table)
        safe_source = cls._sanitize_identifier(source_table)
        
        # Build join condition
        join_conditions = " AND ".join([
            f'target."{cls._sanitize_identifier(k)}" = source."{cls._sanitize_identifier(k)}"'
            for k in key_columns
        ])
        
        # Build update set clause
        update_sets = ", ".join([
            f'target."{cls._sanitize_identifier(c)}" = source."{cls._sanitize_identifier(c)}"'
            for c in update_columns
        ])
        
        # Build insert columns and values
        all_columns = key_columns + update_columns
        insert_cols = ", ".join([f'"{cls._sanitize_identifier(c)}"' for c in all_columns])
        insert_vals = ", ".join([f'source."{cls._sanitize_identifier(c)}"' for c in all_columns])
        
        return f"""
MERGE INTO "{safe_target}" AS target
USING "{safe_source}" AS source
ON {join_conditions}
WHEN MATCHED THEN UPDATE SET {update_sets}, target."_SYNCED_AT" = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT ({insert_cols}, "_SYNC_ID", "_SYNCED_AT")
  VALUES ({insert_vals}, source."_SYNC_ID", CURRENT_TIMESTAMP())
"""
    
    @classmethod
    def get_parquet_schema(cls, columns: List[Dict]) -> Dict[str, str]:
        """
        Get PyArrow schema for Parquet file generation.
        """
        import pyarrow as pa
        
        pa_types = {
            "String": pa.string(),
            "Int64": pa.int64(),
            "Int32": pa.int32(),
            "Double": pa.float64(),
            "Boolean": pa.bool_(),
            "DateTime": pa.timestamp('us'),
            "Date": pa.date32(),
            "Binary": pa.binary(),
        }
        
        schema_fields = []
        for col in columns:
            col_name = col.get("name", "column")
            fabric_type = col.get("dataType", "String")
            pa_type = pa_types.get(fabric_type, pa.string())
            schema_fields.append(pa.field(col_name, pa_type))
        
        return pa.schema(schema_fields)
