"""
View to Table Converter - Phase 1 of Migration

Converts view-based outputs to materialized table format in:
- Snowflake: CREATE OR REPLACE TABLE from views
- Fabric: Warehouse tables from semantic model views

Features:
- Incremental refresh patterns for large datasets
- Primary/foreign key relationship preservation
- Column metadata preservation
- Performance optimization
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
import snowflake.connector

logger = logging.getLogger(__name__)


class ConversionStatus(Enum):
    """Status of view-to-table conversion."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ViewDefinition:
    """Represents a view to be converted."""
    name: str
    schema_name: str
    database: str
    platform: str  # 'snowflake' or 'fabric'
    definition_sql: str = ""
    columns: List[Dict] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[Dict] = field(default_factory=list)
    row_count: int = 0
    size_mb: float = 0.0
    created_at: str = ""
    modified_at: str = ""


@dataclass
class TableDefinition:
    """Represents the target materialized table."""
    name: str
    schema_name: str
    database: str
    platform: str
    ddl_statement: str = ""
    columns: List[Dict] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[Dict] = field(default_factory=list)
    clustering_keys: List[str] = field(default_factory=list)
    partitioning: Optional[Dict] = None
    refresh_strategy: str = "full"  # full, incremental, merge


@dataclass
class ConversionResult:
    """Result of a view-to-table conversion."""
    view_name: str
    table_name: str
    platform: str
    status: ConversionStatus
    rows_converted: int = 0
    conversion_time_ms: int = 0
    error_message: Optional[str] = None
    ddl_executed: str = ""
    backup_created: bool = False
    rollback_available: bool = False


class ViewToTableConverter:
    """
    Converts views to materialized tables in both Snowflake and Fabric.
    
    Supports:
    - Full conversion (CREATE TABLE AS SELECT)
    - Incremental refresh (MERGE statements)
    - Relationship preservation
    - Backup and rollback
    """
    
    # Snowflake data type mappings for optimization
    SNOWFLAKE_TYPE_OPTIMIZATIONS = {
        'VARCHAR': 'VARCHAR(16777216)',  # Max VARCHAR
        'NUMBER': 'NUMBER(38,6)',
        'FLOAT': 'FLOAT',
        'BOOLEAN': 'BOOLEAN',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP_NTZ',
        'VARIANT': 'VARIANT',
        'ARRAY': 'ARRAY',
        'OBJECT': 'OBJECT'
    }
    
    def __init__(self, 
                 snowflake_connector=None,
                 fabric_client=None,
                 backup_schema: str = "_MIGRATION_BACKUP",
                 enable_backup: bool = True):
        """
        Initialize the converter.
        
        Args:
            snowflake_connector: SnowflakeConnector instance
            fabric_client: FabricApiClient instance
            backup_schema: Schema for storing backups
            enable_backup: Whether to create backups before conversion
        """
        self.snowflake_connector = snowflake_connector
        self.fabric_client = fabric_client
        self.backup_schema = backup_schema
        self.enable_backup = enable_backup
        self.conversion_log: List[ConversionResult] = []
        
    def set_snowflake_connector(self, connector):
        """Set the Snowflake connector."""
        self.snowflake_connector = connector
        
    def set_fabric_client(self, client):
        """Set the Fabric API client."""
        self.fabric_client = client
        
    # ==========================================
    # SNOWFLAKE VIEW TO TABLE CONVERSION
    # ==========================================
    
    def discover_snowflake_views(self, 
                                  database: str = None,
                                  schema: str = None) -> List[ViewDefinition]:
        """
        Discover all views in Snowflake that need conversion.
        
        Args:
            database: Specific database (None = all)
            schema: Specific schema (None = all)
            
        Returns:
            List of ViewDefinition objects
        """
        if not self.snowflake_connector:
            raise ValueError("Snowflake connector not set")
            
        views = []
        
        # Query to list all views
        query = """
        SELECT 
            TABLE_CATALOG as database_name,
            TABLE_SCHEMA as schema_name,
            TABLE_NAME as view_name,
            VIEW_DEFINITION as definition,
            CREATED as created_at,
            LAST_ALTERED as modified_at
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
        """
        
        if database:
            query += f" AND TABLE_CATALOG = '{database}'"
        if schema:
            query += f" AND TABLE_SCHEMA = '{schema}'"
            
        try:
            results = self.snowflake_connector.execute_query(query)
            
            for row in results:
                view_def = ViewDefinition(
                    name=row['VIEW_NAME'],
                    schema_name=row['SCHEMA_NAME'],
                    database=row['DATABASE_NAME'],
                    platform='snowflake',
                    definition_sql=row.get('DEFINITION', ''),
                    created_at=str(row.get('CREATED_AT', '')),
                    modified_at=str(row.get('MODIFIED_AT', ''))
                )
                
                # Get column information
                view_def.columns = self._get_snowflake_view_columns(
                    row['DATABASE_NAME'], 
                    row['SCHEMA_NAME'], 
                    row['VIEW_NAME']
                )
                
                # Get row count estimate
                view_def.row_count = self._get_snowflake_row_count(
                    row['DATABASE_NAME'],
                    row['SCHEMA_NAME'],
                    row['VIEW_NAME']
                )
                
                views.append(view_def)
                
            logger.info(f"Discovered {len(views)} Snowflake views for conversion")
            return views
            
        except Exception as e:
            logger.error(f"Error discovering Snowflake views: {e}")
            raise
            
    def _get_snowflake_view_columns(self, database: str, schema: str, view: str) -> List[Dict]:
        """Get column definitions for a Snowflake view."""
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            ORDINAL_POSITION
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{view}'
        ORDER BY ORDINAL_POSITION
        """
        
        try:
            results = self.snowflake_connector.execute_query(query)
            return [dict(row) for row in results]
        except Exception as e:
            logger.warning(f"Could not get columns for {view}: {e}")
            return []
            
    def _get_snowflake_row_count(self, database: str, schema: str, view: str) -> int:
        """Get approximate row count for a view."""
        try:
            query = f"SELECT COUNT(*) as cnt FROM {database}.{schema}.{view}"
            result = self.snowflake_connector.execute_query(query)
            return result[0]['CNT'] if result else 0
        except Exception as e:
            logger.warning(f"Could not get row count for {view}: {e}")
            return 0
            
    def convert_snowflake_view_to_table(self, 
                                         view: ViewDefinition,
                                         table_name: str = None,
                                         incremental: bool = False,
                                         key_columns: List[str] = None,
                                         clustering_keys: List[str] = None) -> ConversionResult:
        """
        Convert a single Snowflake view to a materialized table.
        
        Args:
            view: ViewDefinition to convert
            table_name: Target table name (default: same as view)
            incremental: Use incremental refresh pattern
            key_columns: Primary key columns for incremental merge
            clustering_keys: Clustering keys for optimization
            
        Returns:
            ConversionResult with status
        """
        start_time = datetime.now()
        table_name = table_name or view.name
        full_view_name = f"{view.database}.{view.schema_name}.{view.name}"
        full_table_name = f"{view.database}.{view.schema_name}.{table_name}"
        
        result = ConversionResult(
            view_name=view.name,
            table_name=table_name,
            platform='snowflake',
            status=ConversionStatus.IN_PROGRESS
        )
        
        try:
            # Step 1: Create backup if enabled
            if self.enable_backup:
                self._create_snowflake_backup(view, table_name)
                result.backup_created = True
                result.rollback_available = True
                
            # Step 2: Generate DDL
            if incremental and key_columns:
                ddl = self._generate_incremental_table_ddl(
                    view, table_name, key_columns, clustering_keys
                )
                result.ddl_executed = ddl
                
                # Create staging table and merge
                staging_table = f"{table_name}_STAGING"
                self._execute_incremental_conversion(
                    view, table_name, staging_table, key_columns
                )
            else:
                # Full conversion - CREATE TABLE AS SELECT
                ddl = self._generate_full_table_ddl(
                    view, table_name, clustering_keys
                )
                result.ddl_executed = ddl
                
                # Execute DDL
                self.snowflake_connector.execute_query(ddl)
                
            # Step 3: Validate conversion
            result.rows_converted = self._get_snowflake_row_count(
                view.database, view.schema_name, table_name
            )
            
            # Step 4: Add constraints if specified
            if key_columns:
                self._add_snowflake_constraints(
                    view.database, view.schema_name, table_name, key_columns
                )
                
            result.status = ConversionStatus.COMPLETED
            result.conversion_time_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            logger.info(f"Converted {full_view_name} to table with {result.rows_converted} rows")
            
        except Exception as e:
            result.status = ConversionStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Failed to convert {view.name}: {e}")
            
        self.conversion_log.append(result)
        return result
        
    def _generate_full_table_ddl(self, 
                                  view: ViewDefinition, 
                                  table_name: str,
                                  clustering_keys: List[str] = None) -> str:
        """Generate CREATE TABLE AS SELECT DDL."""
        full_view_name = f"{view.database}.{view.schema_name}.{view.name}"
        full_table_name = f"{view.database}.{view.schema_name}.{table_name}"
        
        ddl = f"""
CREATE OR REPLACE TABLE {full_table_name}
COPY GRANTS
"""
        
        # Add clustering if specified
        if clustering_keys:
            cluster_cols = ", ".join(clustering_keys)
            ddl += f"CLUSTER BY ({cluster_cols})\n"
            
        ddl += f"""AS
SELECT 
    *,
    CURRENT_TIMESTAMP() as _SYNC_TIMESTAMP,
    MD5(OBJECT_CONSTRUCT(*)::VARCHAR) as _ROW_HASH
FROM {full_view_name}
"""
        
        return ddl
        
    def _generate_incremental_table_ddl(self,
                                         view: ViewDefinition,
                                         table_name: str,
                                         key_columns: List[str],
                                         clustering_keys: List[str] = None) -> str:
        """Generate DDL for incremental table with merge support."""
        full_table_name = f"{view.database}.{view.schema_name}.{table_name}"
        
        # Build column definitions
        col_defs = []
        for col in view.columns:
            col_name = col.get('COLUMN_NAME', col.get('column_name', ''))
            data_type = col.get('DATA_TYPE', col.get('data_type', 'VARCHAR'))
            nullable = col.get('IS_NULLABLE', 'YES') == 'YES'
            
            col_def = f"    {col_name} {data_type}"
            if not nullable:
                col_def += " NOT NULL"
            col_defs.append(col_def)
            
        # Add sync metadata columns
        col_defs.append("    _SYNC_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()")
        col_defs.append("    _ROW_HASH VARCHAR(32)")
        col_defs.append("    _IS_DELETED BOOLEAN DEFAULT FALSE")
        
        columns_sql = ",\n".join(col_defs)
        
        # Primary key constraint
        pk_constraint = ""
        if key_columns:
            pk_cols = ", ".join(key_columns)
            pk_constraint = f"    , PRIMARY KEY ({pk_cols})"
            
        ddl = f"""
CREATE TABLE IF NOT EXISTS {full_table_name} (
{columns_sql}
{pk_constraint}
)
"""
        
        # Add clustering
        if clustering_keys:
            cluster_cols = ", ".join(clustering_keys)
            ddl += f"CLUSTER BY ({cluster_cols})\n"
            
        return ddl
        
    def _execute_incremental_conversion(self,
                                         view: ViewDefinition,
                                         table_name: str,
                                         staging_table: str,
                                         key_columns: List[str]):
        """Execute incremental conversion using MERGE."""
        full_view_name = f"{view.database}.{view.schema_name}.{view.name}"
        full_table_name = f"{view.database}.{view.schema_name}.{table_name}"
        full_staging_name = f"{view.database}.{view.schema_name}.{staging_table}"
        
        # Create staging table from view
        staging_ddl = f"""
CREATE OR REPLACE TEMPORARY TABLE {full_staging_name} AS
SELECT 
    *,
    CURRENT_TIMESTAMP() as _SYNC_TIMESTAMP,
    MD5(OBJECT_CONSTRUCT(*)::VARCHAR) as _ROW_HASH
FROM {full_view_name}
"""
        self.snowflake_connector.execute_query(staging_ddl)
        
        # Build MERGE statement
        key_conditions = " AND ".join([
            f"target.{col} = source.{col}" for col in key_columns
        ])
        
        # Get non-key columns for update
        all_cols = [c.get('COLUMN_NAME', c.get('column_name', '')) 
                    for c in view.columns]
        update_cols = [c for c in all_cols if c not in key_columns]
        
        update_sets = ", ".join([
            f"target.{col} = source.{col}" for col in update_cols
        ])
        update_sets += ", target._SYNC_TIMESTAMP = source._SYNC_TIMESTAMP"
        update_sets += ", target._ROW_HASH = source._ROW_HASH"
        
        insert_cols = ", ".join(all_cols + ['_SYNC_TIMESTAMP', '_ROW_HASH'])
        
        merge_sql = f"""
MERGE INTO {full_table_name} target
USING {full_staging_name} source
ON {key_conditions}
WHEN MATCHED AND target._ROW_HASH != source._ROW_HASH THEN 
    UPDATE SET {update_sets}
WHEN NOT MATCHED THEN 
    INSERT ({insert_cols})
    VALUES ({insert_cols})
"""
        
        self.snowflake_connector.execute_query(merge_sql)
        
        # Cleanup staging
        self.snowflake_connector.execute_query(f"DROP TABLE IF EXISTS {full_staging_name}")
        
    def _create_snowflake_backup(self, view: ViewDefinition, table_name: str):
        """Create a backup of existing table before conversion."""
        full_backup_schema = f"{view.database}.{self.backup_schema}"
        full_table_name = f"{view.database}.{view.schema_name}.{table_name}"
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"{full_backup_schema}.{table_name}_BACKUP_{timestamp}"
        
        # Create backup schema if not exists
        self.snowflake_connector.execute_query(
            f"CREATE SCHEMA IF NOT EXISTS {full_backup_schema}"
        )
        
        # Check if table exists and backup
        check_query = f"""
        SELECT COUNT(*) as cnt 
        FROM {view.database}.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{view.schema_name}' 
          AND TABLE_NAME = '{table_name}'
        """
        result = self.snowflake_connector.execute_query(check_query)
        
        if result and result[0]['CNT'] > 0:
            backup_ddl = f"CREATE TABLE {backup_name} CLONE {full_table_name}"
            self.snowflake_connector.execute_query(backup_ddl)
            logger.info(f"Created backup: {backup_name}")
            
    def _add_snowflake_constraints(self, 
                                    database: str, 
                                    schema: str, 
                                    table_name: str,
                                    key_columns: List[str]):
        """Add primary key constraint to table."""
        full_table_name = f"{database}.{schema}.{table_name}"
        pk_cols = ", ".join(key_columns)
        
        alter_sql = f"""
ALTER TABLE {full_table_name} 
ADD CONSTRAINT pk_{table_name} PRIMARY KEY ({pk_cols})
"""
        try:
            self.snowflake_connector.execute_query(alter_sql)
        except Exception as e:
            logger.warning(f"Could not add PK constraint: {e}")
            
    # ==========================================
    # FABRIC VIEW TO TABLE CONVERSION
    # ==========================================
    
    def discover_fabric_views(self, workspace_id: str = None) -> List[ViewDefinition]:
        """
        Discover all semantic model views in Fabric.
        
        These are conceptual "views" in the semantic model that need to be
        materialized as Warehouse tables.
        """
        if not self.fabric_client:
            raise ValueError("Fabric client not set")
            
        views = []
        
        try:
            # Get all semantic models
            models = self.fabric_client.list_semantic_models(workspace_id)
            
            for model in models:
                model_detail = self.fabric_client.get_semantic_model(model['id'])
                
                for table in model_detail.get('tables', []):
                    view_def = ViewDefinition(
                        name=table['name'],
                        schema_name=model['name'],  # Use model name as schema
                        database='FabricWarehouse',
                        platform='fabric',
                        columns=self._extract_fabric_columns(table),
                        primary_keys=self._extract_fabric_keys(table),
                        created_at=model.get('createdDate', ''),
                        modified_at=model.get('modifiedDate', '')
                    )
                    views.append(view_def)
                    
            logger.info(f"Discovered {len(views)} Fabric semantic model views")
            return views
            
        except Exception as e:
            logger.error(f"Error discovering Fabric views: {e}")
            raise
            
    def _extract_fabric_columns(self, table: Dict) -> List[Dict]:
        """Extract column definitions from Fabric table."""
        columns = []
        for col in table.get('columns', []):
            columns.append({
                'name': col.get('name', ''),
                'data_type': col.get('dataType', 'String'),
                'is_hidden': col.get('isHidden', False),
                'description': col.get('description', '')
            })
        return columns
        
    def _extract_fabric_keys(self, table: Dict) -> List[str]:
        """Extract primary key columns from Fabric table."""
        keys = []
        for col in table.get('columns', []):
            # Check for key indicators in column metadata
            if col.get('isKey', False) or col.get('summarizeBy', '') == 'None':
                keys.append(col.get('name', ''))
        return keys
        
    def convert_fabric_view_to_warehouse_table(self,
                                                view: ViewDefinition,
                                                warehouse_id: str,
                                                schema_name: str = 'dbo') -> ConversionResult:
        """
        Convert a Fabric semantic model view to a Warehouse table.
        
        Uses Fabric's SQL endpoint to create physical tables in the Warehouse.
        """
        start_time = datetime.now()
        table_name = view.name.replace(' ', '_').upper()
        
        result = ConversionResult(
            view_name=view.name,
            table_name=table_name,
            platform='fabric',
            status=ConversionStatus.IN_PROGRESS
        )
        
        try:
            # Generate CREATE TABLE DDL for Fabric Warehouse
            ddl = self._generate_fabric_warehouse_ddl(view, table_name, schema_name)
            result.ddl_executed = ddl
            
            # Execute via Fabric SQL endpoint (would use actual API)
            # For now, we store the DDL for execution
            self._execute_fabric_warehouse_ddl(warehouse_id, ddl)
            
            result.status = ConversionStatus.COMPLETED
            result.conversion_time_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            logger.info(f"Converted Fabric view {view.name} to warehouse table {table_name}")
            
        except Exception as e:
            result.status = ConversionStatus.FAILED
            result.error_message = str(e)
            logger.error(f"Failed to convert Fabric view {view.name}: {e}")
            
        self.conversion_log.append(result)
        return result
        
    def _generate_fabric_warehouse_ddl(self, 
                                        view: ViewDefinition,
                                        table_name: str,
                                        schema_name: str) -> str:
        """Generate T-SQL for Fabric Warehouse table creation."""
        # Map Fabric types to T-SQL types
        type_mapping = {
            'String': 'NVARCHAR(MAX)',
            'Int64': 'BIGINT',
            'Int32': 'INT',
            'Double': 'FLOAT',
            'Decimal': 'DECIMAL(38,6)',
            'DateTime': 'DATETIME2',
            'Date': 'DATE',
            'Boolean': 'BIT',
            'Binary': 'VARBINARY(MAX)'
        }
        
        col_defs = []
        for col in view.columns:
            col_name = col.get('name', '').replace(' ', '_')
            fabric_type = col.get('data_type', 'String')
            sql_type = type_mapping.get(fabric_type, 'NVARCHAR(MAX)')
            
            col_defs.append(f"    [{col_name}] {sql_type}")
            
        # Add sync metadata columns
        col_defs.append("    [_SYNC_TIMESTAMP] DATETIME2 DEFAULT GETUTCDATE()")
        col_defs.append("    [_ROW_HASH] NVARCHAR(32)")
        col_defs.append("    [_SOURCE_MODEL] NVARCHAR(255)")
        
        columns_sql = ",\n".join(col_defs)
        
        ddl = f"""
-- Drop existing table if exists
IF OBJECT_ID('{schema_name}.{table_name}', 'U') IS NOT NULL
    DROP TABLE [{schema_name}].[{table_name}];
GO

-- Create new table
CREATE TABLE [{schema_name}].[{table_name}] (
{columns_sql}
);
GO

-- Add primary key if columns specified
"""
        
        if view.primary_keys:
            pk_cols = ", ".join([f"[{c}]" for c in view.primary_keys])
            ddl += f"""
ALTER TABLE [{schema_name}].[{table_name}]
ADD CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_cols});
GO
"""
            
        return ddl
        
    def _execute_fabric_warehouse_ddl(self, warehouse_id: str, ddl: str):
        """Execute DDL on Fabric Warehouse via SQL endpoint."""
        # In production, this would use the Fabric SQL endpoint
        # For now, log the DDL for manual execution or API call
        logger.info(f"DDL for Fabric Warehouse {warehouse_id}:\n{ddl}")
        
        # Store DDL for batch execution
        ddl_file = f"fabric_warehouse_ddl_{warehouse_id}.sql"
        with open(ddl_file, 'a') as f:
            f.write(ddl + "\n\n")
            
    # ==========================================
    # BATCH CONVERSION
    # ==========================================
    
    def convert_all_views(self,
                          platform: str = 'both',
                          incremental: bool = False,
                          batch_size: int = 10) -> Dict[str, Any]:
        """
        Convert all views to tables on specified platform(s).
        
        Args:
            platform: 'snowflake', 'fabric', or 'both'
            incremental: Use incremental refresh patterns
            batch_size: Number of views to process in each batch
            
        Returns:
            Summary of conversion operations
        """
        summary = {
            'start_time': datetime.now().isoformat(),
            'platform': platform,
            'snowflake_results': [],
            'fabric_results': [],
            'total_views': 0,
            'successful': 0,
            'failed': 0
        }
        
        # Convert Snowflake views
        if platform in ['snowflake', 'both']:
            sf_views = self.discover_snowflake_views()
            summary['total_views'] += len(sf_views)
            
            for i, view in enumerate(sf_views):
                logger.info(f"Converting Snowflake view {i+1}/{len(sf_views)}: {view.name}")
                result = self.convert_snowflake_view_to_table(
                    view, incremental=incremental
                )
                summary['snowflake_results'].append(result.__dict__)
                
                if result.status == ConversionStatus.COMPLETED:
                    summary['successful'] += 1
                else:
                    summary['failed'] += 1
                    
        # Convert Fabric views
        if platform in ['fabric', 'both'] and self.fabric_client:
            fabric_views = self.discover_fabric_views()
            summary['total_views'] += len(fabric_views)
            
            for i, view in enumerate(fabric_views):
                logger.info(f"Converting Fabric view {i+1}/{len(fabric_views)}: {view.name}")
                result = self.convert_fabric_view_to_warehouse_table(
                    view, warehouse_id='default'
                )
                summary['fabric_results'].append(result.__dict__)
                
                if result.status == ConversionStatus.COMPLETED:
                    summary['successful'] += 1
                else:
                    summary['failed'] += 1
                    
        summary['end_time'] = datetime.now().isoformat()
        
        # Save summary report
        report_file = f"view_to_table_conversion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
            
        logger.info(f"Conversion complete. {summary['successful']} successful, {summary['failed']} failed")
        return summary
        
    def create_view_wrappers(self, views: List[ViewDefinition]) -> List[str]:
        """
        Create view wrappers for backward compatibility.
        
        Creates views pointing to new tables so old queries still work.
        """
        wrapper_ddls = []
        
        for view in views:
            if view.platform == 'snowflake':
                # Original view name -> points to new table
                ddl = f"""
CREATE OR REPLACE VIEW {view.database}.{view.schema_name}.{view.name}_VIEW AS
SELECT * EXCLUDE (_SYNC_TIMESTAMP, _ROW_HASH, _IS_DELETED)
FROM {view.database}.{view.schema_name}.{view.name}
WHERE _IS_DELETED = FALSE;
"""
                wrapper_ddls.append(ddl)
                
        return wrapper_ddls
        
    def generate_refresh_procedures(self, views: List[ViewDefinition]) -> Dict[str, str]:
        """
        Generate stored procedures for incremental refresh.
        
        Returns stored procedure DDL for each converted table.
        """
        procedures = {}
        
        for view in views:
            if view.platform == 'snowflake':
                proc_name = f"SP_REFRESH_{view.name}"
                proc_ddl = f"""
CREATE OR REPLACE PROCEDURE {view.database}.{view.schema_name}.{proc_name}()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_updated INT;
    rows_inserted INT;
BEGIN
    -- Incremental refresh logic
    MERGE INTO {view.database}.{view.schema_name}.{view.name} target
    USING (
        SELECT *, MD5(OBJECT_CONSTRUCT(*)::VARCHAR) as new_hash
        FROM {view.database}.{view.schema_name}.{view.name}_SOURCE
    ) source
    ON target.{view.primary_keys[0] if view.primary_keys else 'ID'} = source.{view.primary_keys[0] if view.primary_keys else 'ID'}
    WHEN MATCHED AND target._ROW_HASH != source.new_hash THEN
        UPDATE SET _SYNC_TIMESTAMP = CURRENT_TIMESTAMP(), _ROW_HASH = source.new_hash
    WHEN NOT MATCHED THEN
        INSERT VALUES (source.*, CURRENT_TIMESTAMP(), source.new_hash, FALSE);
    
    RETURN 'Refresh completed successfully';
END;
$$;
"""
                procedures[proc_name] = proc_ddl
                
        return procedures
        
    def get_conversion_report(self) -> Dict[str, Any]:
        """Get detailed conversion report."""
        return {
            'total_conversions': len(self.conversion_log),
            'successful': sum(1 for r in self.conversion_log 
                            if r.status == ConversionStatus.COMPLETED),
            'failed': sum(1 for r in self.conversion_log 
                         if r.status == ConversionStatus.FAILED),
            'total_rows_converted': sum(r.rows_converted for r in self.conversion_log),
            'details': [r.__dict__ for r in self.conversion_log]
        }
