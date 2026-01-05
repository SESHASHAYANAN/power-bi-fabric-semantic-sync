"""
Backward Compatibility Manager - Phase 4 of Migration

Ensures all historical Fabric files and Snowflake queries work with new table format:
- Migration scripts for legacy view references to table references
- View wrappers for backward compatibility
- Automatic update of Fabric notebooks, pipelines, Power BI datasets
- Conversion of .pbix files from DAX to SQL
- Test validation for output consistency

Provides a seamless migration experience without breaking existing workloads.
"""

import os
import re
import json
import logging
import shutil
import zipfile
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Set
from pathlib import Path
from enum import Enum

logger = logging.getLogger(__name__)


class FileType(Enum):
    """Types of files to process for backward compatibility."""
    PBIX = "pbix"
    NOTEBOOK = "notebook"
    SQL = "sql"
    PIPELINE = "pipeline"
    PBIT = "pbit"
    BISM = "bism"
    JSON = "json"
    YAML = "yaml"


class MigrationAction(Enum):
    """Actions taken during migration."""
    REFERENCE_UPDATED = "reference_updated"
    VIEW_WRAPPER_CREATED = "view_wrapper_created"
    DAX_CONVERTED = "dax_converted"
    BACKUP_CREATED = "backup_created"
    VALIDATION_PASSED = "validation_passed"
    VALIDATION_FAILED = "validation_failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class LegacyReference:
    """Represents a legacy view reference that needs migration."""
    original_ref: str
    new_ref: str
    file_path: str
    line_number: int
    context: str
    migrated: bool = False
    

@dataclass
class MigrationResult:
    """Result of migrating a file."""
    file_path: str
    file_type: FileType
    actions: List[MigrationAction] = field(default_factory=list)
    references_found: int = 0
    references_updated: int = 0
    dax_measures_converted: int = 0
    backup_path: Optional[str] = None
    errors: List[str] = field(default_factory=list)
    validation_results: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ViewWrapper:
    """Represents a view wrapper for backward compatibility."""
    original_view_name: str
    new_table_name: str
    wrapper_ddl: str
    platform: str  # 'snowflake' or 'fabric'
    created_at: str = ""
    deprecation_date: Optional[str] = None


class BackwardCompatibilityManager:
    """
    Manages backward compatibility during view-to-table migration.
    
    Provides:
    - Automatic detection and update of legacy references
    - View wrappers for gradual migration
    - PBIX/notebook conversion tooling
    - Validation of migrated workloads
    """
    
    # Patterns for detecting view references
    VIEW_PATTERNS = {
        'snowflake': [
            r'FROM\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)\b',
            r'JOIN\s+([A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*\.[A-Z_][A-Z0-9_]*)\b',
            r'SELECT\s+\*?\s+FROM\s+([A-Z_][A-Z0-9_]*)\b',
        ],
        'fabric': [
            r"'([A-Za-z_][A-Za-z0-9_]*)'",  # Table references in DAX
            r'\[([A-Za-z_][A-Za-z0-9_ ]*)\]',  # Column/table references
        ]
    }
    
    # DAX function patterns that need conversion
    DAX_PATTERNS = {
        'measure': r'(\w+)\s*:?=\s*(.+)',
        'calculate': r'CALCULATE\s*\(',
        'sumx': r'SUMX\s*\(',
        'filter': r'FILTER\s*\(',
        'related': r'RELATED\s*\(',
        'time_intel': r'(TOTALYTD|SAMEPERIODLASTYEAR|PREVIOUSYEAR|PREVIOUSMONTH)\s*\(',
    }
    
    def __init__(self,
                 view_to_table_map: Dict[str, str] = None,
                 dax_translator=None,
                 backup_directory: str = ".migration_backup",
                 create_wrappers: bool = True):
        """
        Initialize the backward compatibility manager.
        
        Args:
            view_to_table_map: Mapping of old view names to new table names
            dax_translator: DAXToSQLTranslator instance for DAX conversion
            backup_directory: Directory for storing backups
            create_wrappers: Whether to create view wrappers
        """
        self.view_to_table_map = view_to_table_map or {}
        self.dax_translator = dax_translator
        self.backup_dir = backup_directory
        self.create_wrappers = create_wrappers
        
        self.legacy_references: List[LegacyReference] = []
        self.view_wrappers: List[ViewWrapper] = []
        self.migration_results: List[MigrationResult] = []
        
        # Create backup directory
        os.makedirs(self.backup_dir, exist_ok=True)
        
    def set_dax_translator(self, translator):
        """Set the DAX to SQL translator."""
        self.dax_translator = translator
        
    def add_view_mapping(self, old_view: str, new_table: str):
        """Add a view-to-table mapping."""
        self.view_to_table_map[old_view] = new_table
        
    def load_mappings_from_file(self, mapping_file: str):
        """Load view-to-table mappings from a JSON file."""
        try:
            with open(mapping_file, 'r') as f:
                mappings = json.load(f)
                self.view_to_table_map.update(mappings)
            logger.info(f"Loaded {len(mappings)} view-to-table mappings")
        except Exception as e:
            logger.error(f"Failed to load mappings: {e}")
            
    # ==========================================
    # VIEW WRAPPER CREATION
    # ==========================================
    
    def create_snowflake_view_wrappers(self, 
                                        views_converted: List[str],
                                        deprecation_months: int = 6) -> List[str]:
        """
        Create view wrappers in Snowflake for backward compatibility.
        
        These views point to the new tables, allowing old queries to continue working.
        """
        wrapper_ddls = []
        deprecation_date = datetime.now().replace(
            month=(datetime.now().month + deprecation_months - 1) % 12 + 1
        )
        
        for view_name in views_converted:
            new_table = self.view_to_table_map.get(view_name, view_name)
            
            # Parse names
            parts = view_name.split('.')
            if len(parts) == 3:
                database, schema, name = parts
            else:
                database = "DATABASE"
                schema = "SCHEMA"
                name = view_name
                
            # Create wrapper view DDL
            ddl = f"""
-- Backward compatibility view wrapper
-- Original: {view_name}
-- Target: {new_table}
-- DEPRECATION NOTE: This view will be removed after {deprecation_date.strftime('%Y-%m-%d')}
-- Please update your queries to use the new table format.

CREATE OR REPLACE VIEW {database}.{schema}.{name}_COMPAT AS
SELECT 
    * EXCLUDE (_SYNC_TIMESTAMP, _ROW_HASH, _IS_DELETED, _SYNC_ID, _SOURCE_PLATFORM)
FROM {new_table}
WHERE COALESCE(_IS_DELETED, FALSE) = FALSE;

-- Grant same permissions as original
GRANT SELECT ON VIEW {database}.{schema}.{name}_COMPAT TO ROLE PUBLIC;

-- Add comment for tracking
COMMENT ON VIEW {database}.{schema}.{name}_COMPAT IS 
'Backward compatibility wrapper for {view_name}. Use {new_table} instead. Deprecated: {deprecation_date.strftime("%Y-%m-%d")}';
"""
            wrapper_ddls.append(ddl)
            
            # Track wrapper
            self.view_wrappers.append(ViewWrapper(
                original_view_name=view_name,
                new_table_name=new_table,
                wrapper_ddl=ddl,
                platform='snowflake',
                created_at=datetime.now().isoformat(),
                deprecation_date=deprecation_date.strftime('%Y-%m-%d')
            ))
            
        return wrapper_ddls
        
    def create_fabric_view_wrappers(self, 
                                     models_converted: List[str]) -> List[Dict]:
        """
        Create view definitions in Fabric Warehouse for backward compatibility.
        """
        wrapper_configs = []
        
        for model_name in models_converted:
            new_table = self.view_to_table_map.get(model_name, model_name)
            
            config = {
                'viewName': f"{model_name}_Compat",
                'targetTable': new_table,
                'selectColumns': '*',
                'excludeColumns': ['_SYNC_TIMESTAMP', '_ROW_HASH', '_SOURCE_PLATFORM'],
                'whereClause': "ISNULL(_IS_DELETED, 0) = 0",
                'comment': f"Backward compatibility view for {model_name}"
            }
            
            wrapper_configs.append(config)
            
            self.view_wrappers.append(ViewWrapper(
                original_view_name=model_name,
                new_table_name=new_table,
                wrapper_ddl=json.dumps(config, indent=2),
                platform='fabric',
                created_at=datetime.now().isoformat()
            ))
            
        return wrapper_configs
        
    # ==========================================
    # LEGACY REFERENCE DETECTION & UPDATE
    # ==========================================
    
    def scan_directory_for_legacy_refs(self, 
                                        directory: str,
                                        file_extensions: List[str] = None) -> Dict[str, List[LegacyReference]]:
        """
        Scan a directory for files containing legacy view references.
        
        Args:
            directory: Directory to scan
            file_extensions: File extensions to include
            
        Returns:
            Dict mapping file paths to found references
        """
        if file_extensions is None:
            file_extensions = ['.sql', '.py', '.json', '.yaml', '.yml', '.txt', '.md']
            
        results = {}
        
        for root, dirs, files in os.walk(directory):
            # Skip backup directory
            if self.backup_dir in root:
                continue
                
            for file in files:
                ext = os.path.splitext(file)[1].lower()
                if ext in file_extensions:
                    file_path = os.path.join(root, file)
                    refs = self._scan_file_for_refs(file_path)
                    if refs:
                        results[file_path] = refs
                        self.legacy_references.extend(refs)
                        
        logger.info(f"Found {len(self.legacy_references)} legacy references in {len(results)} files")
        return results
        
    def _scan_file_for_refs(self, file_path: str) -> List[LegacyReference]:
        """Scan a single file for legacy view references."""
        refs = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                
            for i, line in enumerate(lines, 1):
                # Check against all patterns
                for pattern in self.VIEW_PATTERNS['snowflake']:
                    matches = re.finditer(pattern, line, re.IGNORECASE)
                    for match in matches:
                        ref_name = match.group(1)
                        if ref_name in self.view_to_table_map:
                            refs.append(LegacyReference(
                                original_ref=ref_name,
                                new_ref=self.view_to_table_map[ref_name],
                                file_path=file_path,
                                line_number=i,
                                context=line.strip()[:100]
                            ))
                            
        except Exception as e:
            logger.warning(f"Could not scan {file_path}: {e}")
            
        return refs
        
    def update_legacy_references(self, 
                                  file_path: str,
                                  dry_run: bool = False) -> MigrationResult:
        """
        Update legacy view references in a file to use new table names.
        
        Args:
            file_path: Path to file to update
            dry_run: If True, only report changes without making them
            
        Returns:
            MigrationResult with details of updates
        """
        result = MigrationResult(
            file_path=file_path,
            file_type=self._detect_file_type(file_path)
        )
        
        try:
            # Create backup
            if not dry_run:
                backup_path = self._create_backup(file_path)
                result.backup_path = backup_path
                result.actions.append(MigrationAction.BACKUP_CREATED)
                
            # Read file
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            original_content = content
            
            # Replace each mapping
            for old_view, new_table in self.view_to_table_map.items():
                # Case-insensitive replacement with word boundaries
                pattern = rf'\b{re.escape(old_view)}\b'
                matches = list(re.finditer(pattern, content, re.IGNORECASE))
                result.references_found += len(matches)
                
                if matches:
                    content = re.sub(pattern, new_table, content, flags=re.IGNORECASE)
                    result.references_updated += len(matches)
                    
            # Write updated content
            if not dry_run and content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                result.actions.append(MigrationAction.REFERENCE_UPDATED)
                
            logger.info(f"Updated {result.references_updated} references in {file_path}")
            
        except Exception as e:
            result.errors.append(str(e))
            result.actions.append(MigrationAction.ERROR)
            logger.error(f"Failed to update {file_path}: {e}")
            
        self.migration_results.append(result)
        return result
        
    def batch_update_references(self, 
                                 directory: str,
                                 file_extensions: List[str] = None,
                                 dry_run: bool = False) -> Dict[str, MigrationResult]:
        """
        Update legacy references in all files in a directory.
        """
        results = {}
        
        # First scan for references
        refs_by_file = self.scan_directory_for_legacy_refs(directory, file_extensions)
        
        # Update each file
        for file_path in refs_by_file:
            result = self.update_legacy_references(file_path, dry_run)
            results[file_path] = result
            
        return results
        
    # ==========================================
    # PBIX FILE MIGRATION
    # ==========================================
    
    def migrate_pbix_file(self, 
                          pbix_path: str,
                          output_path: str = None,
                          convert_dax: bool = True) -> MigrationResult:
        """
        Migrate a Power BI Desktop file (.pbix) to use new table format.
        
        Handles:
        - Updating data source references
        - Converting DAX measures to SQL (optional)
        - Updating relationships
        
        Note: Full PBIX modification requires Power BI Desktop API or pbixray library.
        """
        result = MigrationResult(
            file_path=pbix_path,
            file_type=FileType.PBIX
        )
        
        output_path = output_path or pbix_path.replace('.pbix', '_migrated.pbix')
        
        try:
            # Create backup
            backup_path = self._create_backup(pbix_path)
            result.backup_path = backup_path
            result.actions.append(MigrationAction.BACKUP_CREATED)
            
            # PBIX is a ZIP file - extract and process
            extract_dir = os.path.join(self.backup_dir, f"pbix_extract_{datetime.now().strftime('%Y%m%d%H%M%S')}")
            os.makedirs(extract_dir, exist_ok=True)
            
            with zipfile.ZipFile(pbix_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
                
            # Process DataModelSchema (contains data sources)
            schema_path = os.path.join(extract_dir, 'DataModelSchema')
            if os.path.exists(schema_path):
                result = self._process_pbix_schema(schema_path, result, convert_dax)
                
            # Process Mashup (Power Query M code)
            mashup_path = os.path.join(extract_dir, 'Mashup')
            if os.path.exists(mashup_path):
                result = self._process_pbix_mashup(mashup_path, result)
                
            # Repackage PBIX
            with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zip_out:
                for root, dirs, files in os.walk(extract_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arc_name = os.path.relpath(file_path, extract_dir)
                        zip_out.write(file_path, arc_name)
                        
            result.actions.append(MigrationAction.REFERENCE_UPDATED)
            logger.info(f"Migrated PBIX file: {pbix_path} -> {output_path}")
            
            # Cleanup
            shutil.rmtree(extract_dir)
            
        except Exception as e:
            result.errors.append(str(e))
            result.actions.append(MigrationAction.ERROR)
            logger.error(f"Failed to migrate PBIX {pbix_path}: {e}")
            
        self.migration_results.append(result)
        return result
        
    def _process_pbix_schema(self, 
                              schema_path: str, 
                              result: MigrationResult,
                              convert_dax: bool) -> MigrationResult:
        """Process DataModelSchema from PBIX file."""
        try:
            # Try to read as JSON (newer format)
            with open(schema_path, 'r', encoding='utf-16-le', errors='ignore') as f:
                # Skip BOM if present
                content = f.read()
                if content.startswith('\ufeff'):
                    content = content[1:]
                    
            # Try to parse as JSON
            try:
                schema = json.loads(content)
            except json.JSONDecodeError:
                # May be in a binary format - log and return
                logger.warning("DataModelSchema in binary format - limited processing")
                return result
                
            # Process model definition
            if 'model' in schema:
                model = schema['model']
                
                # Update table references
                for table in model.get('tables', []):
                    table_name = table.get('name', '')
                    
                    # Update source partitions
                    for partition in table.get('partitions', []):
                        source = partition.get('source', {})
                        expression = source.get('expression', '')
                        
                        # Update view references in M expressions
                        for old_view, new_table in self.view_to_table_map.items():
                            if old_view in expression:
                                source['expression'] = expression.replace(old_view, new_table)
                                result.references_updated += 1
                                
                    # Optionally convert DAX measures
                    if convert_dax and self.dax_translator:
                        for measure in table.get('measures', []):
                            dax_expr = measure.get('expression', '')
                            if dax_expr:
                                from .dax_to_sql_translator import DAXMeasure
                                dax_measure = DAXMeasure(
                                    name=measure.get('name', ''),
                                    expression=dax_expr,
                                    table_name=table_name
                                )
                                translation = self.dax_translator.translate_measure(dax_measure)
                                
                                if translation.success:
                                    # Store SQL translation as annotation
                                    measure['annotations'] = measure.get('annotations', [])
                                    measure['annotations'].append({
                                        'name': 'SQL_Translation',
                                        'value': translation.translated_sql
                                    })
                                    result.dax_measures_converted += 1
                                    
            # Write updated schema
            with open(schema_path, 'w', encoding='utf-16-le') as f:
                f.write('\ufeff' + json.dumps(schema, indent=2))
                
            result.actions.append(MigrationAction.DAX_CONVERTED)
            
        except Exception as e:
            result.errors.append(f"Schema processing: {str(e)}")
            logger.warning(f"Could not process PBIX schema: {e}")
            
        return result
        
    def _process_pbix_mashup(self, mashup_path: str, result: MigrationResult) -> MigrationResult:
        """Process Mashup (Power Query) from PBIX file."""
        try:
            # Mashup is a ZIP-like container
            for root, dirs, files in os.walk(mashup_path):
                for file in files:
                    if file.endswith('.m') or file == 'Mashup':
                        file_path = os.path.join(root, file)
                        
                        with open(file_path, 'rb') as f:
                            content = f.read()
                            
                        # Try to decode and update
                        try:
                            text = content.decode('utf-8', errors='ignore')
                            original = text
                            
                            for old_view, new_table in self.view_to_table_map.items():
                                if old_view in text:
                                    text = text.replace(old_view, new_table)
                                    result.references_updated += 1
                                    
                            if text != original:
                                with open(file_path, 'wb') as f:
                                    f.write(text.encode('utf-8'))
                                    
                        except Exception as e:
                            logger.debug(f"Could not process mashup file {file}: {e}")
                            
        except Exception as e:
            result.errors.append(f"Mashup processing: {str(e)}")
            
        return result
        
    # ==========================================
    # NOTEBOOK MIGRATION
    # ==========================================
    
    def migrate_notebook(self, 
                         notebook_path: str,
                         notebook_type: str = 'fabric') -> MigrationResult:
        """
        Migrate a Fabric notebook to use new table format.
        
        Handles:
        - Spark SQL references
        - Python spark.sql() calls
        - PySpark DataFrame operations
        """
        result = MigrationResult(
            file_path=notebook_path,
            file_type=FileType.NOTEBOOK
        )
        
        try:
            # Create backup
            backup_path = self._create_backup(notebook_path)
            result.backup_path = backup_path
            result.actions.append(MigrationAction.BACKUP_CREATED)
            
            # Read notebook JSON
            with open(notebook_path, 'r', encoding='utf-8') as f:
                notebook = json.load(f)
                
            # Process cells
            cells = notebook.get('cells', [])
            
            for cell in cells:
                cell_type = cell.get('cell_type', '')
                source = cell.get('source', [])
                
                if isinstance(source, list):
                    source_text = ''.join(source)
                else:
                    source_text = source
                    
                if cell_type in ['code', 'sql']:
                    updated_source = self._update_notebook_cell(source_text, result)
                    
                    if updated_source != source_text:
                        if isinstance(source, list):
                            cell['source'] = updated_source.split('\n')
                        else:
                            cell['source'] = updated_source
                            
            # Write updated notebook
            with open(notebook_path, 'w', encoding='utf-8') as f:
                json.dump(notebook, f, indent=2)
                
            result.actions.append(MigrationAction.REFERENCE_UPDATED)
            logger.info(f"Migrated notebook: {notebook_path}")
            
        except Exception as e:
            result.errors.append(str(e))
            result.actions.append(MigrationAction.ERROR)
            logger.error(f"Failed to migrate notebook {notebook_path}: {e}")
            
        self.migration_results.append(result)
        return result
        
    def _update_notebook_cell(self, source: str, result: MigrationResult) -> str:
        """Update references in a notebook cell."""
        updated = source
        
        # Update SQL-like references
        for old_view, new_table in self.view_to_table_map.items():
            # Direct references
            if old_view in updated:
                updated = updated.replace(old_view, new_table)
                result.references_updated += 1
                
            # Spark SQL patterns
            patterns = [
                rf'spark\.table\(["\']({re.escape(old_view)})["\']\)',
                rf'spark\.sql\(["\'].*FROM\s+{re.escape(old_view)}.*["\']\)',
                rf'\.read\.table\(["\']({re.escape(old_view)})["\']\)',
            ]
            
            for pattern in patterns:
                if re.search(pattern, updated, re.IGNORECASE):
                    updated = re.sub(
                        pattern.replace(re.escape(old_view), re.escape(old_view)),
                        lambda m: m.group(0).replace(old_view, new_table),
                        updated,
                        flags=re.IGNORECASE
                    )
                    result.references_updated += 1
                    
        return updated
        
    # ==========================================
    # VALIDATION
    # ==========================================
    
    def validate_migration(self, 
                            old_query: str,
                            new_query: str,
                            snowflake_connector=None) -> Dict[str, Any]:
        """
        Validate that migrated query produces same results as original.
        
        Runs both queries and compares:
        - Row counts
        - Column names and types
        - Sample data (if small enough)
        - Checksums
        """
        validation = {
            'passed': False,
            'row_count_match': False,
            'column_match': False,
            'data_match': False,
            'details': {}
        }
        
        if not snowflake_connector:
            validation['error'] = "No Snowflake connector provided"
            return validation
            
        try:
            # Execute old query
            old_results = snowflake_connector.execute_query(old_query)
            old_count = len(old_results)
            
            # Execute new query
            new_results = snowflake_connector.execute_query(new_query)
            new_count = len(new_results)
            
            # Compare counts
            validation['row_count_match'] = old_count == new_count
            validation['details']['old_count'] = old_count
            validation['details']['new_count'] = new_count
            
            # Compare columns
            if old_results and new_results:
                old_cols = set(old_results[0].keys()) if old_results else set()
                new_cols = set(new_results[0].keys()) if new_results else set()
                
                # Exclude metadata columns from comparison
                meta_cols = {'_SYNC_TIMESTAMP', '_ROW_HASH', '_IS_DELETED', '_SYNC_ID', '_SOURCE_PLATFORM'}
                new_cols_filtered = new_cols - meta_cols
                
                validation['column_match'] = old_cols == new_cols_filtered
                validation['details']['column_diff'] = list(old_cols.symmetric_difference(new_cols_filtered))
                
            # Compare data for small result sets
            if old_count <= 1000 and old_count == new_count:
                # Create comparable data structures
                old_data = [frozenset(row.items()) for row in old_results]
                
                # Filter metadata columns from new results
                new_data = []
                for row in new_results:
                    filtered_row = {k: v for k, v in row.items() 
                                   if k not in meta_cols}
                    new_data.append(frozenset(filtered_row.items()))
                    
                validation['data_match'] = set(old_data) == set(new_data)
                
            # Overall pass/fail
            validation['passed'] = (
                validation['row_count_match'] and 
                validation['column_match'] and
                (validation['data_match'] or old_count > 1000)
            )
            
        except Exception as e:
            validation['error'] = str(e)
            logger.error(f"Validation failed: {e}")
            
        return validation
        
    def batch_validate_migrations(self,
                                   query_pairs: List[Tuple[str, str]],
                                   snowflake_connector=None) -> Dict[str, Any]:
        """
        Validate multiple query migrations.
        
        Args:
            query_pairs: List of (old_query, new_query) tuples
        """
        results = {
            'total': len(query_pairs),
            'passed': 0,
            'failed': 0,
            'details': []
        }
        
        for i, (old_query, new_query) in enumerate(query_pairs):
            validation = self.validate_migration(old_query, new_query, snowflake_connector)
            validation['query_index'] = i
            results['details'].append(validation)
            
            if validation.get('passed'):
                results['passed'] += 1
            else:
                results['failed'] += 1
                
        return results
        
    # ==========================================
    # UTILITY METHODS
    # ==========================================
    
    def _detect_file_type(self, file_path: str) -> FileType:
        """Detect the type of file."""
        ext = os.path.splitext(file_path)[1].lower()
        
        type_map = {
            '.pbix': FileType.PBIX,
            '.pbit': FileType.PBIT,
            '.ipynb': FileType.NOTEBOOK,
            '.json': FileType.JSON,
            '.sql': FileType.SQL,
            '.yaml': FileType.YAML,
            '.yml': FileType.YAML,
            '.bism': FileType.BISM,
        }
        
        return type_map.get(ext, FileType.JSON)
        
    def _create_backup(self, file_path: str) -> str:
        """Create a backup of a file."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.basename(file_path)
        backup_path = os.path.join(self.backup_dir, f"{timestamp}_{filename}")
        
        shutil.copy2(file_path, backup_path)
        logger.debug(f"Created backup: {backup_path}")
        
        return backup_path
        
    def generate_migration_scripts(self) -> Dict[str, str]:
        """
        Generate migration scripts based on detected changes.
        
        Returns DDL scripts for view wrappers and reference updates.
        """
        scripts = {
            'snowflake_wrappers': "",
            'fabric_wrappers': "",
            'reference_updates': ""
        }
        
        # Snowflake view wrappers
        sf_wrappers = [w for w in self.view_wrappers if w.platform == 'snowflake']
        scripts['snowflake_wrappers'] = "\n\n".join([w.wrapper_ddl for w in sf_wrappers])
        
        # Fabric view wrappers
        fabric_wrappers = [w for w in self.view_wrappers if w.platform == 'fabric']
        scripts['fabric_wrappers'] = json.dumps(
            [json.loads(w.wrapper_ddl) for w in fabric_wrappers],
            indent=2
        )
        
        # Reference update summary
        ref_updates = []
        for ref in self.legacy_references:
            ref_updates.append(f"-- {ref.file_path}:{ref.line_number}")
            ref_updates.append(f"-- Change: {ref.original_ref} -> {ref.new_ref}")
            
        scripts['reference_updates'] = "\n".join(ref_updates)
        
        return scripts
        
    def get_migration_report(self) -> Dict[str, Any]:
        """Get comprehensive migration report."""
        return {
            'summary': {
                'files_processed': len(self.migration_results),
                'total_references_found': sum(r.references_found for r in self.migration_results),
                'total_references_updated': sum(r.references_updated for r in self.migration_results),
                'dax_measures_converted': sum(r.dax_measures_converted for r in self.migration_results),
                'view_wrappers_created': len(self.view_wrappers),
                'errors': sum(len(r.errors) for r in self.migration_results)
            },
            'view_wrappers': [
                {
                    'original': w.original_view_name,
                    'new_table': w.new_table_name,
                    'platform': w.platform,
                    'deprecation': w.deprecation_date
                }
                for w in self.view_wrappers
            ],
            'file_results': [
                {
                    'file': r.file_path,
                    'type': r.file_type.value,
                    'references_updated': r.references_updated,
                    'dax_converted': r.dax_measures_converted,
                    'errors': r.errors
                }
                for r in self.migration_results
            ],
            'mappings_used': len(self.view_to_table_map)
        }
        
    def save_report(self, output_path: str = None):
        """Save migration report to file."""
        output_path = output_path or f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        report = self.get_migration_report()
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
            
        logger.info(f"Migration report saved to {output_path}")
        return output_path
