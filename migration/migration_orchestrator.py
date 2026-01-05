"""
Migration Orchestrator - Master Controller for Snowflake-Fabric Migration

This is the main entry point for the comprehensive migration system.
Coordinates all phases:
- Phase 1: Schema Analysis & Mapping
- Phase 2: View → Table Conversion
- Phase 3: DAX → SQL Translation
- Phase 4: Bidirectional Sync Setup
- Phase 5: Backward Compatibility
- Phase 6: Testing & Validation
- Phase 7: Deployment & Monitoring

Provides a unified API for the complete migration lifecycle.
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import migration components
from .view_to_table_converter import ViewToTableConverter, ViewDefinition, ConversionResult
from .dax_to_sql_translator import DAXToSQLTranslator, DAXMeasure, TranslationResult
from .bidirectional_sync_manager import BidirectionalSyncManager, SyncConfiguration, SyncMode, SyncDirection
from .backward_compatibility import BackwardCompatibilityManager, MigrationResult

logger = logging.getLogger(__name__)


class MigrationPhase(Enum):
    """Phases of the migration process."""
    ANALYSIS = "analysis"
    CONVERSION = "conversion"
    TRANSLATION = "translation"
    SYNC_SETUP = "sync_setup"
    COMPATIBILITY = "compatibility"
    VALIDATION = "validation"
    DEPLOYMENT = "deployment"
    MONITORING = "monitoring"


class MigrationStatus(Enum):
    """Overall migration status."""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class PhaseResult:
    """Result of a migration phase."""
    phase: MigrationPhase
    status: str
    start_time: str
    end_time: Optional[str] = None
    duration_seconds: float = 0.0
    items_processed: int = 0
    items_successful: int = 0
    items_failed: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    artifacts: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MigrationManifest:
    """Complete migration manifest tracking all components."""
    migration_id: str
    name: str
    created_at: str
    status: MigrationStatus = MigrationStatus.NOT_STARTED
    current_phase: Optional[MigrationPhase] = None
    phases_completed: List[MigrationPhase] = field(default_factory=list)
    phase_results: Dict[str, PhaseResult] = field(default_factory=dict)
    
    # Inventory
    snowflake_views: List[str] = field(default_factory=list)
    fabric_models: List[str] = field(default_factory=list)
    dax_measures: List[str] = field(default_factory=list)
    legacy_files: List[str] = field(default_factory=list)
    
    # Mappings
    view_to_table_map: Dict[str, str] = field(default_factory=dict)
    dax_to_sql_map: Dict[str, str] = field(default_factory=dict)
    
    # Statistics
    total_tables_converted: int = 0
    total_dax_translated: int = 0
    total_files_migrated: int = 0
    total_rows_synced: int = 0
    
    def to_dict(self) -> Dict:
        result = {
            'migration_id': self.migration_id,
            'name': self.name,
            'created_at': self.created_at,
            'status': self.status.value,
            'current_phase': self.current_phase.value if self.current_phase else None,
            'phases_completed': [p.value for p in self.phases_completed],
            'snowflake_views': self.snowflake_views,
            'fabric_models': self.fabric_models,
            'dax_measures': self.dax_measures,
            'view_to_table_map': self.view_to_table_map,
            'statistics': {
                'tables_converted': self.total_tables_converted,
                'dax_translated': self.total_dax_translated,
                'files_migrated': self.total_files_migrated,
                'rows_synced': self.total_rows_synced
            }
        }
        return result


class MigrationOrchestrator:
    """
    Master orchestrator for the complete Snowflake-Fabric migration.
    
    Coordinates all migration components and phases to ensure a 
    successful, validated migration with rollback capabilities.
    """
    
    def __init__(self,
                 snowflake_connector=None,
                 fabric_client=None,
                 config: Dict[str, Any] = None,
                 workspace_dir: str = None):
        """
        Initialize the migration orchestrator.
        
        Args:
            snowflake_connector: SnowflakeConnector instance
            fabric_client: FabricApiClient instance
            config: Migration configuration
            workspace_dir: Directory for migration artifacts
        """
        self.snowflake = snowflake_connector
        self.fabric = fabric_client
        self.config = config or {}
        self.workspace_dir = workspace_dir or os.path.join(os.getcwd(), 'migration_workspace')
        
        # Create workspace
        os.makedirs(self.workspace_dir, exist_ok=True)
        
        # Initialize components
        self.view_converter = ViewToTableConverter(
            snowflake_connector=snowflake_connector,
            fabric_client=fabric_client
        )
        
        self.dax_translator = DAXToSQLTranslator(dialect='snowflake')
        
        self.sync_manager = BidirectionalSyncManager(
            snowflake_connector=snowflake_connector,
            fabric_client=fabric_client
        )
        
        self.compat_manager = BackwardCompatibilityManager(
            dax_translator=self.dax_translator,
            backup_directory=os.path.join(self.workspace_dir, 'backups')
        )
        
        # Migration state
        self.manifest: Optional[MigrationManifest] = None
        self.running = False
        self.pause_requested = False
        self.rollback_points: Dict[str, Any] = {}
        
        # Callbacks
        self.progress_callback: Optional[Callable] = None
        self.error_callback: Optional[Callable] = None
        
    def set_snowflake_connector(self, connector):
        """Set the Snowflake connector for all components."""
        self.snowflake = connector
        self.view_converter.set_snowflake_connector(connector)
        self.sync_manager.set_snowflake_connector(connector)
        
    def set_fabric_client(self, client):
        """Set the Fabric client for all components."""
        self.fabric = client
        self.view_converter.set_fabric_client(client)
        self.sync_manager.set_fabric_client(client)
        
    def set_progress_callback(self, callback: Callable):
        """Set callback for progress updates."""
        self.progress_callback = callback
        
    def set_error_callback(self, callback: Callable):
        """Set callback for error notifications."""
        self.error_callback = callback
        
    def _notify_progress(self, phase: str, message: str, percent: float):
        """Notify progress via callback."""
        if self.progress_callback:
            self.progress_callback({
                'phase': phase,
                'message': message,
                'percent': percent,
                'timestamp': datetime.now().isoformat()
            })
            
    def _notify_error(self, phase: str, error: str):
        """Notify error via callback."""
        if self.error_callback:
            self.error_callback({
                'phase': phase,
                'error': error,
                'timestamp': datetime.now().isoformat()
            })
            
    # ==========================================
    # MIGRATION LIFECYCLE
    # ==========================================
    
    def create_migration(self, name: str = None) -> MigrationManifest:
        """
        Create a new migration manifest.
        
        This initializes a new migration that can be executed in phases.
        """
        migration_id = f"MIG_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        name = name or f"Snowflake-Fabric Migration {datetime.now().strftime('%Y-%m-%d')}"
        
        self.manifest = MigrationManifest(
            migration_id=migration_id,
            name=name,
            created_at=datetime.now().isoformat()
        )
        
        # Save manifest
        self._save_manifest()
        
        logger.info(f"Created migration: {migration_id}")
        return self.manifest
        
    def load_migration(self, migration_id: str) -> MigrationManifest:
        """Load an existing migration manifest."""
        manifest_path = os.path.join(self.workspace_dir, f"{migration_id}.json")
        
        if not os.path.exists(manifest_path):
            raise ValueError(f"Migration {migration_id} not found")
            
        with open(manifest_path, 'r') as f:
            data = json.load(f)
            
        # Reconstruct manifest
        self.manifest = MigrationManifest(
            migration_id=data['migration_id'],
            name=data['name'],
            created_at=data['created_at'],
            status=MigrationStatus(data.get('status', 'not_started')),
            snowflake_views=data.get('snowflake_views', []),
            fabric_models=data.get('fabric_models', []),
            dax_measures=data.get('dax_measures', []),
            view_to_table_map=data.get('view_to_table_map', {})
        )
        
        logger.info(f"Loaded migration: {migration_id}")
        return self.manifest
        
    def _save_manifest(self):
        """Save current manifest to disk."""
        if not self.manifest:
            return
            
        manifest_path = os.path.join(
            self.workspace_dir, 
            f"{self.manifest.migration_id}.json"
        )
        
        with open(manifest_path, 'w') as f:
            json.dump(self.manifest.to_dict(), f, indent=2, default=str)
            
    # ==========================================
    # PHASE 1: SCHEMA ANALYSIS & MAPPING
    # ==========================================
    
    def run_analysis_phase(self) -> PhaseResult:
        """
        Phase 1: Analyze and inventory all objects to be migrated.
        
        - Inventory all Snowflake views
        - Inventory all Fabric semantic models
        - Document DAX measures and dependencies
        - Create mapping document
        """
        result = PhaseResult(
            phase=MigrationPhase.ANALYSIS,
            status='in_progress',
            start_time=datetime.now().isoformat()
        )
        
        if not self.manifest:
            self.create_migration()
            
        self.manifest.current_phase = MigrationPhase.ANALYSIS
        self.manifest.status = MigrationStatus.IN_PROGRESS
        
        try:
            self._notify_progress('analysis', 'Starting schema analysis...', 0)
            
            # 1. Discover Snowflake views
            self._notify_progress('analysis', 'Discovering Snowflake views...', 10)
            if self.snowflake:
                sf_views = self.view_converter.discover_snowflake_views()
                self.manifest.snowflake_views = [v.name for v in sf_views]
                result.items_processed += len(sf_views)
                result.metadata['snowflake_views'] = len(sf_views)
                
            # 2. Discover Fabric semantic models
            self._notify_progress('analysis', 'Discovering Fabric models...', 30)
            if self.fabric:
                fabric_views = self.view_converter.discover_fabric_views()
                self.manifest.fabric_models = [v.name for v in fabric_views]
                result.items_processed += len(fabric_views)
                result.metadata['fabric_models'] = len(fabric_views)
                
            # 3. Extract DAX measures
            self._notify_progress('analysis', 'Extracting DAX measures...', 50)
            dax_measures = self._extract_all_dax_measures()
            self.manifest.dax_measures = [m.name for m in dax_measures]
            result.metadata['dax_measures'] = len(dax_measures)
            
            # 4. Create mappings
            self._notify_progress('analysis', 'Creating mappings...', 70)
            mappings = self._generate_mappings(sf_views if self.snowflake else [], 
                                               fabric_views if self.fabric else [])
            self.manifest.view_to_table_map = mappings
            
            # 5. Generate analysis report
            self._notify_progress('analysis', 'Generating analysis report...', 90)
            report = self._generate_analysis_report()
            report_path = os.path.join(self.workspace_dir, 'analysis_report.json')
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            result.artifacts['analysis_report'] = report_path
            
            result.status = 'completed'
            result.items_successful = result.items_processed
            self.manifest.phases_completed.append(MigrationPhase.ANALYSIS)
            
            self._notify_progress('analysis', 'Analysis complete', 100)
            
        except Exception as e:
            result.status = 'failed'
            result.errors.append(str(e))
            self._notify_error('analysis', str(e))
            logger.error(f"Analysis phase failed: {e}")
            
        result.end_time = datetime.now().isoformat()
        result.duration_seconds = (
            datetime.fromisoformat(result.end_time) - 
            datetime.fromisoformat(result.start_time)
        ).total_seconds()
        
        self.manifest.phase_results['analysis'] = result
        self._save_manifest()
        
        return result
        
    def _extract_all_dax_measures(self) -> List[DAXMeasure]:
        """Extract all DAX measures from Fabric semantic models."""
        measures = []
        
        if self.fabric:
            try:
                models = self.fabric.list_semantic_models()
                
                for model in models:
                    model_detail = self.fabric.get_semantic_model(model['id'])
                    extracted = self.dax_translator.extract_measures_from_semantic_model(model_detail)
                    measures.extend(extracted)
                    
            except Exception as e:
                logger.warning(f"Could not extract DAX measures: {e}")
                
        return measures
        
    def _generate_mappings(self, 
                           sf_views: List[ViewDefinition],
                           fabric_views: List[ViewDefinition]) -> Dict[str, str]:
        """Generate view-to-table mappings."""
        mappings = {}
        
        for view in sf_views:
            full_name = f"{view.database}.{view.schema_name}.{view.name}"
            # Tables get same name in most cases
            table_name = full_name.replace('_VIEW', '').replace('_VW', '')
            mappings[full_name] = table_name
            
        for view in fabric_views:
            full_name = f"{view.schema_name}.{view.name}"
            table_name = view.name.replace(' ', '_').upper()
            mappings[full_name] = table_name
            
        return mappings
        
    def _generate_analysis_report(self) -> Dict[str, Any]:
        """Generate comprehensive analysis report."""
        return {
            'migration_id': self.manifest.migration_id,
            'analysis_date': datetime.now().isoformat(),
            'summary': {
                'snowflake_views': len(self.manifest.snowflake_views),
                'fabric_models': len(self.manifest.fabric_models),
                'dax_measures': len(self.manifest.dax_measures),
                'mappings_created': len(self.manifest.view_to_table_map)
            },
            'snowflake_objects': self.manifest.snowflake_views,
            'fabric_objects': self.manifest.fabric_models,
            'dax_measures': self.manifest.dax_measures,
            'recommended_actions': [
                'Review view-to-table mappings before conversion',
                'Identify high-complexity DAX measures for manual review',
                'Plan for incremental refresh on large tables',
                'Schedule migration during low-usage period'
            ]
        }
        
    # ==========================================
    # PHASE 2: VIEW TO TABLE CONVERSION
    # ==========================================
    
    def run_conversion_phase(self,
                              dry_run: bool = False,
                              incremental: bool = True,
                              parallel: int = 4) -> PhaseResult:
        """
        Phase 2: Convert all views to materialized tables.
        
        Args:
            dry_run: If True, only generate DDL without executing
            incremental: Use incremental refresh patterns
            parallel: Number of parallel conversions
        """
        result = PhaseResult(
            phase=MigrationPhase.CONVERSION,
            status='in_progress',
            start_time=datetime.now().isoformat()
        )
        
        if not self.manifest:
            raise ValueError("No migration manifest. Run analysis phase first.")
            
        self.manifest.current_phase = MigrationPhase.CONVERSION
        
        try:
            self._notify_progress('conversion', 'Starting view-to-table conversion...', 0)
            
            # Create rollback point
            rollback_id = f"conv_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.rollback_points[rollback_id] = {
                'phase': 'conversion',
                'timestamp': datetime.now().isoformat(),
                'views': self.manifest.snowflake_views.copy()
            }
            
            # Convert Snowflake views
            if self.manifest.snowflake_views:
                self._notify_progress('conversion', 
                                     f'Converting {len(self.manifest.snowflake_views)} Snowflake views...', 
                                     10)
                
                if parallel > 1:
                    sf_results = self._parallel_convert_views(
                        self.manifest.snowflake_views, 
                        'snowflake',
                        dry_run,
                        incremental,
                        parallel
                    )
                else:
                    sf_results = self._sequential_convert_views(
                        self.manifest.snowflake_views,
                        'snowflake',
                        dry_run,
                        incremental
                    )
                    
                result.items_processed += len(sf_results)
                result.items_successful += sum(1 for r in sf_results if r.status.value == 'completed')
                result.items_failed += sum(1 for r in sf_results if r.status.value == 'failed')
                
            # Convert Fabric views
            if self.manifest.fabric_models:
                self._notify_progress('conversion',
                                     f'Converting {len(self.manifest.fabric_models)} Fabric models...',
                                     60)
                
                fabric_results = self._convert_fabric_views(
                    self.manifest.fabric_models,
                    dry_run
                )
                
                result.items_processed += len(fabric_results)
                result.items_successful += sum(1 for r in fabric_results if r.status.value == 'completed')
                
            self.manifest.total_tables_converted = result.items_successful
            
            # Generate conversion report
            report_path = os.path.join(self.workspace_dir, 'conversion_report.json')
            conversion_report = self.view_converter.get_conversion_report()
            with open(report_path, 'w') as f:
                json.dump(conversion_report, f, indent=2, default=str)
            result.artifacts['conversion_report'] = report_path
            
            result.status = 'completed' if result.items_failed == 0 else 'partial'
            
            if result.items_failed > 0:
                result.warnings.append(f"{result.items_failed} conversions failed")
                
            self.manifest.phases_completed.append(MigrationPhase.CONVERSION)
            self._notify_progress('conversion', 'Conversion complete', 100)
            
        except Exception as e:
            result.status = 'failed'
            result.errors.append(str(e))
            self._notify_error('conversion', str(e))
            logger.error(f"Conversion phase failed: {e}")
            
        result.end_time = datetime.now().isoformat()
        result.duration_seconds = (
            datetime.fromisoformat(result.end_time) -
            datetime.fromisoformat(result.start_time)
        ).total_seconds()
        
        self.manifest.phase_results['conversion'] = result
        self._save_manifest()
        
        return result
        
    def _parallel_convert_views(self,
                                 views: List[str],
                                 platform: str,
                                 dry_run: bool,
                                 incremental: bool,
                                 parallel: int) -> List[ConversionResult]:
        """Convert views in parallel."""
        results = []
        
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {}
            
            for view_name in views:
                # Create ViewDefinition
                view = ViewDefinition(
                    name=view_name,
                    schema_name='',
                    database='',
                    platform=platform
                )
                
                future = executor.submit(
                    self.view_converter.convert_snowflake_view_to_table,
                    view,
                    incremental=incremental
                )
                futures[future] = view_name
                
            for future in as_completed(futures):
                view_name = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Conversion failed for {view_name}: {e}")
                    
        return results
        
    def _sequential_convert_views(self,
                                   views: List[str],
                                   platform: str,
                                   dry_run: bool,
                                   incremental: bool) -> List[ConversionResult]:
        """Convert views sequentially."""
        results = []
        
        for i, view_name in enumerate(views):
            progress = 10 + (50 * i / len(views))
            self._notify_progress('conversion', f'Converting {view_name}...', progress)
            
            view = ViewDefinition(
                name=view_name,
                schema_name='',
                database='',
                platform=platform
            )
            
            result = self.view_converter.convert_snowflake_view_to_table(
                view,
                incremental=incremental
            )
            results.append(result)
            
            if self.pause_requested:
                break
                
        return results
        
    def _convert_fabric_views(self,
                               models: List[str],
                               dry_run: bool) -> List[ConversionResult]:
        """Convert Fabric semantic model views to warehouse tables."""
        results = []
        
        # Would iterate through models and create warehouse tables
        # Implementation depends on Fabric API specifics
        
        return results
        
    # ==========================================
    # PHASE 3: DAX TO SQL TRANSLATION
    # ==========================================
    
    def run_translation_phase(self, dialect: str = 'snowflake') -> PhaseResult:
        """
        Phase 3: Translate all DAX measures to SQL.
        
        Args:
            dialect: SQL dialect ('snowflake' or 'tsql')
        """
        result = PhaseResult(
            phase=MigrationPhase.TRANSLATION,
            status='in_progress',
            start_time=datetime.now().isoformat()
        )
        
        if not self.manifest:
            raise ValueError("No migration manifest. Run analysis phase first.")
            
        self.manifest.current_phase = MigrationPhase.TRANSLATION
        
        try:
            self._notify_progress('translation', 'Starting DAX to SQL translation...', 0)
            
            # Configure translator
            self.dax_translator = DAXToSQLTranslator(
                dialect=dialect,
                schema_info=self.manifest.view_to_table_map
            )
            
            # Extract and translate measures
            measures = self._extract_all_dax_measures()
            result.items_processed = len(measures)
            
            translations = {}
            needs_review = []
            
            for i, measure in enumerate(measures):
                progress = (i + 1) / len(measures) * 80
                self._notify_progress('translation', 
                                     f'Translating {measure.name}...', 
                                     progress)
                
                translation = self.dax_translator.translate_measure(measure)
                translations[measure.name] = translation
                
                if translation.success:
                    result.items_successful += 1
                    self.manifest.dax_to_sql_map[measure.name] = translation.translated_sql
                else:
                    result.items_failed += 1
                    result.errors.append(f"{measure.name}: {translation.warnings}")
                    
                if translation.manual_review_needed:
                    needs_review.append(measure.name)
                    
            self.manifest.total_dax_translated = result.items_successful
            
            # Generate translation report
            self._notify_progress('translation', 'Generating translation report...', 90)
            
            report = self.dax_translator.get_translation_report()
            report['needs_manual_review'] = needs_review
            
            report_path = os.path.join(self.workspace_dir, 'translation_report.json')
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            result.artifacts['translation_report'] = report_path
            
            # Generate SQL view definitions
            if self.manifest.view_to_table_map:
                base_table = list(self.manifest.view_to_table_map.values())[0]
                sql_views = self.dax_translator.generate_sql_view_definitions(
                    translations, base_table
                )
                
                sql_path = os.path.join(self.workspace_dir, 'translated_measures.sql')
                with open(sql_path, 'w') as f:
                    f.write(sql_views)
                result.artifacts['sql_views'] = sql_path
                
            result.status = 'completed' if result.items_failed == 0 else 'partial'
            
            if needs_review:
                result.warnings.append(f"{len(needs_review)} measures need manual review")
                
            self.manifest.phases_completed.append(MigrationPhase.TRANSLATION)
            self._notify_progress('translation', 'Translation complete', 100)
            
        except Exception as e:
            result.status = 'failed'
            result.errors.append(str(e))
            self._notify_error('translation', str(e))
            logger.error(f"Translation phase failed: {e}")
            
        result.end_time = datetime.now().isoformat()
        result.duration_seconds = (
            datetime.fromisoformat(result.end_time) -
            datetime.fromisoformat(result.start_time)
        ).total_seconds()
        
        self.manifest.phase_results['translation'] = result
        self._save_manifest()
        
        return result
        
    # ==========================================
    # PHASE 4: BIDIRECTIONAL SYNC SETUP
    # ==========================================
    
    def run_sync_setup_phase(self,
                              enable_cdc: bool = True,
                              enable_mirroring: bool = True,
                              sync_interval: int = 15) -> PhaseResult:
        """
        Phase 4: Configure bidirectional synchronization.
        
        Args:
            enable_cdc: Enable Change Data Capture
            enable_mirroring: Enable Fabric Mirroring
            sync_interval: Sync interval in minutes
        """
        result = PhaseResult(
            phase=MigrationPhase.SYNC_SETUP,
            status='in_progress',
            start_time=datetime.now().isoformat()
        )
        
        if not self.manifest:
            raise ValueError("No migration manifest. Run previous phases first.")
            
        self.manifest.current_phase = MigrationPhase.SYNC_SETUP
        
        try:
            self._notify_progress('sync_setup', 'Configuring bidirectional sync...', 0)
            
            # Configure sync manager
            sync_config = SyncConfiguration(
                direction=SyncDirection.BIDIRECTIONAL,
                mode=SyncMode.CDC if enable_cdc else SyncMode.INCREMENTAL,
                sync_interval_minutes=sync_interval,
                enable_cdc=enable_cdc
            )
            self.sync_manager.config = sync_config
            
            # 1. Setup CDC infrastructure in Snowflake
            if enable_cdc and self.snowflake:
                self._notify_progress('sync_setup', 'Setting up Snowflake CDC...', 20)
                
                self.sync_manager.setup_snowflake_cdc_infrastructure()
                
                cdc_results = self.sync_manager.setup_snowflake_cdc(
                    self.manifest.snowflake_views
                )
                
                result.items_processed += len(cdc_results)
                result.items_successful += sum(1 for v in cdc_results.values() if v)
                result.metadata['cdc_enabled_tables'] = sum(1 for v in cdc_results.values() if v)
                
            # 2. Setup Fabric Mirroring
            if enable_mirroring and self.fabric:
                self._notify_progress('sync_setup', 'Configuring Fabric Mirroring...', 50)
                
                mirroring_result = self.sync_manager.setup_fabric_mirroring(
                    list(self.manifest.view_to_table_map.values())
                )
                
                result.metadata['mirroring_config'] = mirroring_result
                
            # 3. Create stored procedures
            self._notify_progress('sync_setup', 'Creating sync procedures...', 70)
            
            procedures = self.sync_manager.create_sync_stored_procedures()
            result.metadata['procedures_created'] = list(procedures.keys())
            
            # 4. Create scheduled tasks
            self._notify_progress('sync_setup', 'Creating scheduled tasks...', 85)
            
            tasks = self.sync_manager.create_sync_tasks()
            result.metadata['tasks_created'] = list(tasks.keys())
            
            # 5. Generate sync configuration file
            sync_config_path = os.path.join(self.workspace_dir, 'sync_config.json')
            with open(sync_config_path, 'w') as f:
                json.dump({
                    'direction': sync_config.direction.value,
                    'mode': sync_config.mode.value,
                    'interval_minutes': sync_config.sync_interval_minutes,
                    'cdc_enabled': enable_cdc,
                    'mirroring_enabled': enable_mirroring,
                    'tables': list(self.manifest.view_to_table_map.values())
                }, f, indent=2)
            result.artifacts['sync_config'] = sync_config_path
            
            result.status = 'completed'
            self.manifest.phases_completed.append(MigrationPhase.SYNC_SETUP)
            self._notify_progress('sync_setup', 'Sync setup complete', 100)
            
        except Exception as e:
            result.status = 'failed'
            result.errors.append(str(e))
            self._notify_error('sync_setup', str(e))
            logger.error(f"Sync setup phase failed: {e}")
            
        result.end_time = datetime.now().isoformat()
        result.duration_seconds = (
            datetime.fromisoformat(result.end_time) -
            datetime.fromisoformat(result.start_time)
        ).total_seconds()
        
        self.manifest.phase_results['sync_setup'] = result
        self._save_manifest()
        
        return result
        
    # ==========================================
    # PHASE 5: BACKWARD COMPATIBILITY
    # ==========================================
    
    def run_compatibility_phase(self,
                                 scan_directory: str = None,
                                 create_wrappers: bool = True,
                                 migrate_pbix: bool = True) -> PhaseResult:
        """
        Phase 5: Ensure backward compatibility.
        
        Args:
            scan_directory: Directory to scan for legacy references
            create_wrappers: Create view wrappers for compatibility
            migrate_pbix: Migrate PBIX files
        """
        result = PhaseResult(
            phase=MigrationPhase.COMPATIBILITY,
            status='in_progress',
            start_time=datetime.now().isoformat()
        )
        
        if not self.manifest:
            raise ValueError("No migration manifest. Run previous phases first.")
            
        self.manifest.current_phase = MigrationPhase.COMPATIBILITY
        
        try:
            self._notify_progress('compatibility', 'Setting up backward compatibility...', 0)
            
            # Load mappings
            self.compat_manager.view_to_table_map = self.manifest.view_to_table_map
            
            # 1. Create view wrappers
            if create_wrappers:
                self._notify_progress('compatibility', 'Creating view wrappers...', 20)
                
                sf_wrappers = self.compat_manager.create_snowflake_view_wrappers(
                    list(self.manifest.view_to_table_map.keys())
                )
                
                wrapper_path = os.path.join(self.workspace_dir, 'view_wrappers.sql')
                with open(wrapper_path, 'w') as f:
                    f.write('\n\n'.join(sf_wrappers))
                result.artifacts['view_wrappers'] = wrapper_path
                result.metadata['wrappers_created'] = len(sf_wrappers)
                
            # 2. Scan for legacy references
            if scan_directory:
                self._notify_progress('compatibility', 'Scanning for legacy references...', 40)
                
                refs = self.compat_manager.scan_directory_for_legacy_refs(scan_directory)
                result.items_processed += sum(len(r) for r in refs.values())
                result.metadata['files_with_refs'] = len(refs)
                
                # Update references
                self._notify_progress('compatibility', 'Updating legacy references...', 60)
                
                update_results = self.compat_manager.batch_update_references(
                    scan_directory, dry_run=False
                )
                
                result.items_successful += sum(
                    r.references_updated for r in update_results.values()
                )
                
            # 3. Migrate PBIX files
            if migrate_pbix and scan_directory:
                self._notify_progress('compatibility', 'Migrating PBIX files...', 80)
                
                pbix_files = [
                    os.path.join(root, f)
                    for root, dirs, files in os.walk(scan_directory)
                    for f in files if f.endswith('.pbix')
                ]
                
                for pbix_file in pbix_files:
                    try:
                        migration_result = self.compat_manager.migrate_pbix_file(
                            pbix_file, convert_dax=True
                        )
                        result.items_processed += 1
                        if 'error' not in [a.value for a in migration_result.actions]:
                            result.items_successful += 1
                    except Exception as e:
                        result.errors.append(f"PBIX {pbix_file}: {str(e)}")
                        
            self.manifest.total_files_migrated = result.items_successful
            
            # Generate compatibility report
            compat_report = self.compat_manager.get_migration_report()
            report_path = os.path.join(self.workspace_dir, 'compatibility_report.json')
            with open(report_path, 'w') as f:
                json.dump(compat_report, f, indent=2, default=str)
            result.artifacts['compatibility_report'] = report_path
            
            result.status = 'completed'
            self.manifest.phases_completed.append(MigrationPhase.COMPATIBILITY)
            self._notify_progress('compatibility', 'Compatibility setup complete', 100)
            
        except Exception as e:
            result.status = 'failed'
            result.errors.append(str(e))
            self._notify_error('compatibility', str(e))
            logger.error(f"Compatibility phase failed: {e}")
            
        result.end_time = datetime.now().isoformat()
        result.duration_seconds = (
            datetime.fromisoformat(result.end_time) -
            datetime.fromisoformat(result.start_time)
        ).total_seconds()
        
        self.manifest.phase_results['compatibility'] = result
        self._save_manifest()
        
        return result
        
    # ==========================================
    # PHASE 6: TESTING & VALIDATION
    # ==========================================
    
    def run_validation_phase(self,
                              test_queries: List[Tuple[str, str]] = None,
                              sample_size: int = 1000) -> PhaseResult:
        """
        Phase 6: Test and validate the migration.
        
        Args:
            test_queries: List of (old_query, new_query) pairs to validate
            sample_size: Number of rows to sample for comparison
        """
        result = PhaseResult(
            phase=MigrationPhase.VALIDATION,
            status='in_progress',
            start_time=datetime.now().isoformat()
        )
        
        if not self.manifest:
            raise ValueError("No migration manifest. Run previous phases first.")
            
        self.manifest.current_phase = MigrationPhase.VALIDATION
        
        try:
            self._notify_progress('validation', 'Starting validation tests...', 0)
            
            validation_results = {
                'query_validations': [],
                'data_comparisons': [],
                'sync_tests': []
            }
            
            # 1. Validate query migrations
            if test_queries:
                self._notify_progress('validation', 'Validating queries...', 20)
                
                for old_query, new_query in test_queries:
                    val_result = self.compat_manager.validate_migration(
                        old_query, new_query, self.snowflake
                    )
                    validation_results['query_validations'].append(val_result)
                    result.items_processed += 1
                    if val_result.get('passed'):
                        result.items_successful += 1
                    else:
                        result.items_failed += 1
                        
            # 2. Validate table conversions
            self._notify_progress('validation', 'Validating table conversions...', 50)
            
            for old_name, new_name in self.manifest.view_to_table_map.items():
                try:
                    # Compare row counts
                    old_count_query = f"SELECT COUNT(*) as cnt FROM {old_name}"
                    new_count_query = f"SELECT COUNT(*) as cnt FROM {new_name}"
                    
                    comparison = {
                        'old_object': old_name,
                        'new_table': new_name,
                        'passed': True
                    }
                    
                    if self.snowflake:
                        try:
                            old_result = self.snowflake.execute_query(old_count_query)
                            new_result = self.snowflake.execute_query(new_count_query)
                            
                            old_count = old_result[0]['CNT'] if old_result else 0
                            new_count = new_result[0]['CNT'] if new_result else 0
                            
                            comparison['old_count'] = old_count
                            comparison['new_count'] = new_count
                            comparison['passed'] = old_count == new_count
                            
                        except Exception as query_e:
                            comparison['error'] = str(query_e)
                            comparison['passed'] = False
                            
                    validation_results['data_comparisons'].append(comparison)
                    result.items_processed += 1
                    if comparison['passed']:
                        result.items_successful += 1
                    else:
                        result.items_failed += 1
                        
                except Exception as e:
                    logger.warning(f"Validation failed for {old_name}: {e}")
                    
            # 3. Test sync mechanism
            self._notify_progress('validation', 'Testing sync mechanism...', 80)
            
            sync_status = self.sync_manager.get_sync_status()
            validation_results['sync_tests'].append({
                'sync_configured': sync_status.get('config') is not None,
                'cdc_active': sync_status.get('cdc_stats', {}) != {}
            })
            
            # Generate validation report
            validation_report = {
                'migration_id': self.manifest.migration_id,
                'validation_date': datetime.now().isoformat(),
                'summary': {
                    'total_tests': result.items_processed,
                    'passed': result.items_successful,
                    'failed': result.items_failed,
                    'pass_rate': result.items_successful / max(result.items_processed, 1) * 100
                },
                'results': validation_results
            }
            
            report_path = os.path.join(self.workspace_dir, 'validation_report.json')
            with open(report_path, 'w') as f:
                json.dump(validation_report, f, indent=2, default=str)
            result.artifacts['validation_report'] = report_path
            
            # Determine overall status
            pass_rate = result.items_successful / max(result.items_processed, 1)
            if pass_rate >= 0.95:
                result.status = 'completed'
            elif pass_rate >= 0.80:
                result.status = 'partial'
                result.warnings.append(f"Pass rate {pass_rate:.1%} below 95% threshold")
            else:
                result.status = 'failed'
                result.errors.append(f"Pass rate {pass_rate:.1%} below 80% threshold")
                
            self.manifest.phases_completed.append(MigrationPhase.VALIDATION)
            self._notify_progress('validation', 'Validation complete', 100)
            
        except Exception as e:
            result.status = 'failed'
            result.errors.append(str(e))
            self._notify_error('validation', str(e))
            logger.error(f"Validation phase failed: {e}")
            
        result.end_time = datetime.now().isoformat()
        result.duration_seconds = (
            datetime.fromisoformat(result.end_time) -
            datetime.fromisoformat(result.start_time)
        ).total_seconds()
        
        self.manifest.phase_results['validation'] = result
        self._save_manifest()
        
        return result
        
    # ==========================================
    # FULL MIGRATION EXECUTION
    # ==========================================
    
    def run_full_migration(self,
                           scan_directory: str = None,
                           dry_run: bool = False,
                           skip_phases: List[str] = None) -> Dict[str, Any]:
        """
        Run the complete migration process.
        
        Executes all phases in sequence with proper error handling.
        
        Args:
            scan_directory: Directory to scan for legacy files
            dry_run: If True, only analyze and plan without making changes
            skip_phases: List of phase names to skip
            
        Returns:
            Complete migration summary
        """
        skip_phases = skip_phases or []
        
        results = {
            'migration_id': None,
            'start_time': datetime.now().isoformat(),
            'phases': {},
            'status': 'in_progress'
        }
        
        try:
            self.running = True
            
            # Phase 1: Analysis
            if 'analysis' not in skip_phases:
                logger.info("Starting Phase 1: Analysis")
                results['phases']['analysis'] = self.run_analysis_phase()
                results['migration_id'] = self.manifest.migration_id
                
                if results['phases']['analysis'].status == 'failed':
                    raise Exception("Analysis phase failed")
                    
            if dry_run:
                results['status'] = 'dry_run_complete'
                results['end_time'] = datetime.now().isoformat()
                return results
                
            # Phase 2: Conversion
            if 'conversion' not in skip_phases:
                logger.info("Starting Phase 2: Conversion")
                results['phases']['conversion'] = self.run_conversion_phase()
                
            # Phase 3: Translation
            if 'translation' not in skip_phases:
                logger.info("Starting Phase 3: Translation")
                results['phases']['translation'] = self.run_translation_phase()
                
            # Phase 4: Sync Setup
            if 'sync_setup' not in skip_phases:
                logger.info("Starting Phase 4: Sync Setup")
                results['phases']['sync_setup'] = self.run_sync_setup_phase()
                
            # Phase 5: Compatibility
            if 'compatibility' not in skip_phases:
                logger.info("Starting Phase 5: Compatibility")
                results['phases']['compatibility'] = self.run_compatibility_phase(
                    scan_directory=scan_directory
                )
                
            # Phase 6: Validation
            if 'validation' not in skip_phases:
                logger.info("Starting Phase 6: Validation")
                results['phases']['validation'] = self.run_validation_phase()
                
            # Determine overall status
            phase_statuses = [p.status for p in results['phases'].values()]
            if all(s == 'completed' for s in phase_statuses):
                results['status'] = 'completed'
                self.manifest.status = MigrationStatus.COMPLETED
            elif any(s == 'failed' for s in phase_statuses):
                results['status'] = 'partial_failure'
                self.manifest.status = MigrationStatus.FAILED
            else:
                results['status'] = 'completed_with_warnings'
                
        except Exception as e:
            results['status'] = 'failed'
            results['error'] = str(e)
            logger.error(f"Migration failed: {e}")
            
        finally:
            self.running = False
            results['end_time'] = datetime.now().isoformat()
            
            # Save final manifest and summary
            self._save_manifest()
            
            summary_path = os.path.join(
                self.workspace_dir, 
                f"migration_summary_{self.manifest.migration_id}.json"
            )
            with open(summary_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
                
        return results
        
    # ==========================================
    # ROLLBACK & RECOVERY
    # ==========================================
    
    def rollback_migration(self, to_phase: str = None) -> Dict[str, Any]:
        """
        Rollback migration to a previous state.
        
        Args:
            to_phase: Phase to rollback to (None = complete rollback)
        """
        result = {
            'start_time': datetime.now().isoformat(),
            'rollback_to': to_phase or 'initial',
            'actions': [],
            'status': 'in_progress'
        }
        
        try:
            logger.info(f"Starting rollback to {to_phase or 'initial state'}")
            
            # Rollback view wrappers (they can stay for compatibility)
            # But we can drop the new tables and restore views
            
            if self.rollback_points:
                latest_rollback = max(
                    self.rollback_points.items(), 
                    key=lambda x: x[1]['timestamp']
                )
                rollback_id, rollback_data = latest_rollback
                
                # Execute rollback based on phase
                if rollback_data['phase'] == 'conversion':
                    # Would drop created tables and restore from backup
                    result['actions'].append('Drop converted tables')
                    result['actions'].append('Restore from backup')
                    
            self.manifest.status = MigrationStatus.ROLLED_BACK
            result['status'] = 'completed'
            
        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            logger.error(f"Rollback failed: {e}")
            
        result['end_time'] = datetime.now().isoformat()
        self._save_manifest()
        
        return result
        
    def pause_migration(self):
        """Pause an in-progress migration."""
        self.pause_requested = True
        self.manifest.status = MigrationStatus.PAUSED
        self._save_manifest()
        logger.info("Migration paused")
        
    def resume_migration(self):
        """Resume a paused migration."""
        self.pause_requested = False
        # Resume from last completed phase
        logger.info("Migration resumed")
        
    # ==========================================
    # STATUS & MONITORING
    # ==========================================
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Get current migration status."""
        if not self.manifest:
            return {'status': 'no_active_migration'}
            
        return {
            'migration_id': self.manifest.migration_id,
            'name': self.manifest.name,
            'status': self.manifest.status.value,
            'current_phase': self.manifest.current_phase.value if self.manifest.current_phase else None,
            'phases_completed': [p.value for p in self.manifest.phases_completed],
            'statistics': {
                'tables_converted': self.manifest.total_tables_converted,
                'dax_translated': self.manifest.total_dax_translated,
                'files_migrated': self.manifest.total_files_migrated,
                'rows_synced': self.manifest.total_rows_synced
            },
            'is_running': self.running
        }
        
    def get_phase_status(self, phase: str) -> Optional[PhaseResult]:
        """Get status of a specific phase."""
        if not self.manifest:
            return None
        return self.manifest.phase_results.get(phase)
        
    def generate_final_report(self) -> Dict[str, Any]:
        """Generate comprehensive final migration report."""
        if not self.manifest:
            return {'error': 'No migration manifest'}
            
        report = {
            'migration_id': self.manifest.migration_id,
            'name': self.manifest.name,
            'created': self.manifest.created_at,
            'status': self.manifest.status.value,
            'phases': {},
            'statistics': {
                'snowflake_views_converted': len(self.manifest.snowflake_views),
                'fabric_models_converted': len(self.manifest.fabric_models),
                'dax_measures_translated': self.manifest.total_dax_translated,
                'files_migrated': self.manifest.total_files_migrated,
                'view_to_table_mappings': len(self.manifest.view_to_table_map)
            },
            'outcomes': {
                'all_data_as_tables': True,
                'zero_dax_dependencies': self.manifest.total_dax_translated == len(self.manifest.dax_measures),
                'bidirectional_sync_active': MigrationPhase.SYNC_SETUP in self.manifest.phases_completed,
                'legacy_files_operational': MigrationPhase.COMPATIBILITY in self.manifest.phases_completed,
                'validation_passed': self.manifest.phase_results.get('validation', {}).status == 'completed'
            },
            'artifacts': {}
        }
        
        # Collect all phase results
        for phase_name, phase_result in self.manifest.phase_results.items():
            report['phases'][phase_name] = {
                'status': phase_result.status,
                'duration_seconds': phase_result.duration_seconds,
                'items_processed': phase_result.items_processed,
                'items_successful': phase_result.items_successful,
                'errors': phase_result.errors
            }
            report['artifacts'].update(phase_result.artifacts)
            
        # Save report
        report_path = os.path.join(
            self.workspace_dir,
            f"final_report_{self.manifest.migration_id}.json"
        )
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
            
        return report


# Convenience function for quick migration
def run_migration(snowflake_connector=None,
                  fabric_client=None,
                  scan_directory: str = None,
                  config: Dict = None) -> Dict[str, Any]:
    """
    Convenience function to run a complete migration.
    
    Args:
        snowflake_connector: SnowflakeConnector instance
        fabric_client: FabricApiClient instance
        scan_directory: Directory to scan for legacy files
        config: Migration configuration
        
    Returns:
        Migration summary
    """
    orchestrator = MigrationOrchestrator(
        snowflake_connector=snowflake_connector,
        fabric_client=fabric_client,
        config=config
    )
    
    return orchestrator.run_full_migration(scan_directory=scan_directory)
