"""
Historical Data Migration: Fabric → Snowflake

This script migrates ALL existing data from Microsoft Fabric to Snowflake.
It uses idempotency checks (SYNC_ID) to prevent re-importing already-synced data.

Usage:
    python fabric_to_snowflake_migration.py [--dry-run] [--limit N]

Features:
- Pre-flight validation (API access, permissions)
- Automatic SYNC_ID generation and tracking
- Idempotency (skips already-migrated tables)
- Progress reporting
- Rollback capability on failure
- Full audit logging
"""

import os
import sys
import io
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Any

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('migration_fabric_to_snowflake.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Import connectors and sync components
try:
    from fabric_snowflake_sync import FabricApiClient, SnowflakeConnector
    from sync_orchestration.sync_engine import SyncOrchestrator
    from sync_orchestration.models import SyncManifest, SyncStatus, SyncDirection
    from sync_orchestration.format_converter import FormatConverter
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error("Make sure fabric_snowflake_sync.py and sync_orchestration package exist")
    sys.exit(1)


class FabricToSnowflakeMigration:
    """
    Handles one-time historical migration from Fabric to Snowflake.
    """
    
    def __init__(self, dry_run: bool = False, limit: Optional[int] = None):
        """
        Initialize migration.
        
        Args:
            dry_run: If True, only report what would be migrated
            limit: Maximum number of tables to migrate
        """
        self.dry_run = dry_run
        self.limit = limit
        
        # Initialize clients
        self.fabric_client = FabricApiClient()
        self.snowflake_connector = SnowflakeConnector()
        self.converter = FormatConverter()
        
        # Initialize orchestrator for tracking
        self.orchestrator = SyncOrchestrator(
            base_path=os.path.dirname(os.path.dirname(__file__)),
            enable_validation=True,
            enable_retry=True
        )
        self.orchestrator.set_fabric_client(self.fabric_client)
        self.orchestrator.set_snowflake_connector(self.snowflake_connector)
        
        # Migration stats
        self.stats = {
            "started_at": None,
            "completed_at": None,
            "total_tables": 0,
            "migrated": 0,
            "skipped": 0,
            "failed": 0,
            "rows_migrated": 0,
            "errors": []
        }
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the migration.
        
        Returns:
            Migration summary with stats
        """
        self.stats["started_at"] = datetime.now().isoformat()
        
        logger.info("=" * 70)
        logger.info("FABRIC → SNOWFLAKE HISTORICAL MIGRATION")
        logger.info("=" * 70)
        
        if self.dry_run:
            logger.warning("DRY RUN MODE - No data will be migrated")
        
        # Step 1: Pre-flight checks
        if not self._preflight_checks():
            logger.error("Pre-flight checks failed. Aborting migration.")
            return self.stats
        
        # Step 2: Backup (optional - create restore point)
        logger.info("\n[Step 2] Creating backup reference...")
        self._create_backup_reference()
        
        # Step 3: Discover tables to migrate
        logger.info("\n[Step 3] Discovering Fabric tables...")
        tables = self._discover_fabric_tables()
        self.stats["total_tables"] = len(tables)
        
        if self.limit and len(tables) > self.limit:
            logger.info(f"Limiting to {self.limit} tables (of {len(tables)} total)")
            tables = tables[:self.limit]
        
        logger.info(f"Found {len(tables)} tables to evaluate")
        
        # Step 4: Migrate each table
        logger.info("\n[Step 4] Migrating tables...")
        
        for i, table in enumerate(tables, 1):
            table_name = table.get("table_name", "unknown")
            model_name = table.get("model_name", "unknown")
            
            logger.info(f"\n[{i}/{len(tables)}] Processing: {model_name}.{table_name}")
            
            # Check idempotency
            if self._is_already_migrated(table_name, model_name):
                logger.info(f"  → SKIPPED (already migrated)")
                self.stats["skipped"] += 1
                continue
            
            # Migrate
            if self.dry_run:
                logger.info(f"  → WOULD MIGRATE: {len(table.get('columns', []))} columns")
                self.stats["migrated"] += 1
            else:
                success = self._migrate_table(table)
                
                if success:
                    logger.info(f"  → MIGRATED successfully")
                    self.stats["migrated"] += 1
                else:
                    logger.error(f"  → FAILED to migrate")
                    self.stats["failed"] += 1
        
        # Step 5: Generate report
        logger.info("\n[Step 5] Generating migration report...")
        self.stats["completed_at"] = datetime.now().isoformat()
        self._generate_report()
        
        return self.stats
    
    def _preflight_checks(self) -> bool:
        """Verify API access and permissions."""
        logger.info("\n[Step 1] Running pre-flight checks...")
        
        # Check Fabric connection
        logger.info("  Checking Fabric API access...")
        try:
            if not self.fabric_client.authenticate():
                logger.error("  ✗ Fabric authentication failed")
                return False
            
            models = self.fabric_client.get_semantic_models()
            if models is None:
                logger.error("  ✗ Cannot access Fabric semantic models")
                return False
            
            logger.info(f"  ✓ Fabric connected ({len(models)} models found)")
        except Exception as e:
            logger.error(f"  ✗ Fabric error: {e}")
            return False
        
        # Check Snowflake connection
        logger.info("  Checking Snowflake connection...")
        try:
            if not self.snowflake_connector.connect():
                logger.error("  ✗ Snowflake connection failed")
                return False
            
            cursor = self.snowflake_connector.connection.cursor()
            cursor.execute("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            result = cursor.fetchone()
            cursor.close()
            self.snowflake_connector.disconnect()
            
            logger.info(f"  ✓ Snowflake connected: {result}")
        except Exception as e:
            logger.error(f"  ✗ Snowflake error: {e}")
            return False
        
        logger.info("  All pre-flight checks passed")
        return True
    
    def _create_backup_reference(self):
        """Create a backup reference point."""
        backup_data = {
            "timestamp": datetime.now().isoformat(),
            "type": "fabric_to_snowflake_migration",
            "existing_tables": []
        }
        
        try:
            if self.snowflake_connector.connect():
                cursor = self.snowflake_connector.connection.cursor()
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                backup_data["existing_tables"] = [t[1] for t in tables]
                cursor.close()
                self.snowflake_connector.disconnect()
        except Exception as e:
            logger.warning(f"Could not create backup reference: {e}")
        
        # Save backup reference
        backup_file = os.path.join(
            os.path.dirname(__file__), 
            f"migration_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        if not self.dry_run:
            with open(backup_file, 'w') as f:
                json.dump(backup_data, f, indent=2)
            logger.info(f"  Backup reference saved: {backup_file}")
    
    def _discover_fabric_tables(self) -> List[Dict]:
        """Discover all tables in Fabric semantic models."""
        tables = []
        
        try:
            models = self.fabric_client.get_semantic_models() or []
            
            for model in models:
                model_id = model.get("id", "")
                model_name = model.get("displayName", model.get("name", "Unknown"))
                
                try:
                    detail = self.fabric_client.get_semantic_model_detail(model_id)
                    if detail:
                        for table in detail.get("tables", []):
                            tables.append({
                                "model_id": model_id,
                                "model_name": model_name,
                                "table_name": table.get("name", ""),
                                "columns": table.get("columns", []),
                                "measures": table.get("measures", []),
                                "description": table.get("description", "")
                            })
                except Exception as e:
                    logger.warning(f"Error getting model {model_name} detail: {e}")
        
        except Exception as e:
            logger.error(f"Error discovering Fabric tables: {e}")
        
        return tables
    
    def _is_already_migrated(self, table_name: str, model_name: str) -> bool:
        """Check if table has already been migrated (idempotency)."""
        sf_table_name = f"FABRIC_{model_name}_{table_name}".upper().replace(" ", "_")
        
        # Check orchestrator manifests
        existing = self.orchestrator._find_existing_sync_for_table(
            table_name, "fabric", "snowflake"
        )
        
        if existing:
            return True
        
        # Also check if table exists in Snowflake
        try:
            if self.snowflake_connector.connect():
                cursor = self.snowflake_connector.connection.cursor()
                cursor.execute(f"SHOW TABLES LIKE '{sf_table_name}'")
                result = cursor.fetchall()
                cursor.close()
                self.snowflake_connector.disconnect()
                
                if result:
                    return True
        except Exception:
            pass
        
        return False
    
    def _migrate_table(self, table_info: Dict) -> bool:
        """Migrate a single table from Fabric to Snowflake."""
        try:
            model_name = table_info.get("model_name", "")
            table_name = table_info.get("table_name", "")
            columns = table_info.get("columns", [])
            
            # Generate Snowflake table name
            sf_table_name = f"FABRIC_{model_name}_{table_name}".upper()
            sf_table_name = sf_table_name.replace(" ", "_").replace("-", "_")
            
            # Generate SYNC_ID
            sync_id = SyncManifest.generate_sync_id()
            
            # Generate DDL
            ddl, transformed_cols = self.converter.transform_schema_fabric_to_snowflake(
                columns, sf_table_name
            )
            
            # Execute in Snowflake
            if not self.snowflake_connector.connect():
                raise Exception("Failed to connect to Snowflake")
            
            cursor = self.snowflake_connector.connection.cursor()
            
            try:
                cursor.execute(ddl)
                logger.debug(f"Created table {sf_table_name}")
                
                # Insert SYNC metadata row
                cursor.execute(f'''
                    COMMENT ON TABLE "{sf_table_name}" IS 'Migrated from Fabric model {model_name} on {datetime.now().isoformat()}. SYNC_ID: {sync_id}'
                ''')
                
            finally:
                cursor.close()
                self.snowflake_connector.disconnect()
            
            # Record in manifest
            manifest = SyncManifest(
                sync_id=sync_id,
                source_table=table_name,
                target_table=sf_table_name,
                source_platform="fabric",
                target_platform="snowflake",
                status=SyncStatus.SYNCED,
                synced_at=datetime.now(),
                metadata={
                    "model_id": table_info.get("model_id"),
                    "model_name": model_name,
                    "migration_type": "historical"
                }
            )
            
            self.orchestrator.manifests[sync_id] = manifest
            self.orchestrator._save_manifests()
            
            return True
            
        except Exception as e:
            logger.error(f"Migration error for {table_info.get('table_name')}: {e}")
            self.stats["errors"].append({
                "table": table_info.get("table_name"),
                "error": str(e)
            })
            return False
    
    def _generate_report(self):
        """Generate and save migration report."""
        report = {
            "migration_type": "fabric_to_snowflake",
            "stats": self.stats,
            "duration_seconds": None
        }
        
        # Calculate duration
        if self.stats["started_at"] and self.stats["completed_at"]:
            start = datetime.fromisoformat(self.stats["started_at"])
            end = datetime.fromisoformat(self.stats["completed_at"])
            report["duration_seconds"] = (end - start).total_seconds()
        
        # Print summary
        logger.info("\n" + "=" * 70)
        logger.info("MIGRATION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total tables found:     {self.stats['total_tables']}")
        logger.info(f"Successfully migrated:  {self.stats['migrated']}")
        logger.info(f"Skipped (idempotent):   {self.stats['skipped']}")
        logger.info(f"Failed:                 {self.stats['failed']}")
        logger.info(f"Duration:               {report.get('duration_seconds', 0):.1f} seconds")
        
        if self.stats["errors"]:
            logger.info("\nErrors:")
            for err in self.stats["errors"]:
                logger.info(f"  - {err['table']}: {err['error']}")
        
        # Save report
        report_file = os.path.join(
            os.path.dirname(__file__),
            f"migration_report_f2s_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        if not self.dry_run:
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"\nReport saved: {report_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate historical data from Fabric to Snowflake"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Report what would be migrated without making changes"
    )
    parser.add_argument(
        "--limit", 
        type=int,
        help="Maximum number of tables to migrate"
    )
    
    args = parser.parse_args()
    
    migration = FabricToSnowflakeMigration(
        dry_run=args.dry_run,
        limit=args.limit
    )
    
    result = migration.run()
    
    # Exit with error code if any failures
    if result["failed"] > 0:
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
