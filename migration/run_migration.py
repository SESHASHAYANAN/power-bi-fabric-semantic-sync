"""
Snowflake-Fabric Migration CLI

Command-line interface for running the comprehensive migration.
Provides interactive and batch execution modes.

Usage:
    python run_migration.py --mode interactive
    python run_migration.py --phase analysis
    python run_migration.py --full --dry-run
    python run_migration.py --rollback
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('migration.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


def print_banner():
    """Print migration tool banner."""
    banner = """
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║     ❄️  Snowflake ↔ Fabric Migration Tool  📊                   ║
║                                                                  ║
║     Comprehensive View-to-Table Migration with:                 ║
║     • DAX → SQL Conversion                                      ║
║     • Bidirectional Sync                                        ║
║     • Backward Compatibility                                    ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
"""
    print(banner)


def progress_callback(progress_info):
    """Callback for progress updates."""
    phase = progress_info.get('phase', '')
    message = progress_info.get('message', '')
    percent = progress_info.get('percent', 0)
    
    bar_length = 40
    filled = int(bar_length * percent / 100)
    bar = '█' * filled + '░' * (bar_length - filled)
    
    print(f"\r[{phase.upper():12}] [{bar}] {percent:5.1f}% - {message}", end='', flush=True)
    
    if percent >= 100:
        print()  # New line when complete


def error_callback(error_info):
    """Callback for error notifications."""
    phase = error_info.get('phase', '')
    error = error_info.get('error', '')
    print(f"\n❌ ERROR in {phase}: {error}")


def get_connectors():
    """Initialize and return connectors if credentials are available."""
    snowflake_connector = None
    fabric_client = None
    
    # Try to initialize Snowflake
    if os.getenv('SNOWFLAKE_ACCOUNT'):
        try:
            from fabric_snowflake_sync import SnowflakeConnector
            snowflake_connector = SnowflakeConnector()
            if snowflake_connector.connect():
                print("✅ Connected to Snowflake")
            else:
                print("⚠️  Snowflake connection failed - continuing without")
                snowflake_connector = None
        except Exception as e:
            print(f"⚠️  Snowflake setup failed: {e}")
            
    # Try to initialize Fabric
    if os.getenv('FABRIC_TENANT_ID'):
        try:
            from fabric_snowflake_sync import FabricApiClient
            fabric_client = FabricApiClient()
            if fabric_client.authenticate():
                print("✅ Connected to Microsoft Fabric")
            else:
                print("⚠️  Fabric authentication failed - continuing without")
                fabric_client = None
        except Exception as e:
            print(f"⚠️  Fabric setup failed: {e}")
            
    return snowflake_connector, fabric_client


def run_interactive_mode():
    """Run in interactive mode with menu."""
    from migration.migration_orchestrator import MigrationOrchestrator
    
    snowflake, fabric = get_connectors()
    orchestrator = MigrationOrchestrator(
        snowflake_connector=snowflake,
        fabric_client=fabric
    )
    orchestrator.set_progress_callback(progress_callback)
    orchestrator.set_error_callback(error_callback)
    
    while True:
        print("\n" + "=" * 60)
        print("MIGRATION MENU")
        print("=" * 60)
        print("1. Create New Migration")
        print("2. Load Existing Migration")
        print("3. Run Analysis Phase")
        print("4. Run Conversion Phase")
        print("5. Run DAX Translation Phase")
        print("6. Run Sync Setup Phase")
        print("7. Run Compatibility Phase")
        print("8. Run Validation Phase")
        print("9. Run Full Migration")
        print("10. View Migration Status")
        print("11. Generate Final Report")
        print("12. Rollback Migration")
        print("0. Exit")
        print("-" * 60)
        
        choice = input("Enter choice: ").strip()
        
        if choice == '0':
            print("Goodbye!")
            break
            
        elif choice == '1':
            name = input("Migration name (or press Enter for default): ").strip()
            manifest = orchestrator.create_migration(name or None)
            print(f"✅ Created migration: {manifest.migration_id}")
            
        elif choice == '2':
            migration_id = input("Enter migration ID: ").strip()
            try:
                manifest = orchestrator.load_migration(migration_id)
                print(f"✅ Loaded migration: {manifest.name}")
            except Exception as e:
                print(f"❌ Failed to load: {e}")
                
        elif choice == '3':
            print("\n🔍 Running Analysis Phase...")
            result = orchestrator.run_analysis_phase()
            print(f"Status: {result.status}")
            print(f"Items processed: {result.items_processed}")
            
        elif choice == '4':
            incremental = input("Use incremental refresh? (y/n): ").lower() == 'y'
            print("\n🔄 Running Conversion Phase...")
            result = orchestrator.run_conversion_phase(incremental=incremental)
            print(f"Status: {result.status}")
            print(f"Tables converted: {result.items_successful}")
            
        elif choice == '5':
            dialect = input("SQL dialect (snowflake/tsql) [snowflake]: ").strip() or 'snowflake'
            print("\n📝 Running Translation Phase...")
            result = orchestrator.run_translation_phase(dialect=dialect)
            print(f"Status: {result.status}")
            print(f"Measures translated: {result.items_successful}")
            
        elif choice == '6':
            enable_cdc = input("Enable CDC? (y/n) [y]: ").lower() != 'n'
            interval = int(input("Sync interval in minutes [15]: ").strip() or '15')
            print("\n⚡ Running Sync Setup Phase...")
            result = orchestrator.run_sync_setup_phase(
                enable_cdc=enable_cdc,
                sync_interval=interval
            )
            print(f"Status: {result.status}")
            
        elif choice == '7':
            scan_dir = input("Directory to scan (or press Enter to skip): ").strip()
            print("\n🔧 Running Compatibility Phase...")
            result = orchestrator.run_compatibility_phase(
                scan_directory=scan_dir or None
            )
            print(f"Status: {result.status}")
            print(f"Files migrated: {result.items_successful}")
            
        elif choice == '8':
            print("\n✅ Running Validation Phase...")
            result = orchestrator.run_validation_phase()
            print(f"Status: {result.status}")
            print(f"Tests passed: {result.items_successful}/{result.items_processed}")
            
        elif choice == '9':
            dry_run = input("Dry run only? (y/n) [n]: ").lower() == 'y'
            scan_dir = input("Directory to scan for legacy files: ").strip()
            print("\n🚀 Running Full Migration...")
            results = orchestrator.run_full_migration(
                scan_directory=scan_dir or None,
                dry_run=dry_run
            )
            print(f"\n✅ Migration {results['status']}")
            
        elif choice == '10':
            status = orchestrator.get_migration_status()
            print("\n" + json.dumps(status, indent=2))
            
        elif choice == '11':
            report = orchestrator.generate_final_report()
            print("\n📋 Final Report:")
            print(json.dumps(report, indent=2))
            
        elif choice == '12':
            confirm = input("Are you sure you want to rollback? (yes/no): ")
            if confirm.lower() == 'yes':
                result = orchestrator.rollback_migration()
                print(f"Rollback status: {result['status']}")
            else:
                print("Rollback cancelled")
                
        else:
            print("Invalid choice. Please try again.")


def run_single_phase(phase: str, args):
    """Run a single migration phase."""
    from migration.migration_orchestrator import MigrationOrchestrator
    
    snowflake, fabric = get_connectors()
    orchestrator = MigrationOrchestrator(
        snowflake_connector=snowflake,
        fabric_client=fabric,
        workspace_dir=args.workspace
    )
    orchestrator.set_progress_callback(progress_callback)
    orchestrator.set_error_callback(error_callback)
    
    # Load or create migration
    if args.migration_id:
        orchestrator.load_migration(args.migration_id)
    else:
        orchestrator.create_migration(args.name)
        
    # Run specified phase
    phase_map = {
        'analysis': orchestrator.run_analysis_phase,
        'conversion': lambda: orchestrator.run_conversion_phase(
            dry_run=args.dry_run,
            incremental=not args.no_incremental
        ),
        'translation': lambda: orchestrator.run_translation_phase(
            dialect=args.dialect
        ),
        'sync_setup': lambda: orchestrator.run_sync_setup_phase(
            enable_cdc=not args.no_cdc,
            sync_interval=args.sync_interval
        ),
        'compatibility': lambda: orchestrator.run_compatibility_phase(
            scan_directory=args.scan_dir,
            create_wrappers=not args.no_wrappers
        ),
        'validation': orchestrator.run_validation_phase
    }
    
    if phase in phase_map:
        print(f"\n🚀 Running {phase} phase...")
        result = phase_map[phase]()
        print(f"\n✅ Phase complete: {result.status}")
        print(f"   Items processed: {result.items_processed}")
        print(f"   Successful: {result.items_successful}")
        print(f"   Failed: {result.items_failed}")
        
        if result.errors:
            print(f"   Errors: {result.errors}")
        if result.artifacts:
            print(f"   Artifacts: {list(result.artifacts.keys())}")
            
        return result.status == 'completed'
    else:
        print(f"Unknown phase: {phase}")
        return False


def run_full_migration(args):
    """Run full migration."""
    from migration.migration_orchestrator import MigrationOrchestrator
    
    snowflake, fabric = get_connectors()
    orchestrator = MigrationOrchestrator(
        snowflake_connector=snowflake,
        fabric_client=fabric,
        workspace_dir=args.workspace
    )
    orchestrator.set_progress_callback(progress_callback)
    orchestrator.set_error_callback(error_callback)
    
    print("\n🚀 Starting Full Migration...")
    print("=" * 60)
    
    results = orchestrator.run_full_migration(
        scan_directory=args.scan_dir,
        dry_run=args.dry_run,
        skip_phases=args.skip.split(',') if args.skip else None
    )
    
    print("\n" + "=" * 60)
    print(f"Migration Status: {results['status']}")
    print("=" * 60)
    
    for phase, phase_result in results.get('phases', {}).items():
        status_icon = '✅' if phase_result.status == 'completed' else '⚠️' if phase_result.status == 'partial' else '❌'
        print(f"  {status_icon} {phase}: {phase_result.status}")
        
    return results['status'] in ['completed', 'completed_with_warnings']


def main():
    """Main entry point."""
    print_banner()
    
    parser = argparse.ArgumentParser(
        description='Snowflake-Fabric Migration Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode
  python run_migration.py --mode interactive
  
  # Run analysis only
  python run_migration.py --phase analysis
  
  # Run conversion with incremental refresh
  python run_migration.py --phase conversion --incremental
  
  # Full migration with dry run
  python run_migration.py --full --dry-run
  
  # Full migration specifying scan directory
  python run_migration.py --full --scan-dir ./legacy_files
  
  # Skip specific phases
  python run_migration.py --full --skip translation,compatibility
"""
    )
    
    parser.add_argument('--mode', choices=['interactive', 'batch'],
                       default='batch', help='Execution mode')
    parser.add_argument('--phase', choices=[
        'analysis', 'conversion', 'translation', 
        'sync_setup', 'compatibility', 'validation'
    ], help='Run a specific phase')
    parser.add_argument('--full', action='store_true',
                       help='Run full migration')
    parser.add_argument('--dry-run', action='store_true',
                       help='Dry run mode (no changes)')
    parser.add_argument('--rollback', action='store_true',
                       help='Rollback migration')
    
    # Phase-specific options
    parser.add_argument('--no-incremental', action='store_true',
                       help='Disable incremental refresh')
    parser.add_argument('--dialect', default='snowflake',
                       choices=['snowflake', 'tsql'],
                       help='SQL dialect for DAX translation')
    parser.add_argument('--no-cdc', action='store_true',
                       help='Disable CDC setup')
    parser.add_argument('--sync-interval', type=int, default=15,
                       help='Sync interval in minutes')
    parser.add_argument('--no-wrappers', action='store_true',
                       help='Skip creating view wrappers')
    parser.add_argument('--scan-dir', 
                       help='Directory to scan for legacy files')
    
    # Migration identification
    parser.add_argument('--migration-id',
                       help='Existing migration ID to resume')
    parser.add_argument('--name',
                       help='Name for new migration')
    parser.add_argument('--workspace', default='./migration_workspace',
                       help='Workspace directory for artifacts')
    parser.add_argument('--skip',
                       help='Comma-separated phases to skip')
    
    args = parser.parse_args()
    
    # Execute based on arguments
    if args.mode == 'interactive':
        run_interactive_mode()
        
    elif args.rollback:
        from migration.migration_orchestrator import MigrationOrchestrator
        orchestrator = MigrationOrchestrator(workspace_dir=args.workspace)
        if args.migration_id:
            orchestrator.load_migration(args.migration_id)
        result = orchestrator.rollback_migration()
        print(f"Rollback: {result['status']}")
        
    elif args.phase:
        success = run_single_phase(args.phase, args)
        sys.exit(0 if success else 1)
        
    elif args.full:
        success = run_full_migration(args)
        sys.exit(0 if success else 1)
        
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
