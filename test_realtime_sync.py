"""
Test Script for Real-Time Bidirectional Sync Service
Tests the Fabric <-> Snowflake synchronization functionality
"""

import os
import sys
import io

# Fix console encoding for Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import pandas as pd
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(__file__))

def test_realtime_sync():
    """Test the real-time sync service functionality."""
    print("=" * 70)
    print("Real-Time Bidirectional Sync Service - Test Suite")
    print("=" * 70)
    
    # Import the service
    try:
        from realtime_sync_service import RealtimeSyncService, get_realtime_sync_service
        print("✅ Successfully imported RealtimeSyncService")
    except ImportError as e:
        print(f"❌ Failed to import RealtimeSyncService: {e}")
        return False
    
    # Create service instance
    print("\n1. Initializing sync service...")
    try:
        service = get_realtime_sync_service(sync_interval=60)
        print(f"   ✅ Service initialized")
        print(f"   📁 Uploaded datasets path: {service.uploaded_datasets_path}")
        print(f"   📁 Fabric sync path: {service.fabric_sync_path}")
        print(f"   ⏱️  Sync interval: {service.sync_interval}s")
    except Exception as e:
        print(f"   ❌ Failed to initialize service: {e}")
        return False
    
    # Test comparison
    print("\n2. Comparing Fabric and Snowflake data...")
    try:
        comparison = service.compare_systems()
        print(f"   📊 Fabric items: {len(comparison.get('fabric_items', []))}")
        print(f"   ❄️  Snowflake items: {len(comparison.get('snowflake_items', []))}")
        print(f"   ⚠️  Missing in Snowflake: {comparison.get('missing_in_snowflake', [])}")
        print(f"   ⚠️  Missing in Fabric: {comparison.get('missing_in_fabric', [])}")
        print(f"   ✅ Already synced: {comparison.get('synced', [])}")
    except Exception as e:
        print(f"   ❌ Comparison failed: {e}")
    
    # Test sync status
    print("\n3. Getting sync status...")
    try:
        status = service.get_sync_status()
        print(f"   🏃 Running: {status.get('running', False)}")
        print(f"   ⏱️  Interval: {status.get('interval', 0)}s")
        print(f"   📁 Files synced: {status.get('synced_files_count', 0)}")
        print(f"   📊 Fabric synced: {status.get('fabric_synced_count', 0)}")
        print(f"   ❄️  Snowflake synced: {status.get('snowflake_synced_count', 0)}")
        print(f"   📅 Last sync: {status.get('last_full_sync', 'Never')}")
    except Exception as e:
        print(f"   ❌ Status check failed: {e}")
    
    # Test creating a sample CSV and syncing it
    print("\n4. Creating test CSV file...")
    try:
        test_data = {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 30, 35, 28, 32],
            "department": ["Engineering", "Sales", "Marketing", "HR", "Engineering"],
            "salary": [75000.50, 65000.00, 72000.25, 58000.00, 85000.75]
        }
        df = pd.DataFrame(test_data)
        
        test_file = os.path.join(service.uploaded_datasets_path, "test_employees.csv")
        df.to_csv(test_file, index=False)
        print(f"   ✅ Created test file: {test_file}")
        print(f"   📊 Rows: {len(df)}, Columns: {len(df.columns)}")
    except Exception as e:
        print(f"   ❌ Failed to create test file: {e}")
        test_file = None
    
    # Test syncing the file
    if test_file:
        print("\n5. Syncing test file to both systems...")
        try:
            result = service.sync_file_to_both(test_file)
            print(f"   ✅ Sync result: {result.get('success', False)}")
            
            sf_result = result.get('snowflake', {})
            print(f"   ❄️  Snowflake: {sf_result.get('status', 'unknown')}")
            if sf_result.get('status') == 'success':
                print(f"      Table: {sf_result.get('table_name', 'N/A')}")
                print(f"      Rows inserted: {sf_result.get('rows_inserted', 0)}")
            else:
                print(f"      Error: {sf_result.get('message', 'Unknown')}")
            
            fab_result = result.get('fabric', {})
            print(f"   📊 Fabric: {fab_result.get('status', 'unknown')}")
            if fab_result.get('status') == 'success':
                print(f"      Model file: {fab_result.get('model_file', 'N/A')}")
                print(f"      Columns: {fab_result.get('columns_count', 0)}")
            else:
                print(f"      Error: {fab_result.get('message', 'Unknown')}")
                
        except Exception as e:
            print(f"   ❌ Sync failed: {e}")
    
    # Test full bidirectional sync
    print("\n6. Running full bidirectional sync...")
    try:
        result = service.run_full_bidirectional_sync()
        print(f"   ✅ Full sync result: {result.get('success', False)}")
        
        staged = result.get('staged_files', {})
        print(f"   📁 Staged files: {staged.get('synced', 0)}/{staged.get('total', 0)} synced")
        
        f2s = result.get('fabric_to_snowflake', {})
        print(f"   📊→❄️  Fabric to Snowflake: {f2s.get('synced', 0)}/{f2s.get('total', 0)} synced")
        
        s2f = result.get('snowflake_to_fabric', {})
        print(f"   ❄️→📊 Snowflake to Fabric: {s2f.get('synced', 0)}/{s2f.get('total', 0)} synced")
        
    except Exception as e:
        print(f"   ❌ Full sync failed: {e}")
    
    # Show recent logs
    print("\n7. Recent sync logs:")
    try:
        status = service.get_sync_status()
        logs = status.get('recent_logs', [])[-5:]
        for log in logs:
            print(f"   [{log.get('type', 'INFO')}] {log.get('timestamp', '')[:19]} - {log.get('message', '')}")
    except Exception as e:
        print(f"   ❌ Failed to get logs: {e}")
    
    print("\n" + "=" * 70)
    print("Test complete!")
    print("=" * 70)
    
    return True


if __name__ == "__main__":
    test_realtime_sync()
