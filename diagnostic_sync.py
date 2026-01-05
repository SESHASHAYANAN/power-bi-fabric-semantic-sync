"""
Diagnostic Script for Fabric-Snowflake Sync
Check what models exist in Fabric and what tables exist in Snowflake
"""
import os
import sys
import io
import json

# Fix console encoding for Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.dirname(__file__))

from fabric_snowflake_sync import FabricApiClient, SnowflakeConnector

print("=" * 70)
print("Fabric-Snowflake Sync Diagnostic")
print("=" * 70)

# Check Fabric
print("\n1. Checking Fabric Semantic Models...")
fabric = FabricApiClient()
if fabric.authenticate():
    models = fabric.get_semantic_models() or []
    print(f"   Found {len(models)} models in Fabric:")
    
    for i, model in enumerate(models, 1):
        model_id = model.get("id", "N/A")
        model_name = model.get("displayName", model.get("name", "Unknown"))
        print(f"\n   [{i}] {model_name}")
        print(f"       ID: {model_id}")
        
        # Get model details
        if model_id:
            try:
                detail = fabric.get_semantic_model_detail(model_id)
                if detail:
                    tables = detail.get("tables", [])
                    print(f"       Tables: {len(tables)}")
                    for table in tables:
                        table_name = table.get("name", "Unknown")
                        columns = table.get("columns", [])
                        measures = table.get("measures", [])
                        print(f"         - {table_name}: {len(columns)} columns, {len(measures)} measures")
                        
                        # Show column details
                        for col in columns[:5]:  # First 5 columns
                            print(f"           * {col.get('name', 'N/A')} ({col.get('dataType', 'N/A')})")
                        if len(columns) > 5:
                            print(f"           ... and {len(columns) - 5} more columns")
            except Exception as e:
                print(f"       Error getting details: {e}")
else:
    print("   ERROR: Could not authenticate with Fabric")

# Check Snowflake
print("\n" + "-" * 70)
print("\n2. Checking Snowflake Tables...")
snowflake = SnowflakeConnector()
if snowflake.connect():
    cursor = snowflake.connection.cursor()
    
    # Get tables
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    print(f"   Found {len(tables)} tables in Snowflake:")
    
    for table_row in tables:
        table_name = table_row[1]
        if table_name.startswith("_") or table_name.startswith("SYS"):
            continue
        
        try:
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            row_count = cursor.fetchone()[0]
            
            cursor.execute(f'DESCRIBE TABLE "{table_name}"')
            columns = cursor.fetchall()
            
            print(f"\n   [{table_name}]")
            print(f"       Rows: {row_count}, Columns: {len(columns)}")
            for col in columns[:5]:
                print(f"         - {col[0]} ({col[1]})")
            if len(columns) > 5:
                print(f"         ... and {len(columns) - 5} more columns")
        except Exception as e:
            print(f"   [{table_name}] Error: {e}")
    
    cursor.close()
    snowflake.disconnect()
else:
    print("   ERROR: Could not connect to Snowflake")

# Check fabric_sync_data directory
print("\n" + "-" * 70)
print("\n3. Checking Fabric Sync Data Directory...")
fabric_sync_path = os.path.join(os.path.dirname(__file__), "fabric_sync_data")
if os.path.exists(fabric_sync_path):
    files = os.listdir(fabric_sync_path)
    print(f"   Found {len(files)} files in fabric_sync_data/")
    for f in files[:10]:
        filepath = os.path.join(fabric_sync_path, f)
        size = os.path.getsize(filepath)
        print(f"     - {f} ({size} bytes)")
    if len(files) > 10:
        print(f"     ... and {len(files) - 10} more files")
else:
    print("   Directory does not exist")

# Check uploaded_datasets directory
print("\n4. Checking Uploaded Datasets Directory...")
uploaded_path = os.path.join(os.path.dirname(__file__), "uploaded_datasets")
if os.path.exists(uploaded_path):
    files = os.listdir(uploaded_path)
    print(f"   Found {len(files)} files in uploaded_datasets/")
    for f in files[:10]:
        filepath = os.path.join(uploaded_path, f)
        size = os.path.getsize(filepath)
        print(f"     - {f} ({size} bytes)")
    if len(files) > 10:
        print(f"     ... and {len(files) - 10} more files")
else:
    print("   Directory does not exist")

print("\n" + "=" * 70)
print("Diagnostic complete!")
print("=" * 70)
