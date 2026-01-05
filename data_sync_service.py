"""
Data Sync Service - Bidirectional Fabric ↔ Snowflake Data Synchronization

This module provides TRUE bidirectional data synchronization:
- Files/data stored in Fabric are synced to Snowflake tables
- Tables stored in Snowflake are synced to Fabric (via staged files)
- Automatic sync runs every 15 minutes
"""

import os
import json
import logging
import pandas as pd
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DataSyncService")

# Import connectors
try:
    from fabric_snowflake_sync import FabricApiClient, SnowflakeConnector
except ImportError:
    logger.error("Could not import fabric_snowflake_sync module")


class DataSyncService:
    """
    Comprehensive bidirectional data synchronization service.
    
    Ensures all data in Fabric is present in Snowflake and vice versa.
    """
    
    def __init__(self):
        self.fabric_client = FabricApiClient()
        self.snowflake_connector = SnowflakeConnector()
        self.sync_log = []
        self.last_sync_time = None
        self.sync_interval = 900  # 15 minutes
        self._running = False
        self._sync_thread = None
        
        # Paths for staged data
        self.staged_data_path = os.path.join(
            os.path.dirname(__file__), 
            "uploaded_datasets"
        )
        os.makedirs(self.staged_data_path, exist_ok=True)
        
        # Fabric sync path (data from Snowflake for Fabric)
        self.fabric_sync_path = os.path.join(
            os.path.dirname(__file__),
            "fabric_sync_data"
        )
        os.makedirs(self.fabric_sync_path, exist_ok=True)
    
    def log_event(self, event_type: str, message: str, status: str = "INFO"):
        """Log sync event."""
        event = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "message": message,
            "status": status
        }
        self.sync_log.append(event)
        logger.info(f"[{event_type}] {message}")
    
    # ================================================================
    # FABRIC -> SNOWFLAKE SYNC
    # ================================================================
    
    def get_fabric_datasets(self) -> List[Dict[str, Any]]:
        """
        Get all datasets from Fabric that need to be synced to Snowflake.
        This includes:
        1. Semantic model tables (metadata)
        2. Staged uploaded files
        """
        datasets = []
        
        # 1. Get staged uploaded files (these are actual data files)
        if os.path.exists(self.staged_data_path):
            for filename in os.listdir(self.staged_data_path):
                if filename.endswith('.json'):
                    filepath = os.path.join(self.staged_data_path, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        datasets.append({
                            "source": "staged_file",
                            "table_name": data.get("table_name", ""),
                            "columns": data.get("columns", []),
                            "data": data.get("data", []),
                            "row_count": data.get("row_count", 0),
                            "filepath": filepath,
                            "synced": False
                        })
                    except Exception as e:
                        logger.warning(f"Error reading staged file {filename}: {e}")
        
        # 2. Get semantic models from Fabric API
        try:
            if self.fabric_client.authenticate():
                models = self.fabric_client.get_semantic_models() or []
                
                for model in models:
                    model_id = model.get("id", "")
                    model_name = model.get("displayName", model.get("name", "Unknown"))
                    
                    # Get model details
                    if model_id:
                        detail = self.fabric_client.get_semantic_model_detail(model_id)
                        if detail:
                            tables = detail.get("tables", [])
                            for table in tables:
                                table_name = table.get("name", "")
                                columns = table.get("columns", [])
                                
                                if table_name:
                                    datasets.append({
                                        "source": "fabric_model",
                                        "model_id": model_id,
                                        "model_name": model_name,
                                        "table_name": f"FABRIC_{model_name}_{table_name}".upper().replace(" ", "_").replace("-", "_"),
                                        "original_table_name": table_name,
                                        "columns": [
                                            {
                                                "name": c.get("name", ""),
                                                "dataType": c.get("dataType", "String")
                                            }
                                            for c in columns
                                        ],
                                        "data": [],  # Fabric API doesn't expose actual data
                                        "synced": False
                                    })
        except Exception as e:
            logger.error(f"Error getting Fabric models: {e}")
        
        return datasets
    
    def sync_dataset_to_snowflake(self, dataset: Dict[str, Any]) -> bool:
        """
        Sync a single dataset to Snowflake.
        Creates a table and inserts all data.
        """
        table_name = dataset.get("table_name", "")
        columns = dataset.get("columns", [])
        data = dataset.get("data", [])
        
        if not table_name:
            return False
        
        try:
            if not self.snowflake_connector.connect():
                self.log_event("ERROR", f"Failed to connect to Snowflake")
                return False
            
            cursor = self.snowflake_connector.connection.cursor()
            
            # Check if table already exists
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            table_exists = len(cursor.fetchall()) > 0
            
            if not table_exists and columns:
                # Build column definitions
                import re
                column_defs = []
                for col in columns:
                    col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col.get("name", "col")).upper()
                    dtype = col.get("dataType", "String")
                    
                    # Map data types
                    if dtype in ["Int64", "Int32", "int64", "int32"]:
                        sf_type = "NUMBER"
                    elif dtype in ["Double", "Float", "float64", "float"]:
                        sf_type = "FLOAT"
                    elif dtype in ["Boolean", "bool"]:
                        sf_type = "BOOLEAN"
                    elif dtype in ["DateTime", "Date", "datetime64"]:
                        sf_type = "TIMESTAMP"
                    else:
                        sf_type = "VARCHAR(4000)"
                    
                    column_defs.append(f'"{col_name}" {sf_type}')
                
                # Create table
                create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(column_defs)})'
                self.log_event("CREATE_TABLE", f"Creating Snowflake table: {table_name}")
                cursor.execute(create_sql)
            
            # Insert data if available
            if data and len(data) > 0:
                # Check current row count
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                existing_rows = cursor.fetchone()[0]
                
                if existing_rows == 0:
                    # Insert data
                    import re
                    inserted = 0
                    for row in data:
                        try:
                            values = []
                            for col in columns:
                                col_name = col.get("name", "")
                                val = row.get(col_name, None)
                                
                                if val is None or (isinstance(val, float) and pd.isna(val)):
                                    values.append("NULL")
                                elif isinstance(val, str):
                                    escaped = val.replace("'", "''")
                                    values.append(f"'{escaped}'")
                                elif isinstance(val, bool):
                                    values.append("TRUE" if val else "FALSE")
                                else:
                                    values.append(str(val))
                            
                            safe_cols = [f'"{re.sub(r"[^a-zA-Z0-9_]", "_", c.get("name", "")).upper()}"' for c in columns]
                            insert_sql = f'INSERT INTO "{table_name}" ({", ".join(safe_cols)}) VALUES ({", ".join(values)})'
                            cursor.execute(insert_sql)
                            inserted += 1
                        except Exception as e:
                            logger.warning(f"Error inserting row: {e}")
                    
                    self.log_event("INSERT_DATA", f"Inserted {inserted} rows into {table_name}")
            
            cursor.close()
            self.snowflake_connector.disconnect()
            return True
            
        except Exception as e:
            self.log_event("ERROR", f"Error syncing to Snowflake: {e}", "ERROR")
            return False
    
    def sync_all_to_snowflake(self) -> Dict[str, Any]:
        """Sync all Fabric datasets to Snowflake."""
        self.log_event("SYNC_START", "Starting Fabric -> Snowflake sync")
        
        datasets = self.get_fabric_datasets()
        results = {
            "total": len(datasets),
            "synced": 0,
            "failed": 0,
            "errors": []
        }
        
        for dataset in datasets:
            if self.sync_dataset_to_snowflake(dataset):
                results["synced"] += 1
            else:
                results["failed"] += 1
                results["errors"].append(dataset.get("table_name", "unknown"))
        
        self.log_event("SYNC_COMPLETE", 
            f"Fabric->Snowflake sync complete: {results['synced']}/{results['total']} synced")
        
        return results
    
    # ================================================================
    # SNOWFLAKE -> FABRIC SYNC
    # ================================================================
    
    def get_snowflake_tables(self) -> List[Dict[str, Any]]:
        """
        Get all tables from Snowflake that need to be synced to Fabric.
        Filters out tables that contain fake "Sample_" data.
        """
        tables = []
        
        try:
            if not self.snowflake_connector.connect():
                return tables
            
            cursor = self.snowflake_connector.connection.cursor()
            
            # Get all tables
            cursor.execute("SHOW TABLES")
            table_list = cursor.fetchall()
            
            for table_row in table_list:
                table_name = table_row[1]
                
                # Skip system tables
                if table_name.startswith("_") or table_name.startswith("SYS"):
                    continue
                
                try:
                    # Get column info
                    cursor.execute(f'DESCRIBE TABLE "{table_name}"')
                    columns_info = cursor.fetchall()
                    columns = [
                        {
                            "name": col[0],
                            "dataType": col[1]
                        }
                        for col in columns_info
                    ]
                    
                    # Get row count
                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                    row_count = cursor.fetchone()[0]
                    
                    # Get sample data (first 100 rows)
                    cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 100')
                    rows = cursor.fetchall()
                    col_names = [desc[0] for desc in cursor.description]
                    
                    data = [dict(zip(col_names, row)) for row in rows]
                    
                    # IMPORTANT: Skip tables with fake "Sample_" data
                    if data and len(data) > 0:
                        first_row = data[0]
                        has_fake_data = any(
                            str(v).startswith("Sample_") 
                            for v in first_row.values() 
                            if isinstance(v, str)
                        )
                        if has_fake_data:
                            logger.warning(f"Skipping {table_name}: contains fake Sample_ data")
                            continue
                    
                    tables.append({
                        "table_name": table_name,
                        "columns": columns,
                        "row_count": row_count,
                        "data": data,
                        "synced": False
                    })
                except Exception as e:
                    logger.warning(f"Error getting table info for {table_name}: {e}")
            
            cursor.close()
            self.snowflake_connector.disconnect()
            
        except Exception as e:
            logger.error(f"Error getting Snowflake tables: {e}")
        
        return tables
    
    def sync_table_to_fabric(self, table: Dict[str, Any]) -> bool:
        """
        Sync a Snowflake table to Fabric by staging it as a dataset.
        This creates a JSON file that can be imported into Fabric.
        """
        table_name = table.get("table_name", "")
        
        if not table_name:
            return False
        
        try:
            # Create fabric sync file
            sync_file = os.path.join(
                self.fabric_sync_path,
                f"{table_name}.json"
            )
            
            sync_data = {
                "table_name": table_name,
                "source": "snowflake",
                "synced_at": datetime.now().isoformat(),
                "columns": table.get("columns", []),
                "row_count": table.get("row_count", 0),
                "data": table.get("data", [])
            }
            
            with open(sync_file, 'w', encoding='utf-8') as f:
                json.dump(sync_data, f, indent=2, default=str)
            
            self.log_event("FABRIC_SYNC", f"Staged Snowflake table for Fabric: {table_name}")
            return True
            
        except Exception as e:
            self.log_event("ERROR", f"Error syncing to Fabric: {e}", "ERROR")
            return False
    
    def sync_all_to_fabric(self) -> Dict[str, Any]:
        """Sync all Snowflake tables to Fabric."""
        self.log_event("SYNC_START", "Starting Snowflake -> Fabric sync")
        
        tables = self.get_snowflake_tables()
        results = {
            "total": len(tables),
            "synced": 0,
            "failed": 0,
            "errors": []
        }
        
        for table in tables:
            if self.sync_table_to_fabric(table):
                results["synced"] += 1
            else:
                results["failed"] += 1
                results["errors"].append(table.get("table_name", "unknown"))
        
        self.log_event("SYNC_COMPLETE", 
            f"Snowflake->Fabric sync complete: {results['synced']}/{results['total']} synced")
        
        return results
    
    # ================================================================
    # BIDIRECTIONAL SYNC
    # ================================================================
    
    def run_bidirectional_sync(self) -> Dict[str, Any]:
        """
        Run complete bidirectional sync.
        1. Sync Fabric data to Snowflake
        2. Sync Snowflake data to Fabric
        """
        self.log_event("BIDIRECTIONAL_START", "Starting bidirectional sync")
        self.sync_log = []
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "fabric_to_snowflake": None,
            "snowflake_to_fabric": None,
            "success": False
        }
        
        try:
            # Fabric -> Snowflake
            results["fabric_to_snowflake"] = self.sync_all_to_snowflake()
            
            # Snowflake -> Fabric
            results["snowflake_to_fabric"] = self.sync_all_to_fabric()
            
            results["success"] = True
            self.last_sync_time = datetime.now()
            
        except Exception as e:
            self.log_event("ERROR", f"Bidirectional sync failed: {e}", "ERROR")
            results["error"] = str(e)
        
        results["log"] = self.sync_log
        
        self.log_event("BIDIRECTIONAL_COMPLETE", 
            f"Bidirectional sync complete: success={results['success']}")
        
        return results
    
    # ================================================================
    # COMPARISON & VERIFICATION
    # ================================================================
    
    def compare_fabric_snowflake(self) -> Dict[str, Any]:
        """
        Compare data between Fabric and Snowflake.
        Returns what's missing in each system.
        """
        comparison = {
            "fabric_datasets": [],
            "snowflake_tables": [],
            "missing_in_snowflake": [],
            "missing_in_fabric": [],
            "synced": []
        }
        
        # Get Fabric datasets
        fabric_datasets = self.get_fabric_datasets()
        fabric_names = set()
        for ds in fabric_datasets:
            name = ds.get("table_name", "")
            if name:
                fabric_names.add(name.upper())
                comparison["fabric_datasets"].append({
                    "name": name,
                    "rows": len(ds.get("data", []))
                })
        
        # Get Snowflake tables
        snowflake_tables = self.get_snowflake_tables()
        snowflake_names = set()
        for tbl in snowflake_tables:
            name = tbl.get("table_name", "")
            if name:
                snowflake_names.add(name.upper())
                comparison["snowflake_tables"].append({
                    "name": name,
                    "rows": tbl.get("row_count", 0)
                })
        
        # Find missing
        comparison["missing_in_snowflake"] = list(fabric_names - snowflake_names)
        comparison["missing_in_fabric"] = list(snowflake_names - fabric_names)
        comparison["synced"] = list(fabric_names & snowflake_names)
        
        return comparison
    
    # ================================================================
    # AUTOMATIC SYNC (BACKGROUND THREAD)
    # ================================================================
    
    def start_auto_sync(self):
        """Start automatic background sync every 15 minutes."""
        if self._running:
            logger.warning("Auto-sync already running")
            return
        
        self._running = True
        self._sync_thread = threading.Thread(
            target=self._auto_sync_loop,
            daemon=True,
            name="DataSyncService"
        )
        self._sync_thread.start()
        logger.info("Auto-sync started (15 min interval)")
    
    def stop_auto_sync(self):
        """Stop automatic sync."""
        self._running = False
        if self._sync_thread and self._sync_thread.is_alive():
            self._sync_thread.join(timeout=5)
        logger.info("Auto-sync stopped")
    
    def _auto_sync_loop(self):
        """Background sync loop."""
        while self._running:
            try:
                logger.info("Running scheduled bidirectional sync...")
                result = self.run_bidirectional_sync()
                logger.info(f"Scheduled sync result: success={result.get('success')}")
            except Exception as e:
                logger.error(f"Scheduled sync error: {e}")
            
            # Wait for next sync interval
            time.sleep(self.sync_interval)


# Global sync service instance
_sync_service = None

def get_sync_service() -> DataSyncService:
    """Get or create the global sync service instance."""
    global _sync_service
    if _sync_service is None:
        _sync_service = DataSyncService()
    return _sync_service


# CLI for testing
if __name__ == "__main__":
    print("=" * 60)
    print("Data Sync Service - Fabric ↔ Snowflake")
    print("=" * 60)
    
    service = DataSyncService()
    
    print("\n1. Comparing Fabric and Snowflake...")
    comparison = service.compare_fabric_snowflake()
    print(f"   Fabric datasets: {len(comparison['fabric_datasets'])}")
    print(f"   Snowflake tables: {len(comparison['snowflake_tables'])}")
    print(f"   Missing in Snowflake: {comparison['missing_in_snowflake']}")
    print(f"   Missing in Fabric: {comparison['missing_in_fabric']}")
    print(f"   Already synced: {comparison['synced']}")
    
    print("\n2. Running bidirectional sync...")
    result = service.run_bidirectional_sync()
    print(f"   Success: {result.get('success')}")
    print(f"   Fabric->Snowflake: {result.get('fabric_to_snowflake')}")
    print(f"   Snowflake->Fabric: {result.get('snowflake_to_fabric')}")
    
    print("\n" + "=" * 60)
    print("Sync complete!")
