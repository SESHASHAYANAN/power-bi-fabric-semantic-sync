"""
API Client for Backend Communication
Connects to Flask Backend API for real-time sync operations
"""
import requests
import os
from typing import Any, Dict


class BackendAPIClient:
    """Client for communicating with Flask backend API on port 5000."""
    
    def __init__(self, base_url: str = None):
        self.base_url = base_url or os.getenv("BACKEND_URL", "http://localhost:5000")
        self.timeout = 15
    
    def _request(self, method: str, endpoint: str, data: dict = None, files: dict = None) -> Dict[str, Any]:
        """Make HTTP request to backend."""
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, timeout=self.timeout)
            elif method.upper() == "POST":
                if files:
                    response = requests.post(url, files=files, timeout=self.timeout)
                else:
                    response = requests.post(url, json=data, timeout=self.timeout)
            else:
                return {"error": f"Unsupported method: {method}"}
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError:
            return {"error": "Backend not reachable", "connected": False}
        except requests.exceptions.Timeout:
            return {"error": "Request timeout"}
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}
        except Exception as e:
            return {"error": str(e)}
    
    def get_health(self) -> Dict[str, Any]:
        """Get health check status."""
        return self._request("GET", "/api/health")
    
    def test_connections(self) -> Dict[str, Any]:
        """Test connections to Fabric and Snowflake."""
        result = self._request("GET", "/api/connections/test")
        if "error" in result:
            return {
                "fabric": {"connected": False, "message": result.get("error", "")},
                "snowflake": {"connected": False, "message": result.get("error", "")}
            }
        return result
    
    def get_fabric_models(self) -> Dict[str, Any]:
        """Get all semantic models from Fabric."""
        result = self._request("GET", "/api/fabric/models")
        if "error" in result:
            return {"success": False, "models": [], "count": 0, "error": result.get("error")}
        return result
    
    def get_fabric_model_data(self, model_id: str, table_name: str = None, limit: int = 100) -> Dict[str, Any]:
        """Get actual table data from a Fabric semantic model.
        
        Args:
            model_id: The semantic model ID
            table_name: Optional specific table to get data from
            limit: Maximum rows to return (default 100, max 1000)
            
        Returns:
            Dictionary with table data and row counts
        """
        params = f"?limit={limit}"
        if table_name:
            params += f"&table={table_name}"
        return self._request("GET", f"/api/fabric/model-data/{model_id}{params}")
    
    def get_snowflake_views(self) -> Dict[str, Any]:
        """Get all views from Snowflake."""
        result = self._request("GET", "/api/snowflake/views")
        if "error" in result:
            return {"success": False, "views": [], "count": 0, "error": result.get("error")}
        return result
    
    def get_snowflake_view_data(self, view_name: str) -> Dict[str, Any]:
        """Get data from a specific Snowflake view."""
        return self._request("GET", f"/api/snowflake/data/{view_name}")
    
    def run_sync(self, direction: str = "bidirectional", force: bool = False, sync_data: bool = True) -> Dict[str, Any]:
        """Run synchronization between Fabric and Snowflake.
        
        Args:
            direction: Sync direction (fabric_to_snowflake, snowflake_to_fabric, bidirectional)
            force: If True, bypass change detection and recreate all views
            sync_data: If True (default), extract and load actual row-level data to Snowflake
        """
        return self._request("POST", "/api/sync/run", {
            "direction": direction, 
            "force": force,
            "sync_data": sync_data
        })
    
    def reconcile_sync(self) -> Dict[str, Any]:
        """Run reconciliation sync - detect and create only missing views.
        
        This is useful when sync state is corrupted or views are missing.
        """
        return self._request("POST", "/api/sync/reconcile")
    
    def reset_sync_state(self) -> Dict[str, Any]:
        """Reset all sync state files.
        
        Clears sync_state.json and other state files to start fresh.
        """
        return self._request("POST", "/api/sync/reset-state")
    
    def load_data(self, force: bool = True, sync_mode: str = "full_refresh") -> Dict[str, Any]:
        """Load actual row-level data from Fabric into Snowflake tables.
        
        This is the CRITICAL method that populates Snowflake tables with actual business data.
        Uses DAX queries to extract data from Fabric and INSERT/MERGE to load into Snowflake.
        
        Args:
            force: If True, reload all data even if tables exist
            sync_mode: "full_refresh" (replace all), "incremental" (merge), or "append"
            
        Returns:
            Dictionary with rows extracted, rows loaded, and any errors.
        """
        return self._request("POST", "/api/sync/load-data", {
            "force": force,
            "sync_mode": sync_mode
        })
    
    def populate_tables(self, force: bool = True) -> Dict[str, Any]:
        """Populate Snowflake tables with sample data.
        
        This is a fallback when DAX queries fail to extract data from Fabric.
        It creates sample rows based on the table schema.
        
        Args:
            force: If True, replace existing data with sample data
            
        Returns:
            Dictionary with tables populated and row counts.
        """
        return self._request("POST", "/api/sync/populate-tables", {"force": force})
    
    def detect_changes(self) -> Dict[str, Any]:
        """Detect changes between Fabric and Snowflake."""
        result = self._request("GET", "/api/changes/detect")
        if "error" in result:
            return {"success": False, "snapshots": [], "error": result.get("error")}
        return result
    
    def upload_test_file(self, file) -> Dict[str, Any]:
        """Upload a test file for sync validation."""
        try:
            files = {"file": (file.name, file.getvalue(), file.type or "application/octet-stream")}
            return self._request("POST", "/api/test-file", files=files)
        except Exception as e:
            return {"error": str(e)}
    
    # ============================================
    # TRUE BIDIRECTIONAL DATA SYNC METHODS
    # ============================================
    
    def compare_data(self) -> Dict[str, Any]:
        """Compare data between Fabric and Snowflake."""
        return self._request("GET", "/api/data-sync/compare")
    
    def run_full_data_sync(self) -> Dict[str, Any]:
        """Run TRUE bidirectional data sync."""
        return self._request("POST", "/api/data-sync/run")
    
    def sync_fabric_to_snowflake(self) -> Dict[str, Any]:
        """Sync all Fabric data to Snowflake."""
        return self._request("POST", "/api/data-sync/fabric-to-snowflake")
    
    def sync_snowflake_to_fabric(self) -> Dict[str, Any]:
        """Sync all Snowflake data to Fabric."""
        return self._request("POST", "/api/data-sync/snowflake-to-fabric")
    
    def get_auto_sync_status(self) -> Dict[str, Any]:
        """Get auto-sync status."""
        return self._request("GET", "/api/data-sync/auto-sync")
    
    def set_auto_sync(self, enable: bool) -> Dict[str, Any]:
        """Enable or disable auto-sync."""
        return self._request("POST", "/api/data-sync/auto-sync", data={"enable": enable})
    
    # ============================================
    # REAL-TIME SYNC METHODS
    # ============================================
    
    def get_realtime_sync_status(self) -> Dict[str, Any]:
        """Get real-time sync status."""
        return self._request("GET", "/api/realtime-sync/status")
    
    def start_realtime_sync(self, interval: int = 60) -> Dict[str, Any]:
        """Start real-time sync with file watching."""
        return self._request("POST", "/api/realtime-sync/start", data={"interval": interval})
    
    def stop_realtime_sync(self) -> Dict[str, Any]:
        """Stop real-time sync."""
        return self._request("POST", "/api/realtime-sync/stop")
    
    def run_realtime_sync_now(self) -> Dict[str, Any]:
        """Run a full bidirectional sync immediately."""
        return self._request("POST", "/api/realtime-sync/run-now")
    
    def compare_systems(self) -> Dict[str, Any]:
        """Compare data between Fabric and Snowflake."""
        return self._request("GET", "/api/realtime-sync/compare")
    
    def set_sync_interval(self, interval: int) -> Dict[str, Any]:
        """Set the sync interval in seconds."""
        return self._request("POST", "/api/realtime-sync/set-interval", data={"interval": interval})
    
    def sync_file_realtime(self, file) -> Dict[str, Any]:
        """Sync a single file to both systems in real-time."""
        try:
            files = {"file": (file.name, file.getvalue(), file.type or "application/octet-stream")}
            return self._request("POST", "/api/realtime-sync/sync-file", files=files)
        except Exception as e:
            return {"error": str(e)}
    
    def realtime_fabric_to_snowflake(self) -> Dict[str, Any]:
        """Sync all Fabric data to Snowflake using real-time service."""
        return self._request("POST", "/api/realtime-sync/fabric-to-snowflake")
    
    def realtime_snowflake_to_fabric(self) -> Dict[str, Any]:
        """Sync all Snowflake data to Fabric using real-time service."""
        return self._request("POST", "/api/realtime-sync/snowflake-to-fabric")
    
    def get_sync_logs(self, limit: int = 50) -> Dict[str, Any]:
        """Get recent sync logs."""
        return self._request("GET", f"/api/realtime-sync/logs?limit={limit}")

