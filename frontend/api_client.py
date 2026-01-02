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
    
    def get_snowflake_views(self) -> Dict[str, Any]:
        """Get all views from Snowflake."""
        result = self._request("GET", "/api/snowflake/views")
        if "error" in result:
            return {"success": False, "views": [], "count": 0, "error": result.get("error")}
        return result
    
    def get_snowflake_view_data(self, view_name: str) -> Dict[str, Any]:
        """Get data from a specific Snowflake view."""
        return self._request("GET", f"/api/snowflake/data/{view_name}")
    
    def run_sync(self, direction: str = "bidirectional") -> Dict[str, Any]:
        """Run synchronization between Fabric and Snowflake."""
        return self._request("POST", "/api/sync/run", {"direction": direction})
    
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
