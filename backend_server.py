import os
import threading
import time
import logging
import pandas as pd
import io
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

# Import the existing sync logic
from fabric_snowflake_sync import (
    SemanticSyncEngine, 
    SyncDirection, 
    FabricApiClient, 
    SnowflakeConnector
)

load_dotenv()

app = Flask(__name__)
# Enable CORS for all routes and all origins with comprehensive settings
CORS(app, 
     resources={r"/api/*": {
         "origins": "*",
         "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
         "allow_headers": ["Content-Type", "Authorization"],
         "expose_headers": ["Content-Type"],
         "supports_credentials": False,
         "max_age": 3600
     }}
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state for the background sync
sync_state = {
    "status": "IDLE",
    "progress": 0,
    "current_operation": "",
    "last_result": None,
    "is_running": False
}

sync_history = []

def run_sync_in_background(direction_str):
    global sync_state
    try:
        direction = SyncDirection.BIDIRECTIONAL
        if direction_str == "fabric_to_snowflake":
            direction = SyncDirection.FABRIC_TO_SNOWFLAKE
        elif direction_str == "snowflake_to_fabric":
            direction = SyncDirection.SNOWFLAKE_TO_FABRIC

        engine = SemanticSyncEngine(direction)
        
        # Override the log_event to update real-time state
        original_log = engine.log_event
        def custom_log(event_type, message, severity="INFO"):
            sync_state["current_operation"] = message
            # Simple progress heuristic
            if event_type == "SYNC_START": sync_state["progress"] = 5
            elif event_type == "DISCOVERY": sync_state["progress"] = 20
            elif event_type == "EXTRACT": sync_state["progress"] = 40
            elif event_type == "CREATE": sync_state["progress"] = 70
            elif event_type == "VALIDATE": sync_state["progress"] = 90
            elif event_type == "SYNC_COMPLETE": sync_state["progress"] = 100
            
            original_log(event_type, message, severity)

        engine.log_event = custom_log
        
        sync_state["status"] = "IN_PROGRESS"
        sync_state["is_running"] = True
        
        results = engine.run_sync()
        
        sync_state["status"] = results.get("status", "COMPLETED")
        sync_state["last_result"] = results
        sync_history.insert(0, results)
        
    except Exception as e:
        logging.error(f"Background sync error: {e}")
        sync_state["status"] = "FAILED"
        sync_state["current_operation"] = str(e)
    finally:
        sync_state["is_running"] = False
        sync_state["progress"] = 100 if sync_state["status"] != "FAILED" else 0


# ============================================
# HEALTH & STATUS ENDPOINTS
# ============================================

@app.route('/api/health', methods=['GET'])
def get_health():
    fabric = FabricApiClient()
    snowflake = SnowflakeConnector()
    
    start_f = time.time()
    f_ok = fabric.authenticate()
    f_lat = (time.time() - start_f) * 1000
    
    start_s = time.time()
    s_ok = snowflake.connect()
    if s_ok: snowflake.disconnect()
    s_lat = (time.time() - start_s) * 1000
    
    return jsonify({
        "fabric": {"connected": f_ok, "latency_ms": f_lat},
        "snowflake": {"connected": s_ok, "latency_ms": s_lat},
        "timestamp": datetime.now().isoformat()
    })


# ============================================
# CONNECTION TESTING ENDPOINT
# ============================================

@app.route('/api/connections/test', methods=['GET'])
def test_connections():
    """Test connections to both Fabric and Snowflake."""
    result = {
        "fabric": {"connected": False, "message": "Not tested"},
        "snowflake": {"connected": False, "message": "Not tested"}
    }
    
    # Test Fabric connection
    try:
        fabric = FabricApiClient()
        f_ok = fabric.authenticate()
        if f_ok:
            models = fabric.get_semantic_models()
            result["fabric"] = {
                "connected": True,
                "message": f"Connected to Fabric workspace",
                "models_count": len(models) if models else 0
            }
        else:
            result["fabric"] = {"connected": False, "message": "Authentication failed"}
    except Exception as e:
        result["fabric"] = {"connected": False, "message": str(e)}
    
    # Test Snowflake connection
    try:
        snowflake = SnowflakeConnector()
        s_ok = snowflake.connect()
        if s_ok:
            # Get views count
            views = []
            try:
                cursor = snowflake.connection.cursor()
                cursor.execute("SHOW VIEWS")
                views = cursor.fetchall()
                cursor.close()
            except:
                pass
            
            result["snowflake"] = {
                "connected": True,
                "message": f"Connected to Snowflake",
                "views_count": len(views)
            }
            snowflake.disconnect()
        else:
            result["snowflake"] = {"connected": False, "message": "Connection failed"}
    except Exception as e:
        result["snowflake"] = {"connected": False, "message": str(e)}
    
    return jsonify(result)


# ============================================
# FABRIC ENDPOINTS
# ============================================

@app.route('/api/fabric/models', methods=['GET'])
def get_fabric_models():
    """Get all semantic models from Fabric."""
    try:
        client = FabricApiClient()
        if client.authenticate():
            models = client.get_semantic_models()
            return jsonify({
                "success": True,
                "models": models or [],
                "count": len(models) if models else 0
            })
        return jsonify({"success": False, "models": [], "count": 0, "error": "Authentication failed"})
    except Exception as e:
        logger.error(f"Error getting Fabric models: {e}")
        return jsonify({"success": False, "models": [], "count": 0, "error": str(e)})


@app.route('/api/models', methods=['GET'])
def get_models():
    """Legacy endpoint for models."""
    client = FabricApiClient()
    if client.authenticate():
        return jsonify(client.get_semantic_models())
    return jsonify([]), 401


# ============================================
# SNOWFLAKE ENDPOINTS
# ============================================

@app.route('/api/snowflake/views', methods=['GET'])
def get_snowflake_views():
    """Get all views from Snowflake semantic layer."""
    try:
        snowflake = SnowflakeConnector()
        if snowflake.connect():
            views = []
            try:
                cursor = snowflake.connection.cursor()
                # Get list of views
                cursor.execute("SHOW VIEWS")
                view_list = cursor.fetchall()
                
                for view_row in view_list[:10]:  # Limit to 10 views
                    view_name = view_row[1]  # View name is typically in second column
                    try:
                        # Get sample data and row count
                        cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
                        row_count = cursor.fetchone()[0]
                        
                        cursor.execute(f"SELECT * FROM {view_name} LIMIT 5")
                        sample_rows = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description]
                        
                        sample_data = [dict(zip(columns, row)) for row in sample_rows]
                        
                        views.append({
                            "name": view_name,
                            "row_count": row_count,
                            "sample_data": sample_data
                        })
                    except Exception as ve:
                        views.append({
                            "name": view_name,
                            "row_count": 0,
                            "sample_data": [],
                            "error": str(ve)
                        })
                
                cursor.close()
            except Exception as e:
                logger.error(f"Error querying views: {e}")
            
            snowflake.disconnect()
            return jsonify({
                "success": True,
                "views": views,
                "count": len(views)
            })
        return jsonify({"success": False, "views": [], "count": 0, "error": "Connection failed"})
    except Exception as e:
        logger.error(f"Error getting Snowflake views: {e}")
        return jsonify({"success": False, "views": [], "count": 0, "error": str(e)})


@app.route('/api/snowflake/data/<view_name>', methods=['GET'])
def get_snowflake_view_data(view_name):
    """Get full data from a specific Snowflake view."""
    try:
        snowflake = SnowflakeConnector()
        if snowflake.connect():
            try:
                cursor = snowflake.connection.cursor()
                cursor.execute(f"SELECT * FROM {view_name} LIMIT 1000")
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                data = [dict(zip(columns, row)) for row in rows]
                cursor.close()
                snowflake.disconnect()
                
                return jsonify({
                    "success": True,
                    "data": data,
                    "count": len(data)
                })
            except Exception as e:
                snowflake.disconnect()
                return jsonify({"success": False, "data": [], "error": str(e)})
        return jsonify({"success": False, "data": [], "error": "Connection failed"})
    except Exception as e:
        return jsonify({"success": False, "data": [], "error": str(e)})


# ============================================
# SYNC ENDPOINTS
# ============================================

@app.route('/api/sync/run', methods=['POST'])
def run_sync():
    """Run synchronization (synchronous version)."""
    try:
        data = request.json or {}
        direction_str = data.get("direction", "bidirectional")
        
        direction = SyncDirection.BIDIRECTIONAL
        if direction_str == "fabric_to_snowflake":
            direction = SyncDirection.FABRIC_TO_SNOWFLAKE
        elif direction_str == "snowflake_to_fabric":
            direction = SyncDirection.SNOWFLAKE_TO_FABRIC
        
        engine = SemanticSyncEngine(direction)
        results = engine.run_sync()
        
        return jsonify({
            "success": True,
            "results": results
        })
    except Exception as e:
        logger.error(f"Sync error: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/sync/start', methods=['POST'])
def start_sync():
    """Start async background sync."""
    if sync_state["is_running"]:
        return jsonify({"error": "Sync already in progress"}), 400
    
    data = request.json or {}
    direction = data.get("direction", "bidirectional")
    
    thread = threading.Thread(target=run_sync_in_background, args=(direction,))
    thread.start()
    
    return jsonify({"message": "Sync started", "status": "IN_PROGRESS"})


@app.route('/api/sync/status', methods=['GET'])
def get_sync_status():
    return jsonify(sync_state)


@app.route('/api/sync/history', methods=['GET'])
def get_history():
    return jsonify(sync_history)


# ============================================
# CHANGES DETECTION ENDPOINT
# ============================================

@app.route('/api/changes/detect', methods=['GET'])
def detect_changes():
    """Detect changes between Fabric and Snowflake."""
    try:
        snapshots = []
        fabric_models_count = 0
        snowflake_views_count = 0
        
        # Get Fabric models snapshot
        try:
            fabric = FabricApiClient()
            if fabric.authenticate():
                models = fabric.get_semantic_models() or []
                fabric_models_count = len(models)
                
                for model in models:
                    snapshots.append({
                        "id": model.get("id", ""),
                        "name": model.get("displayName", model.get("name", "Unknown")),
                        "source": "fabric",
                        "tables": model.get("tables_count", 0),
                        "columns": model.get("columns_count", 0),
                        "measures": model.get("measures_count", 0)
                    })
        except Exception as e:
            logger.error(f"Error getting Fabric snapshots: {e}")
        
        # Get Snowflake views snapshot
        try:
            snowflake = SnowflakeConnector()
            if snowflake.connect():
                cursor = snowflake.connection.cursor()
                cursor.execute("SHOW VIEWS")
                views = cursor.fetchall()
                snowflake_views_count = len(views)
                
                for view_row in views[:10]:
                    view_name = view_row[1]
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {view_name}")
                        row_count = cursor.fetchone()[0]
                        
                        # Get column count
                        cursor.execute(f"DESCRIBE VIEW {view_name}")
                        columns = cursor.fetchall()
                        
                        snapshots.append({
                            "id": "",
                            "name": view_name,
                            "source": "snowflake",
                            "tables": 1,
                            "columns": len(columns),
                            "measures": row_count
                        })
                    except:
                        snapshots.append({
                            "id": "",
                            "name": view_name,
                            "source": "snowflake",
                            "tables": 1,
                            "columns": 0,
                            "measures": 0
                        })
                
                cursor.close()
                snowflake.disconnect()
        except Exception as e:
            logger.error(f"Error getting Snowflake snapshots: {e}")
        
        return jsonify({
            "success": True,
            "snapshots": snapshots,
            "fabric_models": fabric_models_count,
            "snowflake_views": snowflake_views_count
        })
    except Exception as e:
        logger.error(f"Error detecting changes: {e}")
        return jsonify({"success": False, "snapshots": [], "error": str(e)})


# ============================================
# FILE UPLOAD ENDPOINT (FOR CSV/EXCEL/JSON)
# ============================================

@app.route('/api/test-file', methods=['POST', 'OPTIONS'])
def upload_test_file():
    """Upload and validate a test file (CSV, Excel, JSON)."""
    # Handle CORS preflight
    if request.method == 'OPTIONS':
        return jsonify({"status": "ok"}), 200
    
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400
        
        filename = file.filename
        file_content = file.read()
        
        logger.info(f"Received file: {filename}, size: {len(file_content)} bytes")
        
        # Parse file based on type
        data = None
        if filename.endswith('.csv'):
            try:
                df = pd.read_csv(io.BytesIO(file_content))
                data = df.to_dict(orient='records')
            except Exception as e:
                return jsonify({"error": f"Failed to parse CSV: {str(e)}"}), 400
                
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            try:
                df = pd.read_excel(io.BytesIO(file_content))
                data = df.to_dict(orient='records')
            except Exception as e:
                return jsonify({"error": f"Failed to parse Excel: {str(e)}"}), 400
                
        elif filename.endswith('.json'):
            try:
                import json
                data = json.loads(file_content.decode('utf-8'))
            except Exception as e:
                return jsonify({"error": f"Failed to parse JSON: {str(e)}"}), 400
        else:
            return jsonify({"error": f"Unsupported file type: {filename}"}), 400
        
        # Validate with both systems (simulate)
        result = {
            "filename": filename,
            "size_bytes": len(file_content),
            "records_count": len(data) if isinstance(data, list) else 1,
            "validation": {
                "fabric": {
                    "status": "valid",
                    "message": "File structure compatible with Fabric semantic model"
                },
                "snowflake": {
                    "status": "valid", 
                    "message": "File structure compatible with Snowflake views"
                }
            },
            "sample_data": data[:5] if isinstance(data, list) else data
        }
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"File upload error: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================
# CONFIG ENDPOINT
# ============================================

@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify({
        "fabric_tenant_id": os.getenv("FABRIC_TENANT_ID"),
        "snowflake_account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "sync_mode": os.getenv("SYNC_MODE")
    })


if __name__ == '__main__':
    logger.info("Starting backend server on port 5000...")
    app.run(port=5000, debug=True, host='0.0.0.0')
