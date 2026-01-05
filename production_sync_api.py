"""
Production Sync API - REST Endpoints for Bidirectional Data Sync

This module provides the complete REST API for the sync system:
- POST /api/files/upload - Dual-write file upload
- GET /api/files - List all synced files
- GET /api/files/{sync_id}/sync-status - Get sync status
- GET /api/sync/dashboard - Dashboard statistics
- POST /api/sync/{sync_id}/retry - Retry failed sync
- DELETE /api/files/{sync_id} - Delete from both platforms
- POST /api/sync/run - Trigger full sync
- POST /api/sync/migrate - Run historical migration
"""

import os
import io
import sys
import json
import logging
import time
from datetime import datetime
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from dotenv import load_dotenv

# Fix encoding for Windows
if sys.platform == 'win32':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except Exception:
        pass

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import sync components
try:
    from sync_orchestration.sync_engine import SyncOrchestrator, SyncResult
    from sync_orchestration.models import SyncStatus, SyncDirection
except ImportError:
    # Try relative import
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from sync_orchestration.sync_engine import SyncOrchestrator, SyncResult
    from sync_orchestration.models import SyncStatus, SyncDirection

# Import platform connectors
try:
    from fabric_snowflake_sync import FabricApiClient, SnowflakeConnector
except ImportError:
    FabricApiClient = None
    SnowflakeConnector = None
    logger.warning("Could not import fabric_snowflake_sync module")

# Create Flask app
app = Flask(__name__)

# Configure CORS
CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "X-Requested-With"],
        "expose_headers": ["Content-Type"],
        "supports_credentials": False,
        "max_age": 3600
    }
})

# ==============================================================================
# INITIALIZE SYNC ORCHESTRATOR
# ==============================================================================

def get_orchestrator():
    """Get or create the sync orchestrator with connected clients."""
    if not hasattr(app, '_orchestrator'):
        orchestrator = SyncOrchestrator(
            base_path=os.path.dirname(__file__),
            enable_validation=True,
            enable_retry=True
        )
        
        # Initialize Fabric client
        if FabricApiClient:
            try:
                fabric_client = FabricApiClient()
                if fabric_client.authenticate():
                    orchestrator.set_fabric_client(fabric_client)
                    logger.info("Fabric client connected")
                else:
                    logger.warning("Fabric authentication failed")
            except Exception as e:
                logger.error(f"Error initializing Fabric client: {e}")
        
        # Initialize Snowflake connector
        if SnowflakeConnector:
            try:
                sf_connector = SnowflakeConnector()
                orchestrator.set_snowflake_connector(sf_connector)
                logger.info("Snowflake connector initialized")
            except Exception as e:
                logger.error(f"Error initializing Snowflake connector: {e}")
        
        app._orchestrator = orchestrator
    
    return app._orchestrator


# ==============================================================================
# HEALTH & STATUS ENDPOINTS
# ==============================================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    orchestrator = get_orchestrator()
    
    fabric_status = "unknown"
    snowflake_status = "unknown"
    
    # Check Fabric
    if orchestrator.fabric_client:
        try:
            if orchestrator.fabric_client.authenticate():
                fabric_status = "connected"
            else:
                fabric_status = "auth_failed"
        except Exception as e:
            fabric_status = f"error: {str(e)[:50]}"
    
    # Check Snowflake
    if orchestrator.snowflake_connector:
        try:
            if orchestrator.snowflake_connector.connect():
                snowflake_status = "connected"
                orchestrator.snowflake_connector.disconnect()
            else:
                snowflake_status = "connection_failed"
        except Exception as e:
            snowflake_status = f"error: {str(e)[:50]}"
    
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0",
        "fabric": fabric_status,
        "snowflake": snowflake_status
    })


@app.route('/api/sync/dashboard', methods=['GET'])
def get_dashboard():
    """
    Get sync dashboard statistics.
    
    Returns:
        Dashboard data including total syncs, success rate, recent syncs, etc.
    """
    orchestrator = get_orchestrator()
    stats = orchestrator.get_dashboard_stats()
    
    # Add additional metrics
    stats["fabric_connected"] = orchestrator.fabric_client is not None
    stats["snowflake_connected"] = orchestrator.snowflake_connector is not None
    stats["validation_enabled"] = orchestrator.enable_validation
    stats["retry_enabled"] = orchestrator.enable_retry
    
    return jsonify(stats)


# ==============================================================================
# FILE UPLOAD ENDPOINT (DUAL-WRITE)
# ==============================================================================

@app.route('/api/files/upload', methods=['POST'])
def upload_file():
    """
    Upload a file to BOTH Fabric and Snowflake atomically.
    
    Request:
        - multipart/form-data with 'file' field
        OR
        - JSON body with 'file' (base64) and 'filename'
        
    Returns:
        {
            "sync_id": "uuid",
            "success": true,
            "fabric_url": "...",
            "snowflake_url": "...",
            "status": "SYNCED",
            "rows_synced": 100,
            "validation_passed": true,
            "duration_ms": 1500
        }
    """
    orchestrator = get_orchestrator()
    
    try:
        # Handle multipart form data
        if 'file' in request.files:
            file = request.files['file']
            if not file or not file.filename:
                return jsonify({"error": "No file provided"}), 400
            
            filename = file.filename
            file_content = file.read()
            user_id = request.form.get('user_id', 'api_user')
        
        # Handle JSON body with base64 file
        elif request.is_json:
            data = request.get_json()
            if not data.get('file') or not data.get('filename'):
                return jsonify({"error": "Missing 'file' or 'filename' in request"}), 400
            
            import base64
            file_content = base64.b64decode(data['file'])
            filename = data['filename']
            user_id = data.get('user_id', 'api_user')
        
        else:
            return jsonify({"error": "Invalid request format"}), 400
        
        # Validate file size (max 100MB)
        if len(file_content) > 100 * 1024 * 1024:
            return jsonify({"error": "File too large (max 100MB)"}), 400
        
        # Validate file type
        ext = os.path.splitext(filename)[1].lower()
        if ext not in ['.csv', '.json', '.xlsx', '.xls']:
            return jsonify({"error": f"Unsupported file type: {ext}"}), 400
        
        # Perform dual-write
        result = orchestrator.upload_file_to_both(file_content, filename, user_id)
        
        response_data = result.to_dict()
        response_data["status"] = "SYNCED" if result.success else "FAILED"
        
        status_code = 200 if result.success else 500
        return jsonify(response_data), status_code
        
    except Exception as e:
        logger.error(f"Upload error: {e}")
        return jsonify({
            "error": str(e),
            "success": False
        }), 500


# ==============================================================================
# FILE LIST & STATUS ENDPOINTS
# ==============================================================================

@app.route('/api/files', methods=['GET'])
def list_files():
    """
    List all synced files.
    
    Query Parameters:
        - source: Filter by source ('fabric', 'snowflake', 'file_upload', 'all')
        - status: Filter by status ('synced', 'syncing', 'failed', 'all')
        - limit: Max results (default 100)
        
    Returns:
        List of sync manifests with file information
    """
    orchestrator = get_orchestrator()
    
    source = request.args.get('source', 'all')
    status_filter = request.args.get('status', 'all')
    limit = int(request.args.get('limit', 100))
    
    # Get all syncs
    syncs = orchestrator.get_all_syncs(limit=limit)
    
    # Apply filters
    if source != 'all':
        syncs = [s for s in syncs if s.get('source_platform') == source]
    
    if status_filter != 'all':
        status_map = {
            'synced': 'SYNCED',
            'syncing': 'SYNCING',
            'failed': 'FAILED',
            'pending': 'PENDING'
        }
        target_status = status_map.get(status_filter.lower(), status_filter.upper())
        syncs = [s for s in syncs if s.get('status') == target_status]
    
    return jsonify({
        "files": syncs,
        "total": len(syncs),
        "filters": {
            "source": source,
            "status": status_filter
        }
    })


@app.route('/api/files/<sync_id>/sync-status', methods=['GET'])
def get_sync_status(sync_id):
    """
    Get detailed status of a specific sync operation.
    
    Path Parameters:
        - sync_id: UUID of the sync operation
        
    Returns:
        Detailed sync status including validation results, conflicts, etc.
    """
    orchestrator = get_orchestrator()
    
    status = orchestrator.get_sync_status(sync_id)
    
    if not status:
        return jsonify({"error": "Sync ID not found"}), 404
    
    # Add additional details
    status["validation_summary"] = orchestrator.validator.get_validation_summary()
    status["conflicts"] = orchestrator.conflict_resolver.get_conflicts_for_sync(sync_id)
    
    return jsonify(status)


# ==============================================================================
# SYNC CONTROL ENDPOINTS
# ==============================================================================

@app.route('/api/sync/run', methods=['POST'])
def run_sync():
    """
    Trigger a full bidirectional sync.
    
    Request Body (optional):
        {
            "direction": "bidirectional" | "fabric_to_snowflake" | "snowflake_to_fabric",
            "full_sync": true | false
        }
        
    Returns:
        Sync results summary
    """
    orchestrator = get_orchestrator()
    
    data = request.get_json() or {}
    direction = data.get('direction', 'bidirectional')
    full_sync = data.get('full_sync', False)
    
    try:
        if direction == 'fabric_to_snowflake':
            results = orchestrator.sync_fabric_to_snowflake(full_sync=full_sync)
            return jsonify({
                "direction": direction,
                "results": [r.to_dict() for r in results],
                "total": len(results),
                "succeeded": sum(1 for r in results if r.success),
                "failed": sum(1 for r in results if not r.success)
            })
        
        elif direction == 'snowflake_to_fabric':
            results = orchestrator.sync_snowflake_to_fabric(full_sync=full_sync)
            return jsonify({
                "direction": direction,
                "results": [r.to_dict() for r in results],
                "total": len(results),
                "succeeded": sum(1 for r in results if r.success),
                "failed": sum(1 for r in results if not r.success)
            })
        
        else:
            # Full bidirectional
            results = orchestrator.run_full_sync()
            return jsonify(results)
            
    except Exception as e:
        logger.error(f"Sync error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/sync/<sync_id>/retry', methods=['POST'])
def retry_sync(sync_id):
    """
    Retry a failed sync operation.
    
    Path Parameters:
        - sync_id: UUID of the sync operation to retry
        
    Returns:
        Retry result
    """
    orchestrator = get_orchestrator()
    
    try:
        result = orchestrator.retry_failed_sync(sync_id)
        return jsonify(result.to_dict())
    except Exception as e:
        logger.error(f"Retry error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/sync/migrate', methods=['POST'])
def run_migration():
    """
    Run historical data migration.
    
    Request Body (optional):
        {
            "direction": "bidirectional" | "fabric_to_snowflake" | "snowflake_to_fabric"
        }
        
    Returns:
        Migration summary
    """
    orchestrator = get_orchestrator()
    
    data = request.get_json() or {}
    direction_str = data.get('direction', 'bidirectional')
    
    direction_map = {
        'bidirectional': SyncDirection.BIDIRECTIONAL,
        'fabric_to_snowflake': SyncDirection.FABRIC_TO_SNOWFLAKE,
        'snowflake_to_fabric': SyncDirection.SNOWFLAKE_TO_FABRIC
    }
    
    direction = direction_map.get(direction_str, SyncDirection.BIDIRECTIONAL)
    
    try:
        results = orchestrator.migrate_historical_data(direction)
        return jsonify(results)
    except Exception as e:
        logger.error(f"Migration error: {e}")
        return jsonify({"error": str(e)}), 500


# ==============================================================================
# DELETE ENDPOINT
# ==============================================================================

@app.route('/api/files/<sync_id>', methods=['DELETE'])
def delete_file(sync_id):
    """
    Delete a synced file from BOTH platforms (cascading delete).
    
    Path Parameters:
        - sync_id: UUID of the sync operation
        
    Returns:
        Deletion result with actions taken
    """
    orchestrator = get_orchestrator()
    
    try:
        result = orchestrator.rollback_sync(sync_id)
        
        if result.get("success"):
            return jsonify(result)
        else:
            return jsonify(result), 404 if "not found" in result.get("error", "").lower() else 500
            
    except Exception as e:
        logger.error(f"Delete error: {e}")
        return jsonify({"error": str(e)}), 500


# ==============================================================================
# PLATFORM-SPECIFIC ENDPOINTS
# ==============================================================================

@app.route('/api/fabric/models', methods=['GET'])
def get_fabric_models():
    """Get all semantic models from Fabric."""
    orchestrator = get_orchestrator()
    
    try:
        tables = orchestrator.fabric_detector.get_all_tables()
        return jsonify({
            "models": tables,
            "total": len(tables)
        })
    except Exception as e:
        logger.error(f"Fabric models error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/snowflake/tables', methods=['GET'])
def get_snowflake_tables():
    """Get all tables from Snowflake."""
    orchestrator = get_orchestrator()
    
    try:
        tables = orchestrator.snowflake_detector.get_all_tables()
        return jsonify({
            "tables": tables,
            "total": len(tables)
        })
    except Exception as e:
        logger.error(f"Snowflake tables error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/compare', methods=['GET'])
def compare_platforms():
    """Compare data between Fabric and Snowflake."""
    orchestrator = get_orchestrator()
    
    try:
        fabric_tables = orchestrator.fabric_detector.get_all_tables()
        snowflake_tables = orchestrator.snowflake_detector.get_all_tables()
        
        fabric_names = {t.get("table_name", "").upper() for t in fabric_tables}
        snowflake_names = {t.get("table_name", "").upper() for t in snowflake_tables}
        
        return jsonify({
            "fabric_count": len(fabric_tables),
            "snowflake_count": len(snowflake_tables),
            "in_both": list(fabric_names & snowflake_names),
            "only_in_fabric": list(fabric_names - snowflake_names),
            "only_in_snowflake": list(snowflake_names - fabric_names)
        })
    except Exception as e:
        logger.error(f"Compare error: {e}")
        return jsonify({"error": str(e)}), 500


# ==============================================================================
# AUDIT & MONITORING ENDPOINTS
# ==============================================================================

@app.route('/api/audit', methods=['GET'])
def get_audit_trail():
    """
    Get audit trail entries.
    
    Query Parameters:
        - sync_id: Filter by sync ID
        - action: Filter by action type
        - limit: Max results (default 100)
    """
    orchestrator = get_orchestrator()
    
    sync_id = request.args.get('sync_id')
    action = request.args.get('action')
    limit = int(request.args.get('limit', 100))
    
    entries = [a.to_dict() for a in orchestrator.audit_trail[-limit:]]
    
    if sync_id:
        entries = [e for e in entries if e.get('sync_id') == sync_id]
    
    if action:
        entries = [e for e in entries if e.get('action') == action]
    
    return jsonify({
        "entries": entries,
        "total": len(entries)
    })


@app.route('/api/metrics', methods=['GET'])
def get_metrics():
    """
    Get sync metrics for monitoring.
    
    Returns Prometheus-compatible metrics.
    """
    orchestrator = get_orchestrator()
    stats = orchestrator.get_dashboard_stats()
    
    # Format as Prometheus metrics
    metrics = []
    metrics.append(f'sync_total{{status="success"}} {stats.get("synced", 0)}')
    metrics.append(f'sync_total{{status="failed"}} {stats.get("failed", 0)}')
    metrics.append(f'sync_total{{status="pending"}} {stats.get("pending", 0)}')
    metrics.append(f'sync_success_rate {stats.get("success_rate", 0)}')
    metrics.append(f'sync_conflicts_total {stats.get("conflicts_detected", 0)}')
    metrics.append(f'sync_checksum_mismatches_total {stats.get("checksum_mismatches", 0)}')
    metrics.append(f'sync_retry_queue_size {stats.get("retry_queue_size", 0)}')
    
    return Response('\n'.join(metrics), mimetype='text/plain')


@app.route('/api/retry-queue', methods=['GET'])
def get_retry_queue():
    """Get the retry queue status."""
    orchestrator = get_orchestrator()
    
    queue_status = orchestrator.retry_orchestrator.get_queue_status()
    retry_stats = orchestrator.retry_orchestrator.get_retry_stats()
    
    return jsonify({
        "queue": queue_status,
        "stats": retry_stats
    })


@app.route('/api/conflicts', methods=['GET'])
def get_conflicts():
    """Get conflict summary."""
    orchestrator = get_orchestrator()
    
    summary = orchestrator.conflict_resolver.get_conflict_summary()
    return jsonify(summary)


# ==============================================================================
# ERROR HANDLERS
# ==============================================================================

@app.errorhandler(400)
def bad_request(e):
    return jsonify({"error": "Bad request", "message": str(e)}), 400


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found", "message": str(e)}), 404


@app.errorhandler(500)
def internal_error(e):
    return jsonify({"error": "Internal server error", "message": str(e)}), 500


# ==============================================================================
# MAIN
# ==============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    debug = os.environ.get('DEBUG', 'false').lower() == 'true'
    
    logger.info(f"Starting Production Sync API on port {port}")
    logger.info(f"Endpoints available:")
    logger.info(f"  POST /api/files/upload - Dual-write file upload")
    logger.info(f"  GET  /api/files - List all synced files")
    logger.info(f"  GET  /api/files/<sync_id>/sync-status - Get sync status")
    logger.info(f"  GET  /api/sync/dashboard - Dashboard statistics")
    logger.info(f"  POST /api/sync/run - Trigger full sync")
    logger.info(f"  POST /api/sync/<sync_id>/retry - Retry failed sync")
    logger.info(f"  POST /api/sync/migrate - Historical migration")
    logger.info(f"  DELETE /api/files/<sync_id> - Delete from both platforms")
    
    app.run(host='0.0.0.0', port=port, debug=debug, threaded=True)
