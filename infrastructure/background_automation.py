"""
Background Automation System for Fabric-Snowflake Sync

This module provides a unified interface for managing the background automation
system, coordinating between Snowflake Tasks and Azure Functions.

Author: Data Engineering Team
Created: 2026-01-03
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import threading
import time

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("BackgroundAutomation")
logger.setLevel(logging.INFO)


class SyncDirection(Enum):
    """Sync direction options."""
    FABRIC_TO_SNOWFLAKE = "fabric_to_snowflake"
    SNOWFLAKE_TO_FABRIC = "snowflake_to_fabric"
    BIDIRECTIONAL = "bidirectional"


class SyncStatus(Enum):
    """Sync operation status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"
    SKIPPED = "skipped"


@dataclass
class SyncResult:
    """Result of a sync operation."""
    sync_id: str
    status: SyncStatus
    direction: SyncDirection
    start_time: datetime
    end_time: Optional[datetime] = None
    models_processed: int = 0
    views_created: int = 0
    views_updated: int = 0
    measures_synced: int = 0
    errors: List[str] = field(default_factory=list)
    duration_ms: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "sync_id": self.sync_id,
            "status": self.status.value,
            "direction": self.direction.value,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "models_processed": self.models_processed,
            "views_created": self.views_created,
            "views_updated": self.views_updated,
            "measures_synced": self.measures_synced,
            "errors": self.errors,
            "duration_ms": self.duration_ms
        }


@dataclass
class AlertConfig:
    """Alert configuration."""
    slack_webhook_url: str = ""
    email_recipients: List[str] = field(default_factory=list)
    teams_webhook_url: str = ""
    
    # Thresholds
    duration_threshold_seconds: int = 10
    success_rate_threshold: float = 0.90
    drift_threshold: float = 0.10
    consecutive_failure_threshold: int = 3


class RedisStateManager:
    """Distributed state management with Redis as shared cache."""
    
    def __init__(self):
        self.redis_client = None
        self._connect()
    
    def _connect(self):
        """Establish Redis connection."""
        try:
            import redis
            
            host = os.getenv("REDIS_HOST", "")
            port = int(os.getenv("REDIS_PORT", "6380"))
            password = os.getenv("REDIS_PASSWORD", "")
            ssl = os.getenv("REDIS_SSL", "true").lower() == "true"
            
            if host:
                self.redis_client = redis.Redis(
                    host=host,
                    port=port,
                    password=password,
                    ssl=ssl,
                    decode_responses=True
                )
                self.redis_client.ping()
                logger.info("Connected to Redis for state management")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}. Using local state.")
            self.redis_client = None
    
    def acquire_lock(self, lock_name: str, ttl_seconds: int = 1800) -> Tuple[bool, str]:
        """
        Acquire a distributed lock.
        
        Returns:
            Tuple of (lock_acquired, lock_token)
        """
        if not self.redis_client:
            return True, "local"
        
        lock_key = f"sync_lock:{lock_name}"
        lock_token = hashlib.md5(
            f"{os.getpid()}:{datetime.now().isoformat()}".encode()
        ).hexdigest()
        
        result = self.redis_client.set(
            lock_key, 
            lock_token, 
            nx=True,  # Only set if not exists
            ex=ttl_seconds
        )
        
        return result is not None, lock_token
    
    def release_lock(self, lock_name: str, lock_token: str) -> bool:
        """Release a distributed lock."""
        if not self.redis_client:
            return True
        
        lock_key = f"sync_lock:{lock_name}"
        
        # Only release if we own the lock
        current_token = self.redis_client.get(lock_key)
        if current_token == lock_token:
            self.redis_client.delete(lock_key)
            return True
        return False
    
    def get_state(self, state_key: str) -> Dict[str, Any]:
        """Get state from Redis."""
        if not self.redis_client:
            return {}
        
        data = self.redis_client.get(f"sync_state:{state_key}")
        return json.loads(data) if data else {}
    
    def set_state(self, state_key: str, state: Dict[str, Any], ttl: int = 86400):
        """Set state in Redis."""
        if self.redis_client:
            state["updated_at"] = datetime.now(timezone.utc).isoformat()
            self.redis_client.set(
                f"sync_state:{state_key}",
                json.dumps(state),
                ex=ttl
            )
    
    def record_sync_hash(self, model_name: str, content_hash: str):
        """Record model content hash for change detection."""
        if self.redis_client:
            self.redis_client.set(
                f"model_hash:{model_name}",
                content_hash,
                ex=86400 * 7  # 7 days
            )
    
    def get_sync_hash(self, model_name: str) -> Optional[str]:
        """Get previous model content hash."""
        if self.redis_client:
            return self.redis_client.get(f"model_hash:{model_name}")
        return None
    
    def check_duplicate_operation(self, operation_id: str) -> bool:
        """Check if an operation is a duplicate (already processed)."""
        if not self.redis_client:
            return False
        
        key = f"operation:{operation_id}"
        exists = self.redis_client.exists(key)
        
        if not exists:
            # Mark as processed
            self.redis_client.set(key, "1", ex=3600)  # 1 hour dedup window
        
        return exists


class NotificationManager:
    """Manages notifications across channels."""
    
    def __init__(self, config: AlertConfig):
        self.config = config
    
    def send_slack(self, message: str, is_error: bool = False) -> bool:
        """Send Slack notification."""
        if not self.config.slack_webhook_url:
            return False
        
        try:
            payload = {
                "text": f"{'🚨 ' if is_error else '✅ '}{message}",
                "username": "Fabric-Snowflake Sync",
                "icon_emoji": ":warning:" if is_error else ":white_check_mark:"
            }
            
            response = requests.post(
                self.config.slack_webhook_url,
                json=payload,
                timeout=10
            )
            return response.status_code == 200
            
        except Exception as e:
            logger.warning(f"Slack notification failed: {e}")
            return False
    
    def send_teams(self, message: str, is_error: bool = False) -> bool:
        """Send Microsoft Teams notification."""
        if not self.config.teams_webhook_url:
            return False
        
        try:
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "FF0000" if is_error else "00FF00",
                "summary": "Sync Notification",
                "sections": [{
                    "activityTitle": "Fabric-Snowflake Sync",
                    "text": message
                }]
            }
            
            response = requests.post(
                self.config.teams_webhook_url,
                json=payload,
                timeout=10
            )
            return response.status_code == 200
            
        except Exception as e:
            logger.warning(f"Teams notification failed: {e}")
            return False
    
    def send_all(self, message: str, is_error: bool = False):
        """Send notification to all configured channels."""
        self.send_slack(message, is_error)
        self.send_teams(message, is_error)


class HealthChecker:
    """Health check orchestrator."""
    
    def __init__(self, state_manager: RedisStateManager):
        self.state_manager = state_manager
    
    def check_all(self) -> Dict[str, Any]:
        """Perform comprehensive health check."""
        health_report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_status": "HEALTHY",
            "components": [],
            "warnings": [],
            "errors": []
        }
        
        # Check sync states
        for sync_type in ["BIDIRECTIONAL", "FABRIC_TO_SNOWFLAKE", "SNOWFLAKE_TO_FABRIC"]:
            state = self.state_manager.get_state(sync_type)
            
            component = {
                "name": sync_type,
                "status": "OK",
                "last_success": state.get("completed_at"),
                "consecutive_failures": state.get("consecutive_failures", 0)
            }
            
            # Check for issues
            if component["consecutive_failures"] >= 3:
                component["status"] = "CRITICAL"
                health_report["errors"].append(
                    f"{sync_type} has {component['consecutive_failures']} consecutive failures"
                )
                health_report["overall_status"] = "CRITICAL"
            elif component["consecutive_failures"] >= 1:
                component["status"] = "WARNING"
                health_report["warnings"].append(
                    f"{sync_type} has recent failures"
                )
                if health_report["overall_status"] == "HEALTHY":
                    health_report["overall_status"] = "WARNING"
            
            # Check staleness
            if component["last_success"]:
                last_success = datetime.fromisoformat(
                    component["last_success"].replace("Z", "+00:00")
                )
                minutes_ago = (
                    datetime.now(timezone.utc) - last_success
                ).total_seconds() / 60
                
                if minutes_ago > 120:  # More than 2 hours
                    health_report["warnings"].append(
                        f"{sync_type} has not succeeded in {int(minutes_ago)} minutes"
                    )
            
            health_report["components"].append(component)
        
        return health_report


class BackgroundScheduler:
    """Background scheduler for local development/testing."""
    
    def __init__(self, 
                 sync_interval_hours: float = 1.0,
                 health_check_interval_minutes: int = 15):
        
        self.sync_interval = sync_interval_hours * 3600
        self.health_interval = health_check_interval_minutes * 60
        self.running = False
        self.sync_thread: Optional[threading.Thread] = None
        self.health_thread: Optional[threading.Thread] = None
        
        self.state_manager = RedisStateManager()
        self.health_checker = HealthChecker(self.state_manager)
        
        # Load alert config
        self.alert_config = AlertConfig(
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
            email_recipients=os.getenv("ALERT_EMAIL_RECIPIENTS", "").split(",")
        )
        self.notification_manager = NotificationManager(self.alert_config)
    
    def start(self):
        """Start the background scheduler."""
        if self.running:
            logger.warning("Scheduler already running")
            return
        
        self.running = True
        
        # Start sync thread
        self.sync_thread = threading.Thread(
            target=self._sync_loop,
            daemon=True,
            name="SyncScheduler"
        )
        self.sync_thread.start()
        
        # Start health check thread
        self.health_thread = threading.Thread(
            target=self._health_loop,
            daemon=True,
            name="HealthChecker"
        )
        self.health_thread.start()
        
        logger.info("Background scheduler started")
    
    def stop(self):
        """Stop the background scheduler."""
        self.running = False
        
        if self.sync_thread and self.sync_thread.is_alive():
            self.sync_thread.join(timeout=5)
        
        if self.health_thread and self.health_thread.is_alive():
            self.health_thread.join(timeout=5)
        
        logger.info("Background scheduler stopped")
    
    def _sync_loop(self):
        """Main sync loop."""
        while self.running:
            try:
                self._execute_sync()
            except Exception as e:
                logger.exception(f"Sync loop error: {e}")
            
            # Sleep for the interval
            time.sleep(self.sync_interval)
    
    def _health_loop(self):
        """Health check loop."""
        while self.running:
            try:
                health_report = self.health_checker.check_all()
                
                if health_report["overall_status"] == "CRITICAL":
                    self.notification_manager.send_all(
                        f"⚠️ CRITICAL: {'; '.join(health_report['errors'])}",
                        is_error=True
                    )
                
                logger.info(f"Health check: {health_report['overall_status']}")
                
            except Exception as e:
                logger.exception(f"Health check error: {e}")
            
            time.sleep(self.health_interval)
    
    def _execute_sync(self):
        """Execute a sync operation."""
        sync_id = f"LOCAL_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Check for duplicate
        if self.state_manager.check_duplicate_operation(sync_id):
            logger.info(f"Skipping duplicate operation: {sync_id}")
            return
        
        # Acquire lock
        lock_acquired, lock_token = self.state_manager.acquire_lock(
            "bidirectional_sync"
        )
        
        if not lock_acquired:
            logger.info("Another sync is in progress, skipping")
            return
        
        try:
            logger.info(f"Starting sync: {sync_id}")
            
            # Import and use the main sync engine
            from fabric_snowflake_sync import SemanticSyncEngine, SyncDirection as FSSyncDirection
            
            engine = SemanticSyncEngine(FSSyncDirection.BIDIRECTIONAL)
            
            # Extract and sync
            models = engine.extract_fabric_models()
            successful, failed = engine.sync_to_snowflake(models)
            
            # Update state
            status = "COMPLETED" if failed == 0 else ("PARTIAL" if successful > 0 else "FAILED")
            
            self.state_manager.set_state("BIDIRECTIONAL", {
                "sync_id": sync_id,
                "status": status,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "models_processed": len(models),
                "successful": successful,
                "failed": failed,
                "consecutive_failures": 0 if status == "COMPLETED" else 
                    self.state_manager.get_state("BIDIRECTIONAL").get("consecutive_failures", 0) + 1
            })
            
            logger.info(f"Sync completed: {status} ({successful} successful, {failed} failed)")
            
        except Exception as e:
            logger.exception(f"Sync failed: {e}")
            
            # Update failure state
            current_state = self.state_manager.get_state("BIDIRECTIONAL")
            self.state_manager.set_state("BIDIRECTIONAL", {
                "sync_id": sync_id,
                "status": "FAILED",
                "error": str(e),
                "consecutive_failures": current_state.get("consecutive_failures", 0) + 1
            })
            
            self.notification_manager.send_all(
                f"Sync {sync_id} FAILED: {str(e)}",
                is_error=True
            )
            
        finally:
            self.state_manager.release_lock("bidirectional_sync", lock_token)


class AzureFunctionTrigger:
    """Trigger Azure Function for sync operations."""
    
    def __init__(self):
        self.function_url = os.getenv(
            "AZURE_FUNCTION_URL",
            "https://your-function-app.azurewebsites.net/api"
        )
        self.function_key = os.getenv("AZURE_FUNCTION_KEY", "")
    
    def trigger_sync(self, direction: str = "BIDIRECTIONAL", 
                     force_full: bool = False) -> Dict[str, Any]:
        """Trigger Azure Function for sync."""
        try:
            url = f"{self.function_url}/fabric-snowflake-sync"
            
            headers = {}
            if self.function_key:
                headers["x-functions-key"] = self.function_key
            
            payload = {
                "direction": direction,
                "force_full_sync": force_full
            }
            
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=300
            )
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to trigger Azure Function: {e}")
            return {"error": str(e)}
    
    def check_health(self) -> Dict[str, Any]:
        """Check Azure Function health."""
        try:
            url = f"{self.function_url}/health"
            
            headers = {}
            if self.function_key:
                headers["x-functions-key"] = self.function_key
            
            response = requests.get(url, headers=headers, timeout=10)
            
            return response.json()
            
        except Exception as e:
            return {"status": "unreachable", "error": str(e)}


def create_background_automation():
    """Factory function to create background automation system."""
    
    # Load configuration
    sync_interval = float(os.getenv("SYNC_INTERVAL_HOURS", "1"))
    health_interval = int(os.getenv("HEALTH_CHECK_INTERVAL_MINUTES", "15"))
    
    scheduler = BackgroundScheduler(
        sync_interval_hours=sync_interval,
        health_check_interval_minutes=health_interval
    )
    
    return scheduler


# Main entry point for running as a standalone service
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    print("=" * 60)
    print("Fabric-Snowflake Background Automation System")
    print("=" * 60)
    
    scheduler = create_background_automation()
    
    try:
        scheduler.start()
        
        print("\nBackground automation running. Press Ctrl+C to stop.\n")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
        scheduler.stop()
        print("Shutdown complete.")
