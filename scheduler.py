"""
Scheduler Module - Continuous Real-Time Synchronization

This module provides the "heartbeat" scheduler for automatic sync operations.
The system was previously static, requiring manual intervention. This module
enables continuous, non-blocking synchronization.

Key Features:
- Configurable sync interval (default: 60 seconds)
- Non-blocking execution using threading
- Graceful shutdown handling (KeyboardInterrupt, SIGTERM)
- Integration with change detection to skip unnecessary syncs
- Comprehensive error handling with retry logic
- Health monitoring and status reporting
"""

import os
import sys
import time
import signal
import threading
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager

# Local imports
try:
    from logging_audit import get_audit_logger, EventType, Severity
    from naming_convention import NamingConvention
except ImportError:
    # Fallback for standalone testing
    pass

logger = logging.getLogger(__name__)


class SchedulerState(Enum):
    """Scheduler operational states."""
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPING = "STOPPING"
    ERROR = "ERROR"


@dataclass
class SchedulerStats:
    """Statistics for scheduler operations."""
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    skipped_runs: int = 0  # Skipped due to no changes
    
    last_run_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    last_failure_time: Optional[datetime] = None
    last_run_duration_ms: Optional[float] = None
    
    total_views_created: int = 0
    total_views_failed: int = 0
    total_changes_detected: int = 0
    
    # Timing
    started_at: Optional[datetime] = None
    uptime_seconds: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "failed_runs": self.failed_runs,
            "skipped_runs": self.skipped_runs,
            "success_rate": (
                (self.successful_runs / self.total_runs * 100) 
                if self.total_runs > 0 else 0
            ),
            "last_run_time": self.last_run_time.isoformat() if self.last_run_time else None,
            "last_success_time": self.last_success_time.isoformat() if self.last_success_time else None,
            "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None,
            "last_run_duration_ms": self.last_run_duration_ms,
            "total_views_created": self.total_views_created,
            "total_views_failed": self.total_views_failed,
            "total_changes_detected": self.total_changes_detected,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "uptime_seconds": self.uptime_seconds,
        }


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    # Exponential backoff: wait 10s, then 20s, then 40s
    initial_delay_seconds: float = 10.0
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 300.0  # 5 minutes cap


class ExponentialBackoff:
    """
    Implements exponential backoff with configurable delays.
    
    The requirement specifies: 10s, 20s, 40s delays between retries.
    """
    
    def __init__(self, config: RetryConfig = None):
        """Initialize with retry configuration."""
        self.config = config or RetryConfig()
        self.current_attempt = 0
        self.last_delay = 0.0
    
    def reset(self) -> None:
        """Reset the backoff state."""
        self.current_attempt = 0
        self.last_delay = 0.0
    
    def get_next_delay(self) -> float:
        """
        Get the next delay for exponential backoff.
        
        Returns:
            Delay in seconds (10s, 20s, 40s, ...)
        """
        if self.current_attempt == 0:
            delay = self.config.initial_delay_seconds
        else:
            delay = self.last_delay * self.config.backoff_multiplier
        
        # Cap at maximum delay
        delay = min(delay, self.config.max_delay_seconds)
        
        self.last_delay = delay
        self.current_attempt += 1
        
        return delay
    
    def should_retry(self) -> bool:
        """Check if we should attempt another retry."""
        return self.current_attempt < self.config.max_retries
    
    def get_attempt_number(self) -> int:
        """Get current attempt number (1-indexed)."""
        return self.current_attempt + 1


class SyncScheduler:
    """
    Production-grade scheduler for continuous sync operations.
    
    Features:
    - Configurable sync interval
    - Non-blocking execution
    - Graceful shutdown
    - Integration with change detection
    - Retry logic with exponential backoff
    - Partial failure handling
    """
    
    def __init__(
        self,
        sync_function: Callable[[], tuple],
        change_detector: Optional[Any] = None,
        interval_seconds: int = 60,
        retry_config: Optional[RetryConfig] = None,
        auto_start: bool = False
    ):
        """
        Initialize the scheduler.
        
        Args:
            sync_function: The sync function to call. Should return (success_count, failure_count).
            change_detector: Optional ChangeDetector instance for smart sync.
            interval_seconds: Interval between sync runs in seconds.
            retry_config: Configuration for retry behavior.
            auto_start: If True, start the scheduler immediately.
        """
        self.sync_function = sync_function
        self.change_detector = change_detector
        self.interval_seconds = interval_seconds
        self.retry_config = retry_config or RetryConfig()
        
        # State tracking
        self._state = SchedulerState.STOPPED
        self._state_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()  # Not paused by default
        
        # Thread management
        self._scheduler_thread: Optional[threading.Thread] = None
        
        # Statistics
        self.stats = SchedulerStats()
        
        # Backoff for retry
        self.backoff = ExponentialBackoff(self.retry_config)
        
        # Last metadata hash for change detection
        self._last_metadata_hash: Optional[str] = None
        
        # Audit logger integration
        try:
            self._audit_logger = get_audit_logger()
        except Exception:
            self._audit_logger = None
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        if auto_start:
            self.start()
    
    @property
    def state(self) -> SchedulerState:
        """Get current scheduler state."""
        with self._state_lock:
            return self._state
    
    @state.setter
    def state(self, value: SchedulerState) -> None:
        """Set scheduler state."""
        with self._state_lock:
            self._state = value
    
    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self.state == SchedulerState.RUNNING
    
    @property
    def is_paused(self) -> bool:
        """Check if scheduler is paused."""
        return self.state == SchedulerState.PAUSED
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        # Only setup if we're in the main thread
        if threading.current_thread() is threading.main_thread():
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
            except (ValueError, OSError):
                # Signals can only be set in main thread
                pass
    
    def _signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop()
    
    def start(self) -> bool:
        """
        Start the scheduler.
        
        Returns:
            True if started successfully, False if already running.
        """
        if self.is_running:
            logger.warning("Scheduler is already running")
            return False
        
        if self.state == SchedulerState.STARTING:
            logger.warning("Scheduler is already starting")
            return False
        
        self.state = SchedulerState.STARTING
        self._stop_event.clear()
        
        # Reset statistics for new run
        self.stats.started_at = datetime.now()
        self.stats.total_runs = 0
        
        # Start scheduler thread
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            name="SyncScheduler",
            daemon=True  # Thread will exit when main program exits
        )
        self._scheduler_thread.start()
        
        self.state = SchedulerState.RUNNING
        
        logger.info(f"🚀 Scheduler started with {self.interval_seconds}s interval")
        
        if self._audit_logger:
            self._audit_logger.log(
                EventType.SCHEDULER_START,
                f"Scheduler started with {self.interval_seconds}s interval",
                Severity.INFO,
                context={"interval_seconds": self.interval_seconds}
            )
        
        return True
    
    def stop(self, timeout: float = 30.0) -> bool:
        """
        Stop the scheduler gracefully.
        
        Args:
            timeout: Maximum time to wait for scheduler to stop.
            
        Returns:
            True if stopped successfully, False if timeout.
        """
        if self.state == SchedulerState.STOPPED:
            return True
        
        logger.info("🛑 Stopping scheduler...")
        self.state = SchedulerState.STOPPING
        
        # Signal the scheduler thread to stop
        self._stop_event.set()
        
        # Unpause if paused so the thread can exit
        self._pause_event.set()
        
        # Wait for thread to finish
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=timeout)
            
            if self._scheduler_thread.is_alive():
                logger.warning("Scheduler thread did not stop gracefully")
                return False
        
        self.state = SchedulerState.STOPPED
        self.stats.uptime_seconds = (
            (datetime.now() - self.stats.started_at).total_seconds()
            if self.stats.started_at else 0
        )
        
        logger.info("✅ Scheduler stopped successfully")
        
        if self._audit_logger:
            self._audit_logger.log(
                EventType.SCHEDULER_STOP,
                f"Scheduler stopped after {self.stats.total_runs} runs",
                Severity.INFO,
                context=self.stats.to_dict()
            )
        
        return True
    
    def pause(self) -> None:
        """Pause the scheduler."""
        if self.is_running:
            self._pause_event.clear()
            self.state = SchedulerState.PAUSED
            logger.info("⏸️ Scheduler paused")
    
    def resume(self) -> None:
        """Resume the scheduler."""
        if self.is_paused:
            self._pause_event.set()
            self.state = SchedulerState.RUNNING
            logger.info("▶️ Scheduler resumed")
    
    def trigger_sync_now(self) -> Optional[tuple]:
        """
        Trigger an immediate sync (bypassing the schedule).
        
        Returns:
            Sync result tuple (success_count, failure_count) or None on error.
        """
        logger.info("🔄 Triggering immediate sync...")
        return self._execute_sync(force=True)
    
    def _scheduler_loop(self) -> None:
        """
        Main scheduler loop.
        
        Runs in a separate thread and executes syncs at the configured interval.
        """
        logger.info("Scheduler loop started")
        
        while not self._stop_event.is_set():
            # Wait for pause to be cleared
            self._pause_event.wait()
            
            if self._stop_event.is_set():
                break
            
            # Log tick
            if self._audit_logger:
                self._audit_logger.log(
                    EventType.SCHEDULER_TICK,
                    "Scheduler tick",
                    Severity.DEBUG
                )
            
            # Execute sync
            try:
                self._execute_sync()
            except Exception as e:
                logger.error(f"Unexpected error in scheduler loop: {e}")
                self.stats.failed_runs += 1
            
            # Wait for next interval or stop signal
            # Use small sleep intervals to be responsive to stop signal
            wait_time = self.interval_seconds
            while wait_time > 0 and not self._stop_event.is_set():
                sleep_time = min(1.0, wait_time)
                time.sleep(sleep_time)
                wait_time -= sleep_time
        
        logger.info("Scheduler loop exited")
    
    def _execute_sync(self, force: bool = False) -> Optional[tuple]:
        """
        Execute a single sync operation.
        
        Args:
            force: If True, skip change detection and always sync.
            
        Returns:
            Tuple of (success_count, failure_count) or None on error.
        """
        sync_id = str(uuid.uuid4())[:8]
        start_time = time.time()
        
        self.stats.total_runs += 1
        self.stats.last_run_time = datetime.now()
        
        # Check for changes if change detector is available
        if not force and self.change_detector:
            try:
                has_changes, metadata_hash = self._check_for_changes()
                
                if not has_changes:
                    logger.debug("No changes detected, skipping sync")
                    self.stats.skipped_runs += 1
                    return (0, 0)
                
                self._last_metadata_hash = metadata_hash
                self.stats.total_changes_detected += 1
                
            except Exception as e:
                logger.warning(f"Change detection failed, proceeding with sync: {e}")
        
        # Execute sync with retry logic
        result = self._execute_with_retry(sync_id)
        
        # Update statistics
        duration_ms = (time.time() - start_time) * 1000
        self.stats.last_run_duration_ms = duration_ms
        
        if result:
            success_count, failure_count = result
            self.stats.total_views_created += success_count
            self.stats.total_views_failed += failure_count
            
            if failure_count == 0:
                self.stats.successful_runs += 1
                self.stats.last_success_time = datetime.now()
            else:
                # Partial failure - still counts as a run
                if success_count > 0:
                    self.stats.successful_runs += 1
                else:
                    self.stats.failed_runs += 1
                    self.stats.last_failure_time = datetime.now()
        else:
            self.stats.failed_runs += 1
            self.stats.last_failure_time = datetime.now()
        
        return result
    
    def _check_for_changes(self) -> tuple:
        """
        Check if there are any changes that require sync.
        
        Returns:
            Tuple of (has_changes, metadata_hash).
        """
        # This is a simplified implementation
        # In production, this would compare current Fabric metadata hash
        # against the last successful sync state
        
        if hasattr(self.change_detector, 'compute_current_hash'):
            current_hash = self.change_detector.compute_current_hash()
            has_changes = current_hash != self._last_metadata_hash
            return has_changes, current_hash
        
        # Default: assume changes exist
        return True, None
    
    def _execute_with_retry(self, sync_id: str) -> Optional[tuple]:
        """
        Execute sync with retry logic and exponential backoff.
        
        Args:
            sync_id: Unique identifier for this sync operation.
            
        Returns:
            Sync result or None if all retries exhausted.
        """
        self.backoff.reset()
        last_error = None
        
        while True:
            attempt = self.backoff.get_attempt_number()
            
            try:
                logger.info(f"[Sync {sync_id}] Executing sync (attempt {attempt})")
                result = self.sync_function()
                
                # Reset backoff on success
                self.backoff.reset()
                return result
                
            except Exception as e:
                last_error = e
                logger.error(f"[Sync {sync_id}] Sync failed: {e}")
                
                if self.backoff.should_retry():
                    delay = self.backoff.get_next_delay()
                    
                    logger.warning(
                        f"[Sync {sync_id}] Retrying in {delay:.1f}s "
                        f"(attempt {self.backoff.get_attempt_number()}/{self.retry_config.max_retries})"
                    )
                    
                    if self._audit_logger:
                        self._audit_logger.retry(
                            f"Sync {sync_id} failed",
                            attempt=self.backoff.get_attempt_number(),
                            max_retries=self.retry_config.max_retries,
                            delay_seconds=delay,
                            error=e
                        )
                    
                    # Wait before retry
                    time.sleep(delay)
                else:
                    # Max retries exhausted
                    logger.error(
                        f"[Sync {sync_id}] Max retries ({self.retry_config.max_retries}) exhausted"
                    )
                    
                    if self._audit_logger:
                        self._audit_logger.log(
                            EventType.RETRY_EXHAUSTED,
                            f"Sync {sync_id} failed after {self.retry_config.max_retries} retries",
                            Severity.ERROR,
                            error=last_error
                        )
                    
                    return None
    
    def set_interval(self, interval_seconds: int) -> None:
        """
        Update the sync interval.
        
        Args:
            interval_seconds: New interval in seconds.
        """
        old_interval = self.interval_seconds
        self.interval_seconds = interval_seconds
        logger.info(f"Sync interval changed from {old_interval}s to {interval_seconds}s")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current scheduler status."""
        return {
            "state": self.state.value,
            "is_running": self.is_running,
            "is_paused": self.is_paused,
            "interval_seconds": self.interval_seconds,
            "stats": self.stats.to_dict(),
            "retry_config": {
                "max_retries": self.retry_config.max_retries,
                "initial_delay_seconds": self.retry_config.initial_delay_seconds,
                "backoff_multiplier": self.retry_config.backoff_multiplier,
            }
        }
    
    def get_health(self) -> Dict[str, Any]:
        """Get scheduler health status."""
        now = datetime.now()
        
        # Check if last run was recent enough
        last_run_age = None
        if self.stats.last_run_time:
            last_run_age = (now - self.stats.last_run_time).total_seconds()
        
        # Health assessment
        is_healthy = True
        issues = []
        
        if self.state == SchedulerState.ERROR:
            is_healthy = False
            issues.append("Scheduler in ERROR state")
        
        if last_run_age and last_run_age > self.interval_seconds * 3:
            is_healthy = False
            issues.append(f"Last run was {last_run_age:.0f}s ago (expected {self.interval_seconds}s)")
        
        if self.stats.total_runs > 0:
            failure_rate = self.stats.failed_runs / self.stats.total_runs
            if failure_rate > 0.5:
                is_healthy = False
                issues.append(f"High failure rate: {failure_rate*100:.1f}%")
        
        return {
            "healthy": is_healthy,
            "issues": issues,
            "state": self.state.value,
            "last_run_age_seconds": last_run_age,
            "failure_rate": (
                self.stats.failed_runs / self.stats.total_runs 
                if self.stats.total_runs > 0 else 0
            )
        }


class PartialSyncResult:
    """
    Container for sync results that handles partial failures.
    
    This ensures that if 1 out of 5 models fails, we don't stop the entire process.
    """
    
    def __init__(self):
        """Initialize the result container."""
        self.successes: List[Dict[str, Any]] = []
        self.failures: List[Dict[str, Any]] = []
        self.start_time = time.time()
        self.end_time: Optional[float] = None
    
    def add_success(
        self, 
        model_name: str, 
        table_name: str, 
        view_name: str,
        duration_ms: Optional[float] = None
    ) -> None:
        """Record a successful sync."""
        self.successes.append({
            "model_name": model_name,
            "table_name": table_name,
            "view_name": view_name,
            "duration_ms": duration_ms,
            "timestamp": datetime.now().isoformat()
        })
    
    def add_failure(
        self, 
        model_name: str, 
        table_name: str, 
        error: Exception,
        view_name: Optional[str] = None
    ) -> None:
        """Record a failed sync."""
        self.failures.append({
            "model_name": model_name,
            "table_name": table_name,
            "view_name": view_name,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now().isoformat()
        })
    
    def finalize(self) -> None:
        """Mark the sync as complete."""
        self.end_time = time.time()
    
    @property
    def success_count(self) -> int:
        """Get count of successful syncs."""
        return len(self.successes)
    
    @property
    def failure_count(self) -> int:
        """Get count of failed syncs."""
        return len(self.failures)
    
    @property
    def total_count(self) -> int:
        """Get total sync attempts."""
        return self.success_count + self.failure_count
    
    @property
    def duration_ms(self) -> float:
        """Get total duration in milliseconds."""
        end = self.end_time or time.time()
        return (end - self.start_time) * 1000
    
    @property
    def is_complete_success(self) -> bool:
        """Check if all syncs succeeded."""
        return self.failure_count == 0 and self.success_count > 0
    
    @property
    def is_partial_success(self) -> bool:
        """Check if some syncs succeeded."""
        return self.success_count > 0 and self.failure_count > 0
    
    @property
    def is_complete_failure(self) -> bool:
        """Check if all syncs failed."""
        return self.success_count == 0 and self.failure_count > 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "total_count": self.total_count,
            "duration_ms": self.duration_ms,
            "is_complete_success": self.is_complete_success,
            "is_partial_success": self.is_partial_success,
            "is_complete_failure": self.is_complete_failure,
            "successes": self.successes,
            "failures": self.failures,
        }
    
    def get_result_tuple(self) -> tuple:
        """Get result as (success_count, failure_count) tuple."""
        return (self.success_count, self.failure_count)
    
    def log_summary(self, logger_instance: Optional[logging.Logger] = None) -> None:
        """Log a summary of the sync results."""
        log = logger_instance or logger
        
        log.info(
            f"Sync completed: {self.success_count}/{self.total_count} successful "
            f"({self.duration_ms:.0f}ms)"
        )
        
        if self.failures:
            for failure in self.failures:
                log.error(
                    f"Failed: [{failure['model_name']}].{failure['table_name']} - "
                    f"{failure['error_type']}: {failure['error_message']}"
                )


def create_scheduler(
    sync_function: Callable[[], tuple],
    interval_seconds: int = 60,
    change_detector: Optional[Any] = None,
    max_retries: int = 3,
    initial_retry_delay: float = 10.0
) -> SyncScheduler:
    """
    Factory function to create a configured scheduler.
    
    Args:
        sync_function: The sync function to call.
        interval_seconds: Interval between syncs.
        change_detector: Optional change detector.
        max_retries: Maximum retry attempts.
        initial_retry_delay: Initial delay between retries (10s per requirement).
        
    Returns:
        Configured SyncScheduler instance.
    """
    retry_config = RetryConfig(
        max_retries=max_retries,
        initial_delay_seconds=initial_retry_delay,
        backoff_multiplier=2.0,  # 10s, 20s, 40s as required
    )
    
    return SyncScheduler(
        sync_function=sync_function,
        change_detector=change_detector,
        interval_seconds=interval_seconds,
        retry_config=retry_config
    )


# Entry point for running scheduler standalone
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run the sync scheduler")
    parser.add_argument("--interval", type=int, default=60, help="Sync interval in seconds")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retry attempts")
    args = parser.parse_args()
    
    # Demo sync function
    def demo_sync():
        print(f"[{datetime.now()}] Demo sync executed")
        return (1, 0)  # 1 success, 0 failures
    
    scheduler = create_scheduler(
        sync_function=demo_sync,
        interval_seconds=args.interval,
        max_retries=args.max_retries
    )
    
    print(f"Starting scheduler with {args.interval}s interval...")
    print("Press Ctrl+C to stop")
    
    scheduler.start()
    
    try:
        # Keep main thread alive
        while scheduler.is_running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        scheduler.stop()
        print("Scheduler stopped")
