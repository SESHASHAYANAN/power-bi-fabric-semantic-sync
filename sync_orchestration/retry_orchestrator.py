"""
Retry Orchestrator - Exponential Backoff with Jitter

Handles transient failures with automatic retry using battle-tested patterns:
- Exponential backoff (1s, 2s, 4s, 8s... max 5min)
- Jitter to prevent thundering herd
- Classification of transient vs permanent errors
- Retry queue management
"""

import random
import time
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from functools import wraps

from .models import SyncFailureQueue, ErrorType, SyncStatus

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 5
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 300.0  # 5 minutes
    exponential_base: float = 2.0
    jitter_factor: float = 0.1  # 10% jitter
    
    # Error classification for retry decisions
    retryable_errors: List[str] = field(default_factory=lambda: [
        "ConnectionError",
        "TimeoutError",
        "TemporaryError",
        "ServiceUnavailable",
        "RateLimitExceeded",
        "NetworkError",
        "TransientError"
    ])
    
    non_retryable_errors: List[str] = field(default_factory=lambda: [
        "AuthenticationError",
        "PermissionDenied",
        "ValidationError",
        "DataCorruptionError",
        "SchemaError",
        "PermanentError"
    ])


class RetryableError(Exception):
    """Exception that should trigger a retry."""
    pass


class PermanentError(Exception):
    """Exception that should NOT trigger a retry."""
    pass


class RetryOrchestrator:
    """
    Production-grade retry orchestrator with exponential backoff.
    
    Features:
    - Automatic retry for transient failures
    - Exponential backoff with jitter
    - Error classification
    - Retry queue management
    - Comprehensive logging
    """
    
    def __init__(self, config: RetryConfig = None):
        """
        Initialize retry orchestrator.
        
        Args:
            config: Retry configuration (uses defaults if not provided)
        """
        self.config = config or RetryConfig()
        self.failure_queue: List[SyncFailureQueue] = []
        self.retry_history: List[Dict] = []
    
    # ==================================================================
    # RETRY DECORATOR
    # ==================================================================
    
    def with_retry(self, 
                   max_retries: int = None,
                   on_retry: Callable = None,
                   on_failure: Callable = None):
        """
        Decorator that adds retry logic to any function.
        
        Args:
            max_retries: Override max retries from config
            on_retry: Callback called before each retry (attempt, delay, error)
            on_failure: Callback called on final failure (error)
            
        Usage:
            @retry_orchestrator.with_retry(max_retries=3)
            def sync_data():
                ...
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return self.execute_with_retry(
                    func, args, kwargs,
                    max_retries=max_retries,
                    on_retry=on_retry,
                    on_failure=on_failure
                )
            return wrapper
        return decorator
    
    # ==================================================================
    # CORE RETRY LOGIC
    # ==================================================================
    
    def execute_with_retry(self,
                           func: Callable,
                           args: tuple = None,
                           kwargs: dict = None,
                           max_retries: int = None,
                           on_retry: Callable = None,
                           on_failure: Callable = None) -> Any:
        """
        Execute a function with retry logic.
        
        Args:
            func: Function to execute
            args: Positional arguments
            kwargs: Keyword arguments
            max_retries: Maximum retry attempts
            on_retry: Pre-retry callback
            on_failure: Final failure callback
            
        Returns:
            Function result on success
            
        Raises:
            PermanentError: If max retries exceeded or non-retryable error
        """
        args = args or ()
        kwargs = kwargs or {}
        retries = max_retries if max_retries is not None else self.config.max_retries
        
        last_error = None
        
        for attempt in range(1, retries + 2):  # +2 because first is attempt 1, not a retry
            try:
                result = func(*args, **kwargs)
                
                # Success - log if this was a retry
                if attempt > 1:
                    logger.info(f"Function {func.__name__} succeeded on attempt {attempt}")
                    self._log_retry_success(func.__name__, attempt)
                
                return result
                
            except Exception as e:
                last_error = e
                error_type = self._classify_error(e)
                
                # Check if we should retry
                if error_type == ErrorType.TRANSIENT and attempt <= retries:
                    delay = self._calculate_delay(attempt)
                    
                    logger.warning(
                        f"Transient error in {func.__name__} (attempt {attempt}/{retries}): {e}. "
                        f"Retrying in {delay:.2f}s"
                    )
                    
                    # Call retry callback if provided
                    if on_retry:
                        try:
                            on_retry(attempt, delay, e)
                        except Exception as callback_err:
                            logger.warning(f"Retry callback error: {callback_err}")
                    
                    # Log retry attempt
                    self._log_retry_attempt(func.__name__, attempt, delay, str(e))
                    
                    # Wait before retry
                    time.sleep(delay)
                    continue
                
                else:
                    # Non-retryable or max retries exceeded
                    logger.error(
                        f"{'Permanent' if error_type != ErrorType.TRANSIENT else 'Max retries exceeded'} "
                        f"error in {func.__name__}: {e}"
                    )
                    
                    # Call failure callback if provided
                    if on_failure:
                        try:
                            on_failure(e)
                        except Exception as callback_err:
                            logger.warning(f"Failure callback error: {callback_err}")
                    
                    self._log_retry_failure(func.__name__, attempt, str(e), error_type)
                    
                    raise PermanentError(
                        f"Failed after {attempt} attempts: {e}"
                    ) from e
        
        # Should not reach here, but just in case
        raise PermanentError(f"Unexpected retry loop exit: {last_error}")
    
    # ==================================================================
    # DELAY CALCULATION
    # ==================================================================
    
    def _calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for retry with exponential backoff and jitter.
        
        Formula: min(initial * base^(attempt-1) + jitter, max_delay)
        
        Args:
            attempt: Current attempt number (1-indexed)
            
        Returns:
            Delay in seconds
        """
        # Exponential backoff
        base_delay = self.config.initial_delay_seconds * (
            self.config.exponential_base ** (attempt - 1)
        )
        
        # Cap at max delay
        base_delay = min(base_delay, self.config.max_delay_seconds)
        
        # Add jitter
        jitter = base_delay * self.config.jitter_factor * random.random()
        
        return base_delay + jitter
    
    def get_next_retry_time(self, attempt: int) -> datetime:
        """
        Get the datetime when next retry should occur.
        
        Args:
            attempt: Current attempt number
            
        Returns:
            Datetime of next retry
        """
        delay = self._calculate_delay(attempt)
        return datetime.now() + timedelta(seconds=delay)
    
    # ==================================================================
    # ERROR CLASSIFICATION
    # ==================================================================
    
    def _classify_error(self, error: Exception) -> ErrorType:
        """
        Classify an error as transient or permanent.
        
        Args:
            error: Exception to classify
            
        Returns:
            ErrorType classification
        """
        error_name = type(error).__name__
        error_str = str(error).lower()
        
        # Check for explicit RetryableError
        if isinstance(error, RetryableError):
            return ErrorType.TRANSIENT
        
        # Check for explicit PermanentError
        if isinstance(error, PermanentError):
            return ErrorType.UNKNOWN  # Already permanent
        
        # Check against known retryable patterns
        for pattern in self.config.retryable_errors:
            if pattern.lower() in error_name.lower() or pattern.lower() in error_str:
                return ErrorType.TRANSIENT
        
        # Check against known non-retryable patterns
        for pattern in self.config.non_retryable_errors:
            if pattern.lower() in error_name.lower() or pattern.lower() in error_str:
                return ErrorType.VALIDATION
        
        # Network/connection errors are typically transient
        transient_keywords = [
            "timeout", "connection", "network", "temporary", "unavailable",
            "rate limit", "throttl", "retry", "transient", "503", "502", "504"
        ]
        
        if any(kw in error_str for kw in transient_keywords):
            return ErrorType.TRANSIENT
        
        # Auth/permission errors are permanent
        permanent_keywords = [
            "auth", "permission", "denied", "forbidden", "401", "403",
            "invalid", "corrupt", "schema", "validation"
        ]
        
        if any(kw in error_str for kw in permanent_keywords):
            return ErrorType.PERMISSION
        
        # Default to unknown (will not retry)
        return ErrorType.UNKNOWN
    
    def is_retryable(self, error: Exception) -> bool:
        """Check if an error should be retried."""
        return self._classify_error(error) == ErrorType.TRANSIENT
    
    # ==================================================================
    # FAILURE QUEUE MANAGEMENT
    # ==================================================================
    
    def add_to_queue(self,
                     sync_id: str,
                     error: Exception,
                     context: Dict = None) -> SyncFailureQueue:
        """
        Add a failed operation to the retry queue.
        
        Args:
            sync_id: ID of the failed sync operation
            error: Exception that caused the failure
            context: Additional context for retry
            
        Returns:
            SyncFailureQueue entry
        """
        error_type = self._classify_error(error)
        
        queue_entry = SyncFailureQueue(
            queue_id=SyncFailureQueue.generate_queue_id(),
            sync_id=sync_id,
            error_type=error_type,
            error_message=str(error),
            retry_count=0,
            max_retries=self.config.max_retries,
            operation_context=context or {}
        )
        
        queue_entry.next_retry_at = queue_entry.calculate_next_retry()
        
        self.failure_queue.append(queue_entry)
        
        logger.info(f"Added to retry queue: sync_id={sync_id}, "
                   f"error_type={error_type.value}, next_retry={queue_entry.next_retry_at}")
        
        return queue_entry
    
    def get_pending_retries(self) -> List[SyncFailureQueue]:
        """Get queue entries that are due for retry."""
        now = datetime.now()
        return [
            entry for entry in self.failure_queue
            if entry.should_retry() and 
               entry.next_retry_at and 
               entry.next_retry_at <= now
        ]
    
    def process_queue(self, 
                      retry_func: Callable[[str, Dict], Any]) -> Dict[str, Any]:
        """
        Process the retry queue.
        
        Args:
            retry_func: Function to call for each retry (sync_id, context) -> result
            
        Returns:
            Summary of processing results
        """
        pending = self.get_pending_retries()
        results = {
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
            "exhausted": 0
        }
        
        for entry in pending:
            results["processed"] += 1
            
            try:
                retry_func(entry.sync_id, entry.operation_context)
                
                # Success - remove from queue
                self.failure_queue.remove(entry)
                results["succeeded"] += 1
                logger.info(f"Retry succeeded for sync_id={entry.sync_id}")
                
            except Exception as e:
                # Update retry count
                entry.retry_count += 1
                entry.last_retry_at = datetime.now()
                
                if entry.should_retry():
                    entry.next_retry_at = entry.calculate_next_retry()
                    results["failed"] += 1
                    logger.warning(f"Retry failed for sync_id={entry.sync_id}, "
                                  f"attempt {entry.retry_count}/{entry.max_retries}")
                else:
                    # Max retries exhausted
                    results["exhausted"] += 1
                    logger.error(f"Max retries exhausted for sync_id={entry.sync_id}")
        
        return results
    
    def get_queue_status(self) -> Dict[str, Any]:
        """Get current queue status."""
        return {
            "total_items": len(self.failure_queue),
            "pending_retry": len([e for e in self.failure_queue if e.should_retry()]),
            "exhausted": len([e for e in self.failure_queue if not e.should_retry()]),
            "by_error_type": self._count_by_error_type()
        }
    
    def _count_by_error_type(self) -> Dict[str, int]:
        """Count queue entries by error type."""
        counts = {}
        for entry in self.failure_queue:
            key = entry.error_type.value if isinstance(entry.error_type, ErrorType) else str(entry.error_type)
            counts[key] = counts.get(key, 0) + 1
        return counts
    
    def clear_queue(self, sync_id: str = None):
        """
        Clear the retry queue.
        
        Args:
            sync_id: If provided, only clear entries for this sync_id
        """
        if sync_id:
            self.failure_queue = [e for e in self.failure_queue if e.sync_id != sync_id]
        else:
            self.failure_queue = []
    
    # ==================================================================
    # LOGGING
    # ==================================================================
    
    def _log_retry_attempt(self, func_name: str, attempt: int, delay: float, error: str):
        """Log a retry attempt."""
        self.retry_history.append({
            "timestamp": datetime.now().isoformat(),
            "function": func_name,
            "attempt": attempt,
            "delay_seconds": delay,
            "error": error,
            "status": "RETRYING"
        })
        
        # Keep only last 1000 entries
        if len(self.retry_history) > 1000:
            self.retry_history = self.retry_history[-1000:]
    
    def _log_retry_success(self, func_name: str, attempt: int):
        """Log a successful retry."""
        self.retry_history.append({
            "timestamp": datetime.now().isoformat(),
            "function": func_name,
            "attempt": attempt,
            "status": "SUCCESS"
        })
    
    def _log_retry_failure(self, func_name: str, attempt: int, error: str, error_type: ErrorType):
        """Log a retry failure."""
        self.retry_history.append({
            "timestamp": datetime.now().isoformat(),
            "function": func_name,
            "attempt": attempt,
            "error": error,
            "error_type": error_type.value,
            "status": "FAILED"
        })
    
    def get_retry_stats(self) -> Dict[str, Any]:
        """Get retry statistics."""
        total = len(self.retry_history)
        successes = len([r for r in self.retry_history if r.get("status") == "SUCCESS"])
        failures = len([r for r in self.retry_history if r.get("status") == "FAILED"])
        
        return {
            "total_retries": total,
            "successful_retries": successes,
            "failed_retries": failures,
            "success_rate": (successes / total * 100) if total > 0 else 0,
            "recent_retries": self.retry_history[-10:]
        }
