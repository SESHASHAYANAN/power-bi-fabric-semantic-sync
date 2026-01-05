"""
Audit Logging Module - Comprehensive Sync Audit Trail

This module provides production-grade audit logging with:
- Color-coded console output (Green=Success, Red=Error, Yellow=Warning)
- Structured JSONL file logging for audit trail
- Detailed context for each event
- Performance metrics tracking

Every sync operation is tracked for:
- Compliance requirements
- Debugging failed syncs
- Performance monitoring
- Change tracking
"""

import os
import sys
import json
import logging
import time
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from contextlib import contextmanager
import threading


class Severity(Enum):
    """Log severity levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class EventType(Enum):
    """Types of sync events."""
    # Sync lifecycle events
    SYNC_START = "SYNC_START"
    SYNC_END = "SYNC_END"
    SYNC_PROGRESS = "SYNC_PROGRESS"
    
    # Discovery events
    MODEL_DISCOVERED = "MODEL_DISCOVERED"
    TABLE_DISCOVERED = "TABLE_DISCOVERED"
    VIEW_DISCOVERED = "VIEW_DISCOVERED"
    
    # Creation events
    VIEW_CREATED = "VIEW_CREATED"
    VIEW_UPDATED = "VIEW_UPDATED"
    VIEW_DELETED = "VIEW_DELETED"
    TABLE_CREATED = "TABLE_CREATED"
    
    # Change detection events
    CHANGE_DETECTED = "CHANGE_DETECTED"
    NO_CHANGES = "NO_CHANGES"
    HASH_COMPUTED = "HASH_COMPUTED"
    
    # Error events
    ERROR = "ERROR"
    RETRY = "RETRY"
    RETRY_SUCCESS = "RETRY_SUCCESS"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
    
    # Connection events
    CONNECTION_SUCCESS = "CONNECTION_SUCCESS"
    CONNECTION_FAILED = "CONNECTION_FAILED"
    AUTH_SUCCESS = "AUTH_SUCCESS"
    AUTH_FAILED = "AUTH_FAILED"
    
    # Naming events
    NAME_SANITIZED = "NAME_SANITIZED"
    RESERVED_KEYWORD_DETECTED = "RESERVED_KEYWORD_DETECTED"
    
    # Scheduler events
    SCHEDULER_START = "SCHEDULER_START"
    SCHEDULER_STOP = "SCHEDULER_STOP"
    SCHEDULER_TICK = "SCHEDULER_TICK"
    
    # General
    INFO = "INFO"
    WARNING = "WARNING"


@dataclass
class AuditLogEntry:
    """
    Represents a single audit log entry.
    
    Contains all information needed for audit trail and debugging.
    """
    timestamp: str
    event_type: str
    severity: str
    message: str
    
    # Optional context fields
    model_name: Optional[str] = None
    table_name: Optional[str] = None
    view_name: Optional[str] = None
    
    # Performance metrics
    duration_ms: Optional[float] = None
    
    # Error details
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    stack_trace: Optional[str] = None
    
    # Retry information
    retry_attempt: Optional[int] = None
    max_retries: Optional[int] = None
    
    # Additional context
    context: Dict[str, Any] = field(default_factory=dict)
    
    # Sync identification
    sync_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "severity": self.severity,
            "message": self.message,
        }
        
        # Add optional fields only if they have values
        if self.model_name:
            result["model_name"] = self.model_name
        if self.table_name:
            result["table_name"] = self.table_name
        if self.view_name:
            result["view_name"] = self.view_name
        if self.duration_ms is not None:
            result["duration_ms"] = self.duration_ms
        if self.error_type:
            result["error_type"] = self.error_type
        if self.error_message:
            result["error_message"] = self.error_message
        if self.stack_trace:
            result["stack_trace"] = self.stack_trace
        if self.retry_attempt is not None:
            result["retry_attempt"] = self.retry_attempt
        if self.max_retries is not None:
            result["max_retries"] = self.max_retries
        if self.context:
            result["context"] = self.context
        if self.sync_id:
            result["sync_id"] = self.sync_id
            
        return result
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter that adds color to console output.
    
    Colors:
    - Green: Success/Info
    - Yellow: Warning
    - Red: Error/Critical
    - Cyan: Debug
    """
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m',
    }
    
    # Emoji indicators for quick visual scanning
    EMOJI = {
        'DEBUG': '🔍',
        'INFO': '✅',
        'WARNING': '⚠️',
        'ERROR': '❌',
        'CRITICAL': '🚨',
    }
    
    def __init__(self, use_colors: bool = True, use_emoji: bool = True):
        """
        Initialize the colored formatter.
        
        Args:
            use_colors: Whether to use ANSI colors (set False for Windows CMD).
            use_emoji: Whether to include emoji indicators.
        """
        super().__init__()
        self.use_colors = use_colors and self._supports_color()
        self.use_emoji = use_emoji
    
    def _supports_color(self) -> bool:
        """Check if the terminal supports colors."""
        # Windows Terminal and modern terminals support color
        if os.name == 'nt':
            # Enable ANSI on Windows 10+
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
                return True
            except Exception:
                return False
        return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
    
    def format(self, record: logging.LogRecord) -> str:
        """Format the log record with colors and emoji."""
        level = record.levelname
        
        # Build the message
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if self.use_colors:
            color = self.COLORS.get(level, self.COLORS['RESET'])
            reset = self.COLORS['RESET']
            level_str = f"{color}{level:8}{reset}"
        else:
            level_str = f"{level:8}"
        
        if self.use_emoji:
            emoji = self.EMOJI.get(level, '•')
            prefix = f"{emoji} "
        else:
            prefix = ""
        
        message = f"{prefix}[{timestamp}] {level_str} | {record.getMessage()}"
        
        return message


class AuditLogger:
    """
    Comprehensive audit logger for sync operations.
    
    Features:
    - Color-coded console output
    - Structured JSONL file output
    - Thread-safe logging
    - Performance tracking
    - In-memory log buffer for recent events
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Singleton pattern to ensure single logger instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(
        self, 
        log_file: str = "sync_audit.jsonl",
        console_output: bool = True,
        file_output: bool = True,
        buffer_size: int = 1000,
        log_dir: Optional[str] = None
    ):
        """
        Initialize the audit logger.
        
        Args:
            log_file: Name of the JSONL log file.
            console_output: Whether to output to console.
            file_output: Whether to output to file.
            buffer_size: Maximum number of entries to keep in memory.
            log_dir: Directory for log files (defaults to current directory).
        """
        # Prevent re-initialization of singleton
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        self.log_file = log_file
        self.console_output = console_output
        self.file_output = file_output
        self.buffer_size = buffer_size
        self.log_dir = log_dir or os.getcwd()
        
        # In-memory buffer for recent logs
        self._buffer: List[AuditLogEntry] = []
        self._buffer_lock = threading.Lock()
        
        # Setup Python logger for console
        self._logger = logging.getLogger("AuditLogger")
        self._logger.setLevel(logging.DEBUG)
        
        # Remove existing handlers
        self._logger.handlers = []
        
        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(ColoredFormatter())
            self._logger.addHandler(console_handler)
        
        # Ensure log directory exists
        if self.file_output:
            os.makedirs(self.log_dir, exist_ok=True)
        
        # File lock for thread-safe writing
        self._file_lock = threading.Lock()
        
        # Current sync context
        self._current_sync_id: Optional[str] = None
        
        # Performance tracking
        self._operation_start_times: Dict[str, float] = {}
        
        self._initialized = True
    
    @property
    def log_file_path(self) -> str:
        """Get full path to log file."""
        return os.path.join(self.log_dir, self.log_file)
    
    def set_sync_id(self, sync_id: str) -> None:
        """Set the current sync ID for all subsequent log entries."""
        self._current_sync_id = sync_id
    
    def clear_sync_id(self) -> None:
        """Clear the current sync ID."""
        self._current_sync_id = None
    
    def log(
        self,
        event_type: EventType,
        message: str,
        severity: Severity = Severity.INFO,
        model_name: Optional[str] = None,
        table_name: Optional[str] = None,
        view_name: Optional[str] = None,
        error: Optional[Exception] = None,
        retry_attempt: Optional[int] = None,
        max_retries: Optional[int] = None,
        duration_ms: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None,
        sync_id: Optional[str] = None
    ) -> AuditLogEntry:
        """
        Log an audit event.
        
        Args:
            event_type: Type of event being logged.
            message: Human-readable message.
            severity: Log severity level.
            model_name: Related model name.
            table_name: Related table name.
            view_name: Related view name.
            error: Exception if this is an error event.
            retry_attempt: Current retry attempt number.
            max_retries: Maximum retry attempts.
            duration_ms: Operation duration in milliseconds.
            context: Additional context dictionary.
            sync_id: Override sync ID for this entry.
            
        Returns:
            The created AuditLogEntry.
        """
        import traceback
        
        # Create entry
        entry = AuditLogEntry(
            timestamp=datetime.now().isoformat(),
            event_type=event_type.value if isinstance(event_type, EventType) else str(event_type),
            severity=severity.value if isinstance(severity, Severity) else str(severity),
            message=message,
            model_name=model_name,
            table_name=table_name,
            view_name=view_name,
            duration_ms=duration_ms,
            retry_attempt=retry_attempt,
            max_retries=max_retries,
            context=context or {},
            sync_id=sync_id or self._current_sync_id,
        )
        
        # Add error details if present
        if error:
            entry.error_type = type(error).__name__
            entry.error_message = str(error)
            entry.stack_trace = traceback.format_exc()
        
        # Console output
        if self.console_output:
            log_level = getattr(logging, severity.value if isinstance(severity, Severity) else "INFO")
            log_message = message
            if model_name:
                log_message = f"[{model_name}] {log_message}"
            if table_name:
                log_message = f"[{table_name}] {log_message}"
            self._logger.log(log_level, log_message)
        
        # File output
        if self.file_output:
            self._write_to_file(entry)
        
        # Buffer
        self._add_to_buffer(entry)
        
        return entry
    
    def _write_to_file(self, entry: AuditLogEntry) -> None:
        """Write entry to JSONL file."""
        with self._file_lock:
            try:
                with open(self.log_file_path, 'a', encoding='utf-8') as f:
                    f.write(entry.to_json() + '\n')
            except Exception as e:
                # Don't fail silently but also don't crash
                self._logger.warning(f"Failed to write to audit log file: {e}")
    
    def _add_to_buffer(self, entry: AuditLogEntry) -> None:
        """Add entry to in-memory buffer."""
        with self._buffer_lock:
            self._buffer.append(entry)
            # Trim buffer if needed
            if len(self._buffer) > self.buffer_size:
                self._buffer = self._buffer[-self.buffer_size:]
    
    def get_recent_logs(self, count: int = 100) -> List[Dict[str, Any]]:
        """Get recent log entries from buffer."""
        with self._buffer_lock:
            entries = self._buffer[-count:]
            return [e.to_dict() for e in entries]
    
    def get_errors(self, count: int = 50) -> List[Dict[str, Any]]:
        """Get recent error entries."""
        with self._buffer_lock:
            errors = [
                e.to_dict() for e in self._buffer 
                if e.severity in (Severity.ERROR.value, Severity.CRITICAL.value, "ERROR", "CRITICAL")
            ]
            return errors[-count:]
    
    # Convenience methods for common log types
    
    def sync_start(self, sync_id: str, context: Optional[Dict] = None) -> AuditLogEntry:
        """Log sync start event."""
        self.set_sync_id(sync_id)
        return self.log(
            EventType.SYNC_START,
            f"Starting sync operation: {sync_id}",
            Severity.INFO,
            context=context,
            sync_id=sync_id
        )
    
    def sync_end(self, success: bool, stats: Optional[Dict] = None) -> AuditLogEntry:
        """Log sync end event."""
        severity = Severity.INFO if success else Severity.ERROR
        message = "Sync completed successfully" if success else "Sync completed with errors"
        
        entry = self.log(
            EventType.SYNC_END,
            message,
            severity,
            context=stats
        )
        self.clear_sync_id()
        return entry
    
    def view_created(
        self, 
        view_name: str, 
        model_name: Optional[str] = None,
        table_name: Optional[str] = None,
        duration_ms: Optional[float] = None
    ) -> AuditLogEntry:
        """Log successful view creation."""
        return self.log(
            EventType.VIEW_CREATED,
            f"Created semantic view: {view_name}",
            Severity.INFO,
            model_name=model_name,
            table_name=table_name,
            view_name=view_name,
            duration_ms=duration_ms
        )
    
    def error(
        self, 
        message: str, 
        error: Exception,
        model_name: Optional[str] = None,
        table_name: Optional[str] = None,
        context: Optional[Dict] = None
    ) -> AuditLogEntry:
        """Log an error event."""
        return self.log(
            EventType.ERROR,
            message,
            Severity.ERROR,
            model_name=model_name,
            table_name=table_name,
            error=error,
            context=context
        )
    
    def retry(
        self, 
        message: str,
        attempt: int,
        max_retries: int,
        delay_seconds: float,
        error: Optional[Exception] = None
    ) -> AuditLogEntry:
        """Log a retry event."""
        return self.log(
            EventType.RETRY,
            f"{message} - Retry {attempt}/{max_retries} in {delay_seconds:.1f}s",
            Severity.WARNING,
            retry_attempt=attempt,
            max_retries=max_retries,
            error=error,
            context={"delay_seconds": delay_seconds}
        )
    
    def reserved_keyword_detected(
        self, 
        original_name: str, 
        sanitized_name: str
    ) -> AuditLogEntry:
        """Log when a reserved keyword is detected and sanitized."""
        return self.log(
            EventType.RESERVED_KEYWORD_DETECTED,
            f"Reserved keyword '{original_name}' sanitized to '{sanitized_name}'",
            Severity.WARNING,
            context={
                "original_name": original_name,
                "sanitized_name": sanitized_name
            }
        )
    
    @contextmanager
    def track_operation(self, operation_name: str):
        """
        Context manager to track operation duration.
        
        Usage:
            with audit_logger.track_operation("create_view"):
                create_the_view()
        """
        start_time = time.time()
        operation_id = f"{operation_name}_{id(start_time)}"
        self._operation_start_times[operation_id] = start_time
        
        try:
            yield
        finally:
            duration_ms = (time.time() - start_time) * 1000
            self._operation_start_times.pop(operation_id, None)
            self.log(
                EventType.INFO,
                f"Operation '{operation_name}' completed in {duration_ms:.2f}ms",
                Severity.DEBUG,
                duration_ms=duration_ms,
                context={"operation": operation_name}
            )
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get logging statistics."""
        with self._buffer_lock:
            total = len(self._buffer)
            by_severity = {}
            by_type = {}
            
            for entry in self._buffer:
                # Count by severity
                sev = entry.severity
                by_severity[sev] = by_severity.get(sev, 0) + 1
                
                # Count by type
                evt = entry.event_type
                by_type[evt] = by_type.get(evt, 0) + 1
            
            return {
                "total_entries": total,
                "by_severity": by_severity,
                "by_event_type": by_type,
                "buffer_capacity": self.buffer_size,
                "log_file": self.log_file_path
            }


# Global singleton instance
_audit_logger: Optional[AuditLogger] = None


def get_audit_logger() -> AuditLogger:
    """Get the global audit logger instance."""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger()
    return _audit_logger


def configure_audit_logger(
    log_file: str = "sync_audit.jsonl",
    console_output: bool = True,
    file_output: bool = True,
    log_dir: Optional[str] = None
) -> AuditLogger:
    """Configure and get the audit logger."""
    global _audit_logger
    _audit_logger = AuditLogger(
        log_file=log_file,
        console_output=console_output,
        file_output=file_output,
        log_dir=log_dir
    )
    return _audit_logger
