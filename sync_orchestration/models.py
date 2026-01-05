"""
Data Models for Sync Orchestration

Production-ready data classes for:
- Sync manifest tracking
- Failure queue management
- Conflict logging
- Audit trail
"""

import uuid
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum


class SyncStatus(Enum):
    """Sync operation status."""
    PENDING = "PENDING"
    SYNCING = "SYNCING"
    SYNCED = "SYNCED"
    FAILED = "FAILED"
    CONFLICT = "CONFLICT"
    ROLLBACK = "ROLLBACK"
    RETRY_PENDING = "RETRY_PENDING"


class SyncDirection(Enum):
    """Sync direction enumeration."""
    FABRIC_TO_SNOWFLAKE = "fabric_to_snowflake"
    SNOWFLAKE_TO_FABRIC = "snowflake_to_fabric"
    BIDIRECTIONAL = "bidirectional"
    FILE_UPLOAD = "file_upload"


class ErrorType(Enum):
    """Error classification for retry logic."""
    TRANSIENT = "TRANSIENT"  # Network, timeout - retry with backoff
    VALIDATION = "VALIDATION"  # Schema mismatch - manual review
    PERMISSION = "PERMISSION"  # Auth issue - alert
    DATA_CORRUPTION = "DATA_CORRUPTION"  # Checksum fail - critical alert
    UNKNOWN = "UNKNOWN"


class ResolutionMethod(Enum):
    """Conflict resolution methods."""
    LAST_WRITE_WINS = "LAST_WRITE_WINS"
    PLATFORM_PRIORITY = "PLATFORM_PRIORITY"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    SOURCE_WINS = "SOURCE_WINS"


@dataclass
class SyncManifest:
    """
    Sync manifest record - tracks individual sync operations.
    
    This is the source of truth for idempotency checking.
    Every sync operation MUST have a SYNC_ID in the manifest.
    """
    sync_id: str
    source_table: str
    target_table: Optional[str] = None
    filename: Optional[str] = None
    source_platform: str = "fabric"  # fabric | snowflake | file_upload
    target_platform: str = "snowflake"  # fabric | snowflake
    
    # Status tracking
    status: SyncStatus = field(default=SyncStatus.PENDING)
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str = "system"
    synced_at: Optional[datetime] = None
    last_modified_at: Optional[datetime] = None
    
    # Data integrity
    row_count_source: Optional[int] = None
    row_count_target: Optional[int] = None
    schema_hash: Optional[str] = None
    data_hash: Optional[str] = None
    
    # Error tracking
    error_message: Optional[str] = None
    retry_count: int = 0
    
    # Sync version for conflict detection
    sync_version: int = 1
    
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @staticmethod
    def generate_sync_id() -> str:
        """Generate a unique SYNC_ID using UUID v4."""
        return str(uuid.uuid4())
    
    @staticmethod
    def compute_data_hash(data: bytes) -> str:
        """Compute SHA256 hash of data for integrity verification."""
        return hashlib.sha256(data).hexdigest()
    
    @staticmethod
    def compute_schema_hash(columns: List[Dict]) -> str:
        """Compute hash of schema for change detection."""
        import json
        schema_str = json.dumps(columns, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "sync_id": self.sync_id,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "filename": self.filename,
            "source_platform": self.source_platform,
            "target_platform": self.target_platform,
            "status": self.status.value if isinstance(self.status, SyncStatus) else self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "created_by": self.created_by,
            "synced_at": self.synced_at.isoformat() if self.synced_at else None,
            "last_modified_at": self.last_modified_at.isoformat() if self.last_modified_at else None,
            "row_count_source": self.row_count_source,
            "row_count_target": self.row_count_target,
            "schema_hash": self.schema_hash,
            "data_hash": self.data_hash,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "sync_version": self.sync_version,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SyncManifest':
        """Create from dictionary."""
        return cls(
            sync_id=data.get("sync_id", cls.generate_sync_id()),
            source_table=data.get("source_table", ""),
            target_table=data.get("target_table"),
            filename=data.get("filename"),
            source_platform=data.get("source_platform", "fabric"),
            target_platform=data.get("target_platform", "snowflake"),
            status=SyncStatus(data.get("status", "PENDING")),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(),
            created_by=data.get("created_by", "system"),
            synced_at=datetime.fromisoformat(data["synced_at"]) if data.get("synced_at") else None,
            row_count_source=data.get("row_count_source"),
            row_count_target=data.get("row_count_target"),
            schema_hash=data.get("schema_hash"),
            data_hash=data.get("data_hash"),
            error_message=data.get("error_message"),
            retry_count=data.get("retry_count", 0),
            sync_version=data.get("sync_version", 1),
            metadata=data.get("metadata", {})
        )


@dataclass
class SyncFailureQueue:
    """
    Failed sync operations pending retry.
    
    Implements exponential backoff retry strategy.
    """
    queue_id: str
    sync_id: str
    error_type: ErrorType = field(default=ErrorType.UNKNOWN)
    error_message: str = ""
    
    # Retry configuration
    retry_count: int = 0
    max_retries: int = 5
    next_retry_at: Optional[datetime] = None
    
    # Timestamps
    failed_at: datetime = field(default_factory=datetime.now)
    last_retry_at: Optional[datetime] = None
    
    # Context for retry
    operation_context: Dict[str, Any] = field(default_factory=dict)
    
    @staticmethod
    def generate_queue_id() -> str:
        """Generate unique queue ID."""
        return str(uuid.uuid4())
    
    def calculate_next_retry(self) -> datetime:
        """Calculate next retry time with exponential backoff + jitter."""
        import random
        
        base_delay = min(2 ** self.retry_count, 300)  # Max 5 minutes
        jitter = base_delay * 0.1 * random.random()
        delay_seconds = base_delay + jitter
        
        return datetime.now() + __import__('datetime').timedelta(seconds=delay_seconds)
    
    def should_retry(self) -> bool:
        """Determine if this failure should be retried."""
        if self.retry_count >= self.max_retries:
            return False
        
        # Don't retry permanent errors
        if self.error_type in [ErrorType.PERMISSION, ErrorType.DATA_CORRUPTION]:
            return False
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "queue_id": self.queue_id,
            "sync_id": self.sync_id,
            "error_type": self.error_type.value if isinstance(self.error_type, ErrorType) else self.error_type,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "next_retry_at": self.next_retry_at.isoformat() if self.next_retry_at else None,
            "failed_at": self.failed_at.isoformat() if self.failed_at else None,
            "last_retry_at": self.last_retry_at.isoformat() if self.last_retry_at else None,
            "operation_context": self.operation_context
        }


@dataclass
class ConflictLog:
    """
    Log of detected conflicts between platforms.
    
    Stores both versions for audit and potential manual resolution.
    """
    conflict_id: str
    sync_id: str
    record_id: str
    
    source_platform: str = "fabric"
    target_platform: str = "snowflake"
    
    # Conflict details
    source_version: Dict[str, Any] = field(default_factory=dict)
    target_version: Dict[str, Any] = field(default_factory=dict)
    source_timestamp: Optional[datetime] = None
    target_timestamp: Optional[datetime] = None
    
    # Resolution
    resolved_record: Dict[str, Any] = field(default_factory=dict)
    resolution_method: ResolutionMethod = field(default=ResolutionMethod.LAST_WRITE_WINS)
    
    # Audit
    detected_at: datetime = field(default_factory=datetime.now)
    resolved_at: Optional[datetime] = None
    resolved_by: str = "system"
    
    @staticmethod
    def generate_conflict_id() -> str:
        """Generate unique conflict ID."""
        return str(uuid.uuid4())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "conflict_id": self.conflict_id,
            "sync_id": self.sync_id,
            "record_id": self.record_id,
            "source_platform": self.source_platform,
            "target_platform": self.target_platform,
            "source_version": self.source_version,
            "target_version": self.target_version,
            "source_timestamp": self.source_timestamp.isoformat() if self.source_timestamp else None,
            "target_timestamp": self.target_timestamp.isoformat() if self.target_timestamp else None,
            "resolved_record": self.resolved_record,
            "resolution_method": self.resolution_method.value if isinstance(self.resolution_method, ResolutionMethod) else self.resolution_method,
            "detected_at": self.detected_at.isoformat() if self.detected_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "resolved_by": self.resolved_by
        }


@dataclass
class AuditTrail:
    """
    Complete audit trail for all sync operations.
    
    Every action is logged for compliance and debugging.
    """
    audit_id: str
    sync_id: str
    
    # Operation details
    action: str  # DUAL_WRITE, SYNC_START, SYNC_COMPLETE, RETRY, CONFLICT_DETECTED, etc.
    actor: str = "system"  # user_id or 'system'
    
    # Context
    source_platform: Optional[str] = None
    target_platform: Optional[str] = None
    affected_table: Optional[str] = None
    affected_rows: Optional[int] = None
    
    # Status
    status: str = "INFO"  # INFO, WARNING, ERROR, CRITICAL
    message: str = ""
    
    # Performance
    latency_ms: Optional[int] = None
    
    # Timestamps
    timestamp: datetime = field(default_factory=datetime.now)
    
    # Additional context
    context: Dict[str, Any] = field(default_factory=dict)
    
    @staticmethod
    def generate_audit_id() -> str:
        """Generate unique audit ID."""
        return str(uuid.uuid4())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "audit_id": self.audit_id,
            "sync_id": self.sync_id,
            "action": self.action,
            "actor": self.actor,
            "source_platform": self.source_platform,
            "target_platform": self.target_platform,
            "affected_table": self.affected_table,
            "affected_rows": self.affected_rows,
            "status": self.status,
            "message": self.message,
            "latency_ms": self.latency_ms,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "context": self.context
        }


@dataclass
class SyncMetrics:
    """
    Aggregated metrics for monitoring dashboard.
    """
    total_syncs: int = 0
    successful_syncs: int = 0
    failed_syncs: int = 0
    
    # Success rate
    @property
    def success_rate(self) -> float:
        if self.total_syncs == 0:
            return 0.0
        return (self.successful_syncs / self.total_syncs) * 100
    
    # Latency metrics
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    
    # Data integrity
    checksum_mismatches: int = 0
    conflicts_detected: int = 0
    conflicts_auto_resolved: int = 0
    
    # Queue status
    retry_queue_size: int = 0
    pending_syncs: int = 0
    
    # Time window
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_syncs": self.total_syncs,
            "successful_syncs": self.successful_syncs,
            "failed_syncs": self.failed_syncs,
            "success_rate": round(self.success_rate, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "p95_latency_ms": round(self.p95_latency_ms, 2),
            "p99_latency_ms": round(self.p99_latency_ms, 2),
            "checksum_mismatches": self.checksum_mismatches,
            "conflicts_detected": self.conflicts_detected,
            "conflicts_auto_resolved": self.conflicts_auto_resolved,
            "retry_queue_size": self.retry_queue_size,
            "pending_syncs": self.pending_syncs,
            "window_start": self.window_start.isoformat() if self.window_start else None,
            "window_end": self.window_end.isoformat() if self.window_end else None
        }
