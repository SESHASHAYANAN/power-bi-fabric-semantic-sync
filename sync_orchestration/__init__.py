"""
Sync Orchestration Package - Production-Ready Bidirectional Data Sync

This package provides zero-downtime bidirectional data synchronization
between Microsoft Fabric and Snowflake with:
- SYNC_ID-based idempotency
- Checksum validation
- Conflict resolution (last-write-wins)
- Exponential backoff retry
- Full audit trail
"""

from .sync_engine import SyncOrchestrator
from .format_converter import FormatConverter
from .conflict_resolver import ConflictResolver
from .validation_engine import ValidationEngine
from .retry_orchestrator import RetryOrchestrator
from .change_detector import FabricChangeDetector, SnowflakeChangeDetector
from .models import SyncManifest, SyncFailureQueue, ConflictLog, AuditTrail

__version__ = "1.0.0"
__all__ = [
    "SyncOrchestrator",
    "FormatConverter", 
    "ConflictResolver",
    "ValidationEngine",
    "RetryOrchestrator",
    "FabricChangeDetector",
    "SnowflakeChangeDetector",
    "SyncManifest",
    "SyncFailureQueue",
    "ConflictLog",
    "AuditTrail",
]
