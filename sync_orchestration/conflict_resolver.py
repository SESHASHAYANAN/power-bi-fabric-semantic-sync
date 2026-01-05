"""
Conflict Resolver - Last-Write-Wins with Audit Trail

Handles concurrent modification conflicts between Fabric and Snowflake
with full audit logging and optional manual review escalation.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

from .models import ConflictLog, ResolutionMethod, AuditTrail

logger = logging.getLogger(__name__)


@dataclass
class ConflictResolution:
    """Result of conflict resolution."""
    sync_id: str
    record_id: str
    winner_platform: str
    winner_version: Dict[str, Any]
    loser_platform: str
    loser_version: Dict[str, Any]
    resolution_method: ResolutionMethod
    auto_resolved: bool
    conflict_log: ConflictLog


class ConflictResolver:
    """
    Conflict resolution engine for bidirectional sync.
    
    Default strategy: Last-Write-Wins based on timestamps
    Fallback: Platform priority (Fabric wins ties)
    
    All conflicts are logged with both versions for audit purposes.
    """
    
    # Platform priority for tie-breaking (higher priority wins)
    PLATFORM_PRIORITY = {
        "fabric": 2,
        "snowflake": 1,
        "file_upload": 3
    }
    
    # Default timestamp column names to check
    TIMESTAMP_COLUMNS = [
        "updated_at",
        "modified_at",
        "last_modified",
        "timestamp",
        "_synced_at",
        "created_at"
    ]
    
    def __init__(self, 
                 default_method: ResolutionMethod = ResolutionMethod.LAST_WRITE_WINS,
                 timestamp_column: str = None):
        """
        Initialize conflict resolver.
        
        Args:
            default_method: Default resolution method
            timestamp_column: Specific timestamp column to use for comparison
        """
        self.default_method = default_method
        self.timestamp_column = timestamp_column
        self.conflict_logs: List[ConflictLog] = []
        self.audit_logs: List[AuditTrail] = []
    
    # ==================================================================
    # CONFLICT DETECTION
    # ==================================================================
    
    def detect_conflict(self,
                        source_record: Dict[str, Any],
                        target_record: Dict[str, Any],
                        source_platform: str,
                        target_platform: str) -> bool:
        """
        Detect if a conflict exists between two records.
        
        A conflict exists when:
        1. Both records have been modified since last sync
        2. The modifications result in different values
        
        Args:
            source_record: Record from source platform
            target_record: Record from target platform
            source_platform: Name of source platform
            target_platform: Name of target platform
            
        Returns:
            True if conflict detected, False otherwise
        """
        if target_record is None:
            return False  # Target doesn't exist, no conflict
        
        # Compare non-metadata columns
        ignore_cols = {"_sync_id", "_sync_source", "_synced_at", "_sync_version"}
        
        source_keys = {k.lower() for k in source_record.keys()} - ignore_cols
        target_keys = {k.lower() for k in target_record.keys()} - ignore_cols
        
        common_keys = source_keys & target_keys
        
        for key in common_keys:
            source_val = self._get_value_case_insensitive(source_record, key)
            target_val = self._get_value_case_insensitive(target_record, key)
            
            if str(source_val) != str(target_val):
                # Values differ - check if this is from different updates
                source_ts = self._get_timestamp(source_record)
                target_ts = self._get_timestamp(target_record)
                
                if source_ts and target_ts:
                    # Both have been modified - conflict!
                    return True
        
        return False
    
    def detect_batch_conflicts(self,
                                source_data: List[Dict],
                                target_data: List[Dict],
                                key_column: str,
                                source_platform: str,
                                target_platform: str) -> List[Dict]:
        """
        Detect conflicts in batch data.
        
        Args:
            source_data: Records from source platform
            target_data: Records from target platform
            key_column: Primary key column for matching
            source_platform: Name of source platform
            target_platform: Name of target platform
            
        Returns:
            List of detected conflicts with details
        """
        conflicts = []
        
        # Index target by key
        target_by_key = {
            str(r.get(key_column, r.get(key_column.upper(), ""))): r
            for r in target_data
        }
        
        for source_record in source_data:
            key_val = str(source_record.get(key_column, source_record.get(key_column.upper(), "")))
            
            if key_val in target_by_key:
                target_record = target_by_key[key_val]
                
                if self.detect_conflict(source_record, target_record, source_platform, target_platform):
                    conflicts.append({
                        "key": key_val,
                        "source_record": source_record,
                        "target_record": target_record,
                        "source_platform": source_platform,
                        "target_platform": target_platform
                    })
        
        return conflicts
    
    # ==================================================================
    # CONFLICT RESOLUTION
    # ==================================================================
    
    def resolve_conflict(self,
                         sync_id: str,
                         record_id: str,
                         source_record: Dict[str, Any],
                         target_record: Dict[str, Any],
                         source_platform: str,
                         target_platform: str,
                         method: ResolutionMethod = None) -> ConflictResolution:
        """
        Resolve a conflict between two records.
        
        Args:
            sync_id: ID of the sync operation
            record_id: ID of the conflicting record
            source_record: Record from source platform
            target_record: Record from target platform
            source_platform: Name of source platform
            target_platform: Name of target platform
            method: Resolution method (defaults to configured method)
            
        Returns:
            ConflictResolution with winner and audit info
        """
        if method is None:
            method = self.default_method
        
        winner_record = None
        winner_platform = None
        loser_platform = None
        loser_record = None
        
        if method == ResolutionMethod.LAST_WRITE_WINS:
            winner_record, winner_platform, loser_record, loser_platform = \
                self._resolve_by_timestamp(
                    source_record, target_record,
                    source_platform, target_platform
                )
        
        elif method == ResolutionMethod.PLATFORM_PRIORITY:
            winner_record, winner_platform, loser_record, loser_platform = \
                self._resolve_by_platform(
                    source_record, target_record,
                    source_platform, target_platform
                )
        
        elif method == ResolutionMethod.SOURCE_WINS:
            winner_record = source_record
            winner_platform = source_platform
            loser_record = target_record
            loser_platform = target_platform
        
        else:
            # MANUAL_REVIEW - don't auto-resolve
            winner_record = None
            winner_platform = "pending_review"
        
        # Create conflict log
        conflict_log = ConflictLog(
            conflict_id=ConflictLog.generate_conflict_id(),
            sync_id=sync_id,
            record_id=record_id,
            source_platform=source_platform,
            target_platform=target_platform,
            source_version=source_record,
            target_version=target_record,
            source_timestamp=self._get_timestamp(source_record),
            target_timestamp=self._get_timestamp(target_record),
            resolved_record=winner_record or {},
            resolution_method=method,
            resolved_at=datetime.now() if winner_record else None,
            resolved_by="system"
        )
        
        self.conflict_logs.append(conflict_log)
        
        # Create audit entry
        audit = AuditTrail(
            audit_id=AuditTrail.generate_audit_id(),
            sync_id=sync_id,
            action="CONFLICT_RESOLVED" if winner_record else "CONFLICT_PENDING_REVIEW",
            source_platform=source_platform,
            target_platform=target_platform,
            status="INFO" if winner_record else "WARNING",
            message=f"Conflict on record {record_id}: {winner_platform} won via {method.value}" if winner_record else f"Conflict on record {record_id} requires manual review",
            context={
                "record_id": record_id,
                "resolution_method": method.value,
                "winner": winner_platform
            }
        )
        
        self.audit_logs.append(audit)
        
        logger.info(f"Conflict resolved: sync_id={sync_id}, record_id={record_id}, "
                   f"winner={winner_platform}, method={method.value}")
        
        return ConflictResolution(
            sync_id=sync_id,
            record_id=record_id,
            winner_platform=winner_platform or "pending",
            winner_version=winner_record or {},
            loser_platform=loser_platform or "unknown",
            loser_version=loser_record or {},
            resolution_method=method,
            auto_resolved=winner_record is not None,
            conflict_log=conflict_log
        )
    
    def resolve_batch_conflicts(self,
                                 conflicts: List[Dict],
                                 sync_id: str,
                                 key_column: str,
                                 method: ResolutionMethod = None) -> List[ConflictResolution]:
        """
        Resolve multiple conflicts in batch.
        
        Args:
            conflicts: List of detected conflicts
            sync_id: ID of the sync operation
            key_column: Primary key column
            method: Resolution method
            
        Returns:
            List of ConflictResolution objects
        """
        resolutions = []
        
        for conflict in conflicts:
            resolution = self.resolve_conflict(
                sync_id=sync_id,
                record_id=str(conflict.get("key", "")),
                source_record=conflict.get("source_record", {}),
                target_record=conflict.get("target_record", {}),
                source_platform=conflict.get("source_platform", "unknown"),
                target_platform=conflict.get("target_platform", "unknown"),
                method=method
            )
            resolutions.append(resolution)
        
        return resolutions
    
    # ==================================================================
    # RESOLUTION STRATEGIES
    # ==================================================================
    
    def _resolve_by_timestamp(self,
                               source_record: Dict,
                               target_record: Dict,
                               source_platform: str,
                               target_platform: str) -> Tuple[Dict, str, Dict, str]:
        """
        Resolve conflict by timestamp (last-write-wins).
        
        Returns:
            Tuple of (winner_record, winner_platform, loser_record, loser_platform)
        """
        source_ts = self._get_timestamp(source_record)
        target_ts = self._get_timestamp(target_record)
        
        if source_ts and target_ts:
            if source_ts > target_ts:
                return source_record, source_platform, target_record, target_platform
            elif target_ts > source_ts:
                return target_record, target_platform, source_record, source_platform
            else:
                # Equal timestamps - fall back to platform priority
                return self._resolve_by_platform(
                    source_record, target_record,
                    source_platform, target_platform
                )
        
        # If only one has timestamp, it wins
        if source_ts and not target_ts:
            return source_record, source_platform, target_record, target_platform
        if target_ts and not source_ts:
            return target_record, target_platform, source_record, source_platform
        
        # Neither has timestamp - fall back to platform priority
        return self._resolve_by_platform(
            source_record, target_record,
            source_platform, target_platform
        )
    
    def _resolve_by_platform(self,
                              source_record: Dict,
                              target_record: Dict,
                              source_platform: str,
                              target_platform: str) -> Tuple[Dict, str, Dict, str]:
        """
        Resolve conflict by platform priority.
        
        Returns:
            Tuple of (winner_record, winner_platform, loser_record, loser_platform)
        """
        source_priority = self.PLATFORM_PRIORITY.get(source_platform.lower(), 0)
        target_priority = self.PLATFORM_PRIORITY.get(target_platform.lower(), 0)
        
        if source_priority >= target_priority:
            return source_record, source_platform, target_record, target_platform
        else:
            return target_record, target_platform, source_record, source_platform
    
    # ==================================================================
    # HELPER METHODS
    # ==================================================================
    
    def _get_timestamp(self, record: Dict) -> Optional[datetime]:
        """Extract timestamp from record."""
        if self.timestamp_column:
            columns_to_check = [self.timestamp_column]
        else:
            columns_to_check = self.TIMESTAMP_COLUMNS
        
        for col in columns_to_check:
            val = record.get(col) or record.get(col.upper()) or record.get(col.lower())
            
            if val:
                if isinstance(val, datetime):
                    return val
                try:
                    return datetime.fromisoformat(str(val).replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    pass
        
        return None
    
    def _get_value_case_insensitive(self, record: Dict, key: str) -> Any:
        """Get value from record with case-insensitive key lookup."""
        key_lower = key.lower()
        
        for k, v in record.items():
            if k.lower() == key_lower:
                return v
        
        return None
    
    # ==================================================================
    # REPORTING
    # ==================================================================
    
    def get_conflict_summary(self) -> Dict[str, Any]:
        """Get summary of all conflicts."""
        auto_resolved = sum(1 for r in self.conflict_logs if r.resolved_at is not None)
        pending_review = sum(1 for r in self.conflict_logs if r.resolved_at is None)
        
        by_method = {}
        for log in self.conflict_logs:
            method = log.resolution_method.value if isinstance(log.resolution_method, ResolutionMethod) else str(log.resolution_method)
            by_method[method] = by_method.get(method, 0) + 1
        
        return {
            "total_conflicts": len(self.conflict_logs),
            "auto_resolved": auto_resolved,
            "pending_review": pending_review,
            "by_resolution_method": by_method,
            "recent_conflicts": [c.to_dict() for c in self.conflict_logs[-10:]]
        }
    
    def get_conflicts_for_sync(self, sync_id: str) -> List[Dict]:
        """Get all conflicts for a specific sync operation."""
        return [
            c.to_dict() 
            for c in self.conflict_logs 
            if c.sync_id == sync_id
        ]
    
    def clear_logs(self):
        """Clear all conflict and audit logs."""
        self.conflict_logs = []
        self.audit_logs = []
