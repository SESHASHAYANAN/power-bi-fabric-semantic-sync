# Bidirectional Sync System - Architecture Decision Record

## Document Information
- **Status**: Approved
- **Version**: 1.0
- **Date**: 2026-01-04
- **Author**: Systems Architect

---

## 1. Context

### Problem Statement
Organizations using both Microsoft Fabric and Snowflake face critical data silos:
- Manual file uploads are error-prone and not scalable
- No automatic synchronization between platforms
- Format mismatches (Fabric Delta vs Snowflake tables)
- No data integrity verification
- No conflict resolution for concurrent updates
- No audit trail for compliance

### Goals
1. Zero mock data - only real API calls
2. Automatic bidirectional sync within 5 minutes
3. Atomic dual-write for file uploads
4. Format transparency for users
5. 99.5% sync success rate
6. Full audit trail and rollback capability

---

## 2. Decision Summary

### Core Architecture
We chose a **centralized orchestrator pattern** over alternatives:

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Centralized Orchestrator | Single source of truth, easier debugging, consistent state | Single point of failure | ✅ **Selected** |
| Event-Driven (Kafka) | Highly scalable, decoupled | Complex, eventual consistency | ❌ Rejected |
| Direct Platform Sync | Native, no middleware | Lock-in, limited control | ❌ Rejected |
| Database Replication | Real-time, proven | Requires same DB technology | ❌ Rejected |

**Rationale**: A centralized orchestrator provides the reliability and observability required for this critical infrastructure. The added complexity of event-driven architecture is not justified given the sync volume expectations (<10,000 operations/day).

---

## 3. Key Design Decisions

### 3.1 SYNC_ID for Idempotency

**Decision**: Every sync operation is assigned a UUID v4 `SYNC_ID` at initiation.

**Implementation**:
```python
sync_id = str(uuid.uuid4())  # e.g., "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

**Rationale**:
- Prevents duplicate imports during retries
- Enables correlation across platforms
- Supports cascading deletes (delete by SYNC_ID)
- Required for audit trail linkage

**Idempotency Check**:
```python
existing = SELECT COUNT(*) FROM sync_manifest WHERE sync_id = ?
if existing > 0 and status == 'SYNCED':
    return "Already synced"  # Skip operation
```

### 3.2 Checksum Validation (SHA256)

**Decision**: Validate data integrity using SHA256 checksums after every sync.

**Implementation**:
```python
source_hash = hashlib.sha256(json.dumps(source_data, sort_keys=True).encode()).hexdigest()
target_hash = hashlib.sha256(json.dumps(target_data, sort_keys=True).encode()).hexdigest()

if source_hash != target_hash:
    raise DataIntegrityError("Checksum mismatch")
```

**Rationale**:
- Detects data corruption during transfer
- Catches silent failures in platform APIs
- Provides audit evidence of data integrity
- Zero tolerance for mismatches (no threshold)

### 3.3 Conflict Resolution: Last-Write-Wins

**Decision**: Resolve concurrent modification conflicts using timestamp-based last-write-wins.

**Implementation**:
```python
if source_timestamp > target_timestamp:
    winner = source_record
elif target_timestamp > source_timestamp:
    winner = target_record
else:
    # Tie-breaker: Platform priority (Fabric > Snowflake)
    winner = source_record if source_platform == "fabric" else target_record
```

**Rationale**:
- Simple, deterministic, easy to understand
- Matches user expectations ("my latest change should win")
- Platform priority provides consistent tie-breaking
- All versions logged for audit (no data loss)

**Alternatives Considered**:
- **First-Write-Wins**: Rejected - users expect latest changes to be visible
- **Manual Resolution Only**: Rejected - too slow for automated systems
- **Vector Clocks**: Rejected - overkill for this use case

### 3.4 Exponential Backoff Retry

**Decision**: Retry transient failures with exponential backoff + jitter.

**Implementation**:
```python
delay = min(initial_delay * (2 ** attempt), max_delay)  # 1s, 2s, 4s, 8s... max 5min
jitter = delay * 0.1 * random.random()  # 10% jitter
time.sleep(delay + jitter)
```

**Rationale**:
- Exponential backoff prevents overwhelming recovering services
- Jitter prevents thundering herd when multiple syncs retry simultaneously
- Max delay cap (5 minutes) ensures timely alerts
- 5 retry attempts balance persistence vs. escalation speed

### 3.5 Parallel Dual-Write for File Uploads

**Decision**: Write to both Fabric and Snowflake in parallel, validate both, then commit.

**Implementation**:
```python
with ThreadPoolExecutor(max_workers=2) as executor:
    fabric_future = executor.submit(write_to_fabric, ...)
    snowflake_future = executor.submit(write_to_snowflake, ...)
    
    fabric_result = fabric_future.result(timeout=120)
    snowflake_result = snowflake_future.result(timeout=120)

# Validate both succeeded
if fabric_result.success and snowflake_result.success:
    validate_checksums()
    mark_as_synced()
else:
    rollback_partial()
    queue_for_retry()
```

**Rationale**:
- Parallel writes reduce total latency (2x faster than sequential)
- Timeout prevents indefinite hangs
- Validation ensures atomicity guarantee
- Partial failures are explicitly handled

---

## 4. Data Model Decisions

### 4.1 Sync Manifest Table

**Decision**: Store sync metadata in both platforms for local querying.

**Schema**:
```sql
sync_manifest (
    sync_id         UUID PRIMARY KEY,
    source_table    VARCHAR NOT NULL,
    target_table    VARCHAR,
    status          ENUM('PENDING', 'SYNCING', 'SYNCED', 'FAILED', 'CONFLICT'),
    row_count       BIGINT,
    data_hash       VARCHAR(64),   -- SHA256
    schema_hash     VARCHAR(64),   -- Schema fingerprint
    created_at      TIMESTAMP,
    synced_at       TIMESTAMP,
    error_message   TEXT,
    retry_count     INT DEFAULT 0
)
```

**Rationale**: Each platform can independently verify sync status without cross-platform queries.

### 4.2 Audit Trail Retention

**Decision**: Retain audit trail for 90 days, then archive to cold storage.

**Rationale**:
- 90 days covers most compliance requirements
- Reduces storage costs
- Archived data available for forensics if needed
- Configurable for stricter requirements

---

## 5. API Design Decisions

### 5.1 RESTful Endpoints

**Decision**: Use REST API with JSON payloads.

**Key Endpoints**:
| Method | Endpoint | Purpose |
|--------|----------|---------|
| POST | `/api/files/upload` | Dual-write file upload |
| GET | `/api/files` | List synced files |
| GET | `/api/files/{id}/sync-status` | Get sync status |
| POST | `/api/sync/run` | Trigger full sync |
| POST | `/api/sync/{id}/retry` | Retry failed sync |
| DELETE | `/api/files/{id}` | Cascading delete |

**Rationale**: REST is well-understood, tool-supported, and sufficient for sync operations.

### 5.2 API Rate Limiting

**Decision**: Target ≤2 API calls per file sync.

**Implementation**:
- Batch operations where possible
- Cache authentication tokens
- Reuse connections

**Rationale**: Minimizes API costs and avoids rate limiting from platforms.

---

## 6. Observability Decisions

### 6.1 Metrics to Emit

| Metric | Type | Purpose |
|--------|------|---------|
| `sync_duration_seconds` | Histogram | Track latency distribution |
| `sync_success_total` | Counter | Track success rate |
| `sync_failure_total` | Counter | Track failure rate |
| `checksum_mismatch_total` | Counter | Data integrity issues |
| `conflict_detected_total` | Counter | Conflict frequency |
| `retry_queue_size` | Gauge | Pending retries |

### 6.2 Alert Thresholds

| Alert | Threshold | Severity |
|-------|-----------|----------|
| Checksum mismatch | Any | CRITICAL |
| Failure rate > 5% | 1 hour | CRITICAL |
| Latency p95 > 15min | 15 min | HIGH |
| Retry queue > 100 | 30 min | HIGH |

---

## 7. Trade-offs Accepted

### 7.1 Consistency vs. Availability
**Choice**: Strong consistency over availability.

When a sync fails validation, we do NOT proceed with partial data. Users see stale data until sync succeeds.

**Rationale**: Data integrity is non-negotiable for analytics workloads.

### 7.2 Complexity vs. Reliability
**Choice**: More complex validation over simpler happy-path.

Every sync includes checksum validation, schema comparison, and row count verification.

**Rationale**: The cost of undetected data corruption far exceeds validation overhead.

### 7.3 Sync Latency vs. Batch Efficiency
**Choice**: 5-minute target over real-time.

Near-real-time sync is achieved via polling every 5 minutes rather than streaming.

**Rationale**: 5-minute latency is acceptable for analytics. Polling is simpler and more reliable than maintaining streaming connections.

---

## 8. Future Considerations

### 8.1 Not Implemented Yet
- **Delta Lake format direct reading** (requires Fabric OneLake SDK)
- **Snowflake Streams for real-time CDC** (requires additional infrastructure)
- **Multi-workspace support** (requires workspace mapping)
- **Schema evolution handling** (auto-migrate on schema changes)

### 8.2 Scaling Path
If sync volume exceeds 100,000/day:
1. Consider message queue (Azure Service Bus) for decoupling
2. Implement worker pool for parallel sync execution
3. Add Redis for distributed locking
4. Consider Snowflake External Tables for direct read

---

## 9. References

- [Microsoft Fabric REST API Documentation](https://learn.microsoft.com/en-us/rest/api/fabric/)
- [Snowflake Streams for CDC](https://docs.snowflake.com/en/user-guide/streams)
- [Exponential Backoff Pattern](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
- [Two-Phase Commit Considerations](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)

---

## 10. Approval

| Role | Name | Date |
|------|------|------|
| Systems Architect | Auto-generated | 2026-01-04 |
| Tech Lead | Pending | - |
| Data Platform Owner | Pending | - |
