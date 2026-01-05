# Troubleshooting Guide - Bidirectional Sync System

## Quick Reference

| Symptom | Likely Cause | First Action |
|---------|--------------|--------------|
| No syncs completing | Platform connectivity | Check `/api/health` endpoint |
| High failure rate | Schema mismatch | Review error logs for patterns |
| Checksum mismatch | Data corruption | **STOP** and investigate |
| Slow syncs | Large data volume | Check row counts |
| Conflicts detected | Concurrent edits | Review conflict log |

---

## 1. Connection Issues

### 1.1 Fabric Connection Failed

**Symptoms**:
- Error: "Fabric authentication failed"
- Health check shows `fabric: auth_failed`

**Diagnosis**:
```bash
# Check environment variables
echo %FABRIC_TENANT_ID%
echo %FABRIC_CLIENT_ID%
echo %FABRIC_CLIENT_SECRET%
echo %FABRIC_WORKSPACE_ID%
```

**Common Causes**:
1. **Expired client secret**: Regenerate in Azure Portal
2. **Wrong tenant ID**: Verify in Azure Active Directory
3. **API permissions missing**: Check App Registration permissions
4. **Workspace access revoked**: Verify service principal has access

**Resolution**:
1. Go to Azure Portal → App Registrations → Your App
2. Check Certificates & Secrets → Regenerate if expired
3. Verify API Permissions: `Fabric.ReadWrite.All`, `Workspace.ReadWrite.All`
4. Update `.env` file with new credentials
5. Restart the sync service

### 1.2 Snowflake Connection Failed

**Symptoms**:
- Error: "Failed to connect to Snowflake"
- Error: "IP not allowed" or "Network policy"

**Diagnosis**:
```sql
-- In Snowflake, check network policies
SHOW NETWORK POLICIES;
DESCRIBE NETWORK POLICY <policy_name>;

-- Check account status
SELECT SYSTEM$WHITELIST();
```

**Common Causes**:
1. **Wrong account identifier**: Format should be `ACCOUNT.REGION.CLOUD`
2. **Network policy blocking IP**: Add sync service IP to allowlist
3. **Warehouse suspended**: Start or resize warehouse
4. **Password expired**: Reset user password

**Resolution**:
```sql
-- Allow IP (run as ACCOUNTADMIN)
CREATE OR REPLACE NETWORK POLICY sync_service_policy
  ALLOWED_IP_LIST = ('YOUR.SYNC.SERVICE.IP');

ALTER USER SYNC_SERVICE SET NETWORK_POLICY = 'sync_service_policy';

-- Resume warehouse
ALTER WAREHOUSE COMPUTE_WAREHOUSE RESUME;
```

---

## 2. Sync Failures

### 2.1 Schema Mismatch Errors

**Symptoms**:
- Error: "Column X not found in target"
- Error: "Type incompatibility: String vs Integer"

**Diagnosis**:
```python
# Compare schemas
GET /api/fabric/models  # Get Fabric schema
GET /api/snowflake/tables  # Get Snowflake schema
# Compare column names (case-insensitive) and types
```

**Common Causes**:
1. **Case sensitivity**: Fabric uses mixed case, Snowflake defaults to uppercase
2. **Type mapping mismatch**: New Fabric type not in converter
3. **Column added/removed**: Schema evolved since last sync

**Resolution**:
1. Review `sync_orchestration/format_converter.py` type mappings
2. Add missing type mapping if needed
3. For schema evolution:
   - Drop and recreate target table (if acceptable)
   - OR add missing columns manually:
   ```sql
   ALTER TABLE "TARGET_TABLE" ADD COLUMN "NEW_COL" VARCHAR;
   ```

### 2.2 Timeout Errors

**Symptoms**:
- Error: "Operation timed out after 120 seconds"
- Sync stuck in "SYNCING" status

**Diagnosis**:
```sql
-- Check for locks in Snowflake
SELECT * FROM INFORMATION_SCHEMA.LOCKS;

-- Check for long-running queries
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE EXECUTION_STATUS = 'RUNNING';
```

**Common Causes**:
1. **Large data volume**: File > 1GB or > 1M rows
2. **Warehouse undersized**: Need larger compute
3. **Network latency**: Geographic distance
4. **Platform throttling**: Hit API rate limits

**Resolution**:
1. For large data:
   ```python
   # Increase timeout in sync_engine.py
   result = future.result(timeout=300)  # 5 minutes
   ```
2. For Snowflake:
   ```sql
   ALTER WAREHOUSE COMPUTE_WAREHOUSE 
   SET WAREHOUSE_SIZE = 'LARGE';
   ```
3. For rate limiting:
   - Add delay between operations
   - Implement request batching

### 2.3 Row Count Mismatch

**Symptoms**:
- Error: "Row count mismatch: 1000 vs 999"
- Validation failed after sync

**Diagnosis**:
```sql
-- In Snowflake, check for NULL handling
SELECT COUNT(*) FROM "TABLE";  -- Total
SELECT COUNT(*) FROM "TABLE" WHERE "COLUMN" IS NOT NULL;  -- Non-null

-- Check for duplicate keys
SELECT "ID", COUNT(*) 
FROM "TABLE" 
GROUP BY "ID" 
HAVING COUNT(*) > 1;
```

**Common Causes**:
1. **NULL handling difference**: One platform counts NULLs differently
2. **Duplicate inserts**: Retry without idempotency
3. **Concurrent deletes**: Data changed during sync
4. **Filter discrepancy**: WHERE clause mismatch

**Resolution**:
1. Check SYNC_ID in target to ensure idempotency working
2. Run reconciliation query:
   ```sql
   -- Find missing rows
   SELECT * FROM source_table s
   LEFT JOIN target_table t ON s.id = t.id
   WHERE t.id IS NULL;
   ```
3. Re-run sync with validation disabled, then validate manually

---

## 3. Data Integrity Issues

### 3.1 Checksum Mismatch (CRITICAL)

**Symptoms**:
- Alert: "Checksum mismatch detected"
- Error: "Data integrity verification failed"

**IMPORTANT**: Do NOT overwrite target data until investigation complete.

**Diagnosis**:
```python
# Get both datasets
source_data = query_fabric(table)
target_data = query_snowflake(table)

# Compare checksums
import hashlib, json
source_hash = hashlib.sha256(json.dumps(source_data, sort_keys=True).encode()).hexdigest()
target_hash = hashlib.sha256(json.dumps(target_data, sort_keys=True).encode()).hexdigest()

print(f"Source: {source_hash}")
print(f"Target: {target_hash}")

# Find differences
import pandas as pd
df_source = pd.DataFrame(source_data)
df_target = pd.DataFrame(target_data)
diff = df_source.compare(df_target)
print(diff)
```

**Common Causes**:
1. **Floating point precision**: Different rounding between platforms
2. **Timestamp format**: Timezone or precision differences
3. **Encoding issues**: Unicode normalization
4. **Actual data corruption**: Network/disk issues

**Resolution**:
1. For precision issues:
   ```python
   # Round floats before hashing
   for col in numeric_columns:
       data[col] = round(data[col], 6)
   ```
2. For timestamps:
   ```python
   # Normalize to ISO format
   data[col] = data[col].isoformat()
   ```
3. For actual corruption:
   - Identify affected rows
   - Re-sync from authoritative source
   - Investigate root cause (network, disk, memory)

---

## 4. Conflict Resolution Issues

### 4.1 Unexpected Winner

**Symptoms**:
- Wrong version after conflict resolution
- User reports "my changes were overwritten"

**Diagnosis**:
```sql
-- Check conflict log
SELECT * FROM CONFLICT_LOG 
WHERE SYNC_ID = 'xxx'
ORDER BY DETECTED_AT DESC;
```

**Common Causes**:
1. **Clock skew**: Source system has wrong time
2. **Timezone mismatch**: UTC vs local time
3. **Missing timestamp**: One record has no timestamp

**Resolution**:
1. Check timestamps in both versions:
   ```python
   print(f"Source timestamp: {conflict.source_timestamp}")
   print(f"Target timestamp: {conflict.target_timestamp}")
   ```
2. If clock skew detected:
   ```bash
   # Sync system clocks (Windows)
   w32tm /resync
   ```
3. Configure explicit timestamp column:
   ```python
   resolver = ConflictResolver(timestamp_column="updated_at")
   ```

### 4.2 Too Many Conflicts

**Symptoms**:
- High conflict count in dashboard
- Alert: "Frequent conflicts detected"

**Diagnosis**:
- Check if same data being modified in both platforms
- Review sync timing vs. user activity patterns

**Common Causes**:
1. **Sync during active hours**: Users editing while sync runs
2. **Bidirectional loop**: Changes syncing back and forth
3. **Auto-generated timestamps**: Every sync looks like a change

**Resolution**:
1. Schedule syncs during off-hours
2. Implement sync direction locking:
   ```python
   if record.sync_source == "fabric" and sync_direction == "snowflake_to_fabric":
       skip  # Already synced from Fabric, don't sync back
   ```
3. Exclude auto-generated columns from conflict detection

---

## 5. Performance Issues

### 5.1 Slow Syncs

**Symptoms**:
- Sync latency > 5 minutes
- Dashboard shows high p95 latency

**Diagnosis**:
```python
# Check sync timing
GET /api/files/{sync_id}/sync-status
# Review "duration_ms" field
```

**Common Causes**:
1. **Large data volume**: > 100K rows
2. **Wide tables**: > 100 columns
3. **Complex types**: Nested JSON, arrays
4. **Row-by-row inserts**: Not batched

**Resolution**:
1. For large data, use bulk loading:
   ```python
   # Instead of row-by-row INSERT
   # Use COPY INTO from staged parquet
   cursor.execute(f"""
       COPY INTO "{table_name}"
       FROM @stage/{sync_id}/
       FILE_FORMAT = (TYPE = PARQUET)
   """)
   ```
2. Increase parallelism:
   ```python
   with ThreadPoolExecutor(max_workers=4) as executor:
       # Split data into chunks
   ```
3. Consider incremental sync (only changed rows)

### 5.2 Retry Queue Growing

**Symptoms**:
- Retry queue size > 100
- Same failures repeating

**Diagnosis**:
```python
GET /api/retry-queue
# Check error patterns
```

**Resolution**:
1. If permanent errors dominating:
   - Clear those from queue manually
   - Fix root cause before re-adding
2. If transient errors:
   - Check platform health
   - Increase retry delay
3. Process queue manually:
   ```bash
   python -c "
   from sync_orchestration.sync_engine import SyncOrchestrator
   orch = SyncOrchestrator()
   orch.retry_orchestrator.process_queue(orch.retry_failed_sync)
   "
   ```

---

## 6. Common Error Messages

| Error | Meaning | Action |
|-------|---------|--------|
| "SYNC_ID already exists" | Duplicate sync attempt | Safe - operation was idempotent |
| "Table does not exist" | Target table dropped | Re-run sync to recreate |
| "Insufficient privileges" | Permission issue | Check Snowflake grants |
| "Token expired" | Auth token stale | Service will auto-refresh |
| "Rate limit exceeded" | Too many API calls | Implement backoff |
| "Warehouse is suspended" | Snowflake compute stopped | Resume warehouse |

---

## 7. Emergency Procedures

### 7.1 Stop All Syncs Immediately

```bash
# Stop the sync service
# Windows
taskkill /F /IM python.exe /FI "WINDOWTITLE eq *production_sync_api*"

# Or if running as service
net stop SyncService
```

### 7.2 Rollback a Bad Sync

```python
from sync_orchestration.sync_engine import SyncOrchestrator

orch = SyncOrchestrator()

# Rollback specific sync
result = orch.rollback_sync("sync_id_here")
print(result)

# The rollback will:
# 1. Delete rows with that SYNC_ID from target
# 2. Mark manifest as ROLLBACK
# 3. Log the action
```

### 7.3 Clear All Pending Retries

```python
from sync_orchestration.retry_orchestrator import RetryOrchestrator

retry = RetryOrchestrator()
retry.clear_queue()  # Clear all
# OR
retry.clear_queue(sync_id="specific_id")  # Clear specific
```

---

## 8. Log Locations

| Log | Location | Purpose |
|-----|----------|---------|
| Sync operations | `semantic_sync.log` | Detailed sync logs |
| API requests | `flask.log` or stdout | HTTP request logs |
| Audit trail | `sync_data/audit_trail.json` | Action audit |
| Migration | `migration_*.log` | Migration runs |
| Errors | `sync_data/errors/` | Captured exceptions |

---

## 9. Health Check Commands

```bash
# Check API health
curl http://localhost:5050/api/health

# Check dashboard stats
curl http://localhost:5050/api/sync/dashboard

# Check retry queue
curl http://localhost:5050/api/retry-queue

# Check conflicts
curl http://localhost:5050/api/conflicts
```

---

## 10. Escalation Path

1. **L1 (Self-Service)**: Use this guide
2. **L2 (Dev Team)**: If issue persists > 30 minutes
3. **L3 (Platform Team)**: For Fabric/Snowflake infrastructure issues
4. **Critical**: Page on-call for checksum mismatches or > 5% failure rate
