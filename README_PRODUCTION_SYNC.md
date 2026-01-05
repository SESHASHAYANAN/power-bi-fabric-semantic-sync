# Power BI Fabric ↔ Snowflake Semantic Model Synchronization

## 🎯 Project Overview

This system provides **production-ready, automated, bidirectional synchronization** between Microsoft Power BI Fabric semantic models and Snowflake views. It handles:

- ✅ **Reserved Keyword Protection**: Automatically sanitizes names like "day" that conflict with Snowflake keywords
- ✅ **Continuous Automation**: Scheduler runs sync every 60 seconds (configurable)
- ✅ **Smart Change Detection**: Only syncs when changes are detected
- ✅ **Robust Error Handling**: Exponential backoff retry (10s, 20s, 40s)
- ✅ **Partial Failure Handling**: One failed model doesn't stop the entire sync
- ✅ **Comprehensive Audit Trail**: Color-coded console + JSONL file logging

---

## 📁 File Structure

```
API connector/
├── fabric_snowflake_sync.py    # Main sync engine (updated with all integrations)
├── naming_convention.py         # Smart identifier sanitization
├── scheduler.py                 # Continuous sync automation
├── change_detector.py           # Change detection with hash computation
├── logging_audit.py             # Audit logging system
├── sync_state.json              # Last successful sync state (auto-generated)
├── sync_audit.jsonl             # Audit trail (auto-generated)
├── sync_results.json            # Last sync results (auto-generated)
└── .env                         # Configuration (credentials)
```

---

## 🚀 Quick Start

### One-Time Sync
```bash
python fabric_snowflake_sync.py
```

### Continuous Sync Mode (Scheduler)
```bash
# Default: 60 second interval
python fabric_snowflake_sync.py --scheduler

# Custom interval: 120 seconds
python fabric_snowflake_sync.py --scheduler --interval 120

# Specify sync direction
python fabric_snowflake_sync.py --scheduler --direction fabric_to_snowflake
```

---

## 🔧 Module Documentation

### 1. naming_convention.py - Smart Sanitization

**Problem Solved**: The model named "day" failed to sync because "day" is a Snowflake reserved keyword.

**Solution**: The `NamingConvention` class automatically detects and prefixes reserved keywords.

```python
from naming_convention import NamingConvention, IdentifierType

# Reserved keyword handling
NamingConvention.sanitize_name("day", IdentifierType.VIEW)
# Returns: "DIM_DAY"

# Standard view naming
from naming_convention import generate_semantic_view_name
generate_semantic_view_name("SalesModel", "DimCustomer")
# Returns: "SV_FABRIC_SALESMODEL_DIMCUSTOMER"

# Check if name is reserved
NamingConvention.is_reserved_keyword("user")  # True
```

**Reserved Keywords Handled**:
- Date/Time: `DAY`, `MONTH`, `YEAR`, `HOUR`, `WEEK`, `QUARTER`, `DATE`, `TIME`
- SQL: `TABLE`, `USER`, `GROUP`, `SELECT`, `FROM`, `WHERE`, etc.
- Data Types: `INTEGER`, `VARCHAR`, `BOOLEAN`, etc.
- Functions: `COUNT`, `SUM`, `AVG`, etc.

---

### 2. scheduler.py - Continuous Automation

**Problem Solved**: The system was "Stopped" and required manual intervention.

**Solution**: The `SyncScheduler` provides a heartbeat loop that runs sync at configurable intervals.

```python
from scheduler import create_scheduler, SyncScheduler

# Create scheduler
scheduler = create_scheduler(
    sync_function=my_sync_function,
    interval_seconds=60,      # Run every 60 seconds
    max_retries=3,            # Retry up to 3 times
    initial_retry_delay=10.0  # 10s, 20s, 40s backoff
)

# Start continuous sync
scheduler.start()

# Check status
print(scheduler.get_status())
print(scheduler.get_health())

# Stop gracefully
scheduler.stop()
```

**Features**:
- ⏰ Configurable interval (default: 60s)
- 🔄 Exponential backoff retry: 10s → 20s → 40s
- 🛡️ Graceful shutdown on Ctrl+C / SIGTERM
- 📊 Health monitoring and statistics
- ⚡ Trigger immediate sync: `scheduler.trigger_sync_now()`

---

### 3. change_detector.py - Smart Change Detection

**Problem Solved**: Full sync runs even when nothing has changed, wasting resources.

**Solution**: Compare current metadata hash against last successful sync.

```python
from change_detector import SmartChangeDetector, ChangeDetector

# For scheduler integration
detector = SmartChangeDetector(fabric_client, snowflake_connector)

# Check if sync is needed
needs_sync, reason = detector.check_for_changes()
if needs_sync:
    # Run sync
    run_sync()
    # Mark complete
    detector.mark_sync_complete()
else:
    print(f"Skipping sync: {reason}")
```

**Features**:
- 🔍 SHA256 hash of entire metadata
- 💾 State persistence in `sync_state.json`
- ⏭️ Skip sync when no changes detected
- 📈 Bidirectional change comparison

---

### 4. logging_audit.py - Comprehensive Audit Logging

**Problem Solved**: No visibility into what happened and when.

**Solution**: Color-coded console output + structured JSONL file logging.

```python
from logging_audit import get_audit_logger, EventType, Severity

audit = get_audit_logger()

# Log events
audit.sync_start("sync_001")
audit.view_created("SV_FABRIC_SALES_DIM", model_name="Sales", table_name="Dim")
audit.error("Failed to create view", exception, model_name="Sales")
audit.sync_end(success=True, stats={"views_created": 5})

# Performance tracking
with audit.track_operation("create_view"):
    create_the_view()
```

**Console Output** (color-coded):
```
✅ [2026-01-04 21:30:00] INFO     | Starting sync operation: sync_001
✅ [2026-01-04 21:30:05] INFO     | Created semantic view: SV_FABRIC_SALES_DIM
⚠️ [2026-01-04 21:30:06] WARNING  | Reserved keyword 'day' sanitized to 'DIM_DAY'
❌ [2026-01-04 21:30:10] ERROR    | Failed to sync table Users: Connection timeout
```

**JSONL Output** (sync_audit.jsonl):
```json
{"timestamp": "2026-01-04T21:30:00", "event_type": "SYNC_START", "severity": "INFO", ...}
{"timestamp": "2026-01-04T21:30:05", "event_type": "VIEW_CREATED", "view_name": "SV_FABRIC_SALES_DIM", ...}
```

---

## 🔄 Retry Logic

When a sync fails (network blip, API timeout), the system retries with exponential backoff:

| Attempt | Delay |
|---------|-------|
| 1       | 10s   |
| 2       | 20s   |
| 3       | 40s   |
| 4       | Give up |

```python
from scheduler import RetryConfig

config = RetryConfig(
    max_retries=3,
    initial_delay_seconds=10.0,
    backoff_multiplier=2.0,
    max_delay_seconds=300.0  # Cap at 5 minutes
)
```

---

## 🛡️ Partial Failure Handling

If syncing 5 models and 1 fails:
- ✅ Continue with the 4 successful ones
- 📝 Log the failure clearly
- 📊 Report partial success in results

```python
from scheduler import PartialSyncResult

result = PartialSyncResult()
for model in models:
    try:
        sync_model(model)
        result.add_success(model.name, table.name, view_name)
    except Exception as e:
        result.add_failure(model.name, table.name, e)
        continue  # Don't stop!

result.finalize()
print(f"Completed: {result.success_count}/{result.total_count}")
```

---

## 📊 View Naming Convention

All synced views follow the format:

```
SV_FABRIC_{MODEL_NAME}_{TABLE_NAME}
```

Examples:
| Model Name | Table Name | View Name |
|------------|------------|-----------|
| Sales      | DimCustomer | SV_FABRIC_SALES_DIMCUSTOMER |
| Inventory  | FactOrders | SV_FABRIC_INVENTORY_FACTORDERS |
| day        | data       | SV_FABRIC_DIM_DAY_TBL_DATA |

---

## 🔧 Configuration

Set credentials in `.env`:

```env
# Microsoft Fabric
FABRIC_TENANT_ID=your-tenant-id
FABRIC_CLIENT_ID=your-client-id
FABRIC_CLIENT_SECRET=your-client-secret
FABRIC_WORKSPACE_ID=your-workspace-id

# Snowflake
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
```

---

## 📈 Monitoring

### Get Scheduler Status
```python
scheduler.get_status()
# {
#     "state": "RUNNING",
#     "interval_seconds": 60,
#     "stats": {
#         "total_runs": 100,
#         "successful_runs": 95,
#         "failed_runs": 5,
#         "skipped_runs": 20,
#         "success_rate": 95.0
#     }
# }
```

### Get Recent Errors
```python
audit = get_audit_logger()
errors = audit.get_errors(count=10)
```

### Check Health
```python
health = scheduler.get_health()
# {"healthy": True, "issues": [], "failure_rate": 0.05}
```

---

## ✅ Summary of Fixes

| Issue | Solution | Module |
|-------|----------|--------|
| "day" reserved keyword crash | Auto-prefix DIM_DAY | naming_convention.py |
| Manual intervention required | 60s heartbeat scheduler | scheduler.py |
| No change detection | Hash comparison | change_detector.py |
| Network blips crash | 10s/20s/40s retry | scheduler.py |
| 1 failure stops all | Partial failure handling | scheduler.py |
| No audit trail | JSONL + color console | logging_audit.py |

---

## 🚀 Production Deployment

```bash
# Run as background service
nohup python fabric_snowflake_sync.py --scheduler --interval 60 &

# Or use a process manager
pm2 start fabric_snowflake_sync.py --interpreter python -- --scheduler
```

---

*Last Updated: 2026-01-04*
