# Snowflake-Fabric Connector Migration System

## 🎯 Overview

This comprehensive migration system transforms existing Snowflake and Microsoft Fabric connectors from view-based format to table-based format with bidirectional sync and DAX-to-SQL conversion.

### ✅ Expected Outcomes
- **All data outputs in both Snowflake and Fabric displayed as physical tables**
- **Zero DAX dependencies** - all logic converted to standard SQL
- **Automatic bidirectional sync** maintaining data consistency
- **All legacy files operational** with new architecture
- **Improved query performance** through materialized table format
- **Unified table structure** across both platforms

---

## 📦 Package Structure

```
migration/
├── __init__.py                    # Package initialization
├── view_to_table_converter.py     # Phase 1: View → Table conversion
├── dax_to_sql_translator.py       # Phase 2: DAX → SQL translation
├── bidirectional_sync_manager.py  # Phase 3: Sync configuration
├── backward_compatibility.py      # Phase 4: Legacy support
├── migration_orchestrator.py      # Master controller
└── run_migration.py               # CLI interface
```

---

## 🚀 Quick Start

### 1. Prerequisites

```bash
# Ensure dependencies are installed
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file with your credentials:

```env
# Microsoft Fabric Configuration
FABRIC_TENANT_ID=your-tenant-id
FABRIC_CLIENT_ID=your-client-id
FABRIC_CLIENT_SECRET=your-client-secret
FABRIC_WORKSPACE_ID=your-workspace-id

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account.region.cloud
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
```

### 3. Run Migration

#### Interactive Mode
```bash
cd migration
python run_migration.py --mode interactive
```

#### Full Migration (Batch)
```bash
python run_migration.py --full --scan-dir ../legacy_files
```

#### Dry Run (Analysis Only)
```bash
python run_migration.py --full --dry-run
```

---

## 📋 Migration Phases

### Phase 1: Schema Analysis & Mapping

Inventories all objects and creates the migration plan:
- Discovers all Snowflake views
- Lists all Fabric semantic models
- Extracts DAX measures and dependencies
- Generates view→table mapping document

```python
from migration import MigrationOrchestrator

orchestrator = MigrationOrchestrator(
    snowflake_connector=sf_connector,
    fabric_client=fabric_client
)

# Run analysis
result = orchestrator.run_analysis_phase()
print(f"Found {len(orchestrator.manifest.snowflake_views)} views to convert")
```

### Phase 2: View → Table Conversion

Converts all views to materialized tables:

```python
# With incremental refresh patterns
result = orchestrator.run_conversion_phase(
    incremental=True,
    parallel=4  # Parallel conversions
)
```

**Features:**
- `CREATE OR REPLACE TABLE` from views
- Incremental refresh with MERGE statements
- Clustering key optimization
- Automatic backup creation
- Primary/Foreign key preservation

**Snowflake DDL Generated:**
```sql
CREATE OR REPLACE TABLE ANALYTICS_DB.SEMANTIC_LAYER.SALES_FACT
CLUSTER BY (DATE_KEY, PRODUCT_KEY)
AS
SELECT 
    *,
    CURRENT_TIMESTAMP() as _SYNC_TIMESTAMP,
    MD5(OBJECT_CONSTRUCT(*)::VARCHAR) as _ROW_HASH
FROM ANALYTICS_DB.SEMANTIC_LAYER.SALES_FACT_VIEW
```

### Phase 3: DAX → SQL Translation

Translates all DAX measures to equivalent SQL:

```python
# Translate to Snowflake SQL
result = orchestrator.run_translation_phase(dialect='snowflake')

# Or to T-SQL for Fabric Warehouse
result = orchestrator.run_translation_phase(dialect='tsql')
```

**Supported DAX Functions:**

| DAX Function | SQL Equivalent |
|--------------|----------------|
| `SUMX(Table, Expression)` | `SUM(expression)` with JOIN |
| `CALCULATE(Measure, Filter)` | CTE with WHERE clause |
| `TOTALYTD(Measure, DateCol)` | Window function with PARTITION BY YEAR |
| `SAMEPERIODLASTYEAR(DateCol)` | `DATEADD(year, -1, date)` |
| `RELATED(Table[Column])` | LEFT JOIN to lookup table |
| `ALL(Column)` | Remove from GROUP BY |
| `VALUES(Column)` | `SELECT DISTINCT` |

**Example Translation:**
```dax
-- Original DAX
YTD Sales := 
TOTALYTD(
    SUM(Sales[Amount]),
    'Date'[Date]
)
```

```sql
-- Translated Snowflake SQL
SUM(Sales.Amount) OVER (
    PARTITION BY YEAR(Date.Date)
    ORDER BY Date.Date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

### Phase 4: Bidirectional Sync Setup

Configures automatic synchronization:

```python
result = orchestrator.run_sync_setup_phase(
    enable_cdc=True,           # Change Data Capture
    enable_mirroring=True,     # Fabric Mirroring
    sync_interval=15           # Minutes
)
```

**Components Created:**

1. **Snowflake CDC Streams** - Track changes on each table
2. **CDC Change Log Table** - Central change tracking
3. **Sync Stored Procedures:**
   - `SP_SYNC_TO_FABRIC` - Push changes to Fabric
   - `SP_SYNC_ROLLBACK` - Rollback failed syncs
   - `SP_SYNC_HEALTH_CHECK` - Monitor sync health
4. **Scheduled Tasks:**
   - `TASK_BIDIRECTIONAL_SYNC` - Runs every N minutes
   - `TASK_SYNC_HEALTH_CHECK` - Health monitoring
   - `TASK_RETRY_FAILED_SYNCS` - Retry failed records

### Phase 5: Backward Compatibility

Ensures existing workloads continue functioning:

```python
result = orchestrator.run_compatibility_phase(
    scan_directory='./legacy_files',
    create_wrappers=True,
    migrate_pbix=True
)
```

**Actions:**
- Creates compatibility view wrappers (deprecated after 6 months)
- Updates SQL files to reference new tables
- Migrates PBIX files with DAX to SQL annotations
- Updates Fabric notebooks

**View Wrapper Example:**
```sql
CREATE OR REPLACE VIEW SALES_FACT_COMPAT AS
SELECT * EXCLUDE (_SYNC_TIMESTAMP, _ROW_HASH, _IS_DELETED)
FROM SALES_FACT
WHERE COALESCE(_IS_DELETED, FALSE) = FALSE;

-- DEPRECATION NOTE: Use SALES_FACT table directly
```

### Phase 6: Testing & Validation

Validates the migration:

```python
result = orchestrator.run_validation_phase()

# Check specific queries
validation = compat_manager.validate_migration(
    old_query="SELECT * FROM OLD_VIEW",
    new_query="SELECT * FROM NEW_TABLE",
    snowflake_connector=connector
)
```

**Validation Checks:**
- Row count comparison
- Column structure matching
- Data integrity verification
- Sync mechanism testing

---

## 🔧 Component APIs

### ViewToTableConverter

```python
from migration import ViewToTableConverter

converter = ViewToTableConverter(
    snowflake_connector=sf_conn,
    fabric_client=fabric
)

# Discover views
views = converter.discover_snowflake_views(
    database='ANALYTICS_DB',
    schema='SEMANTIC_LAYER'
)

# Convert single view
result = converter.convert_snowflake_view_to_table(
    view=views[0],
    table_name='SALES_FACT',
    incremental=True,
    key_columns=['ID'],
    clustering_keys=['DATE_KEY']
)

# Convert all views
summary = converter.convert_all_views(
    platform='both',
    incremental=True
)
```

### DAXToSQLTranslator

```python
from migration import DAXToSQLTranslator

translator = DAXToSQLTranslator(dialect='snowflake')

# Translate single measure
result = translator.translate_measure(DAXMeasure(
    name='Total Sales',
    expression='SUMX(Sales, Sales[Qty] * Sales[Price])',
    table_name='Sales'
))

print(result.translated_sql)
# SUM(Sales.Qty * Sales.Price)

# Generate SQL views for all measures
sql = translator.generate_sql_view_definitions(
    measures=translations,
    base_table='SALES_FACT'
)
```

### BidirectionalSyncManager

```python
from migration import BidirectionalSyncManager, SyncConfiguration

sync = BidirectionalSyncManager(
    snowflake_connector=sf_conn,
    fabric_client=fabric,
    config=SyncConfiguration(
        direction=SyncDirection.BIDIRECTIONAL,
        mode=SyncMode.CDC,
        sync_interval_minutes=15
    )
)

# Setup CDC
sync.setup_snowflake_cdc_infrastructure()
sync.setup_snowflake_cdc(tables=['SALES_FACT', 'PRODUCT_DIM'])

# Run incremental sync
results = sync.run_incremental_sync()

# Get sync status
status = sync.get_sync_status()
```

### BackwardCompatibilityManager

```python
from migration import BackwardCompatibilityManager

compat = BackwardCompatibilityManager(
    view_to_table_map={'OLD_VIEW': 'NEW_TABLE'},
    dax_translator=translator
)

# Scan for legacy references
refs = compat.scan_directory_for_legacy_refs('./queries/')

# Update references
results = compat.batch_update_references('./queries/')

# Migrate PBIX file
result = compat.migrate_pbix_file(
    'reports/dashboard.pbix',
    convert_dax=True
)

# Validate migration
validation = compat.validate_migration(
    old_query="SELECT * FROM OLD_VIEW",
    new_query="SELECT * FROM NEW_TABLE",
    snowflake_connector=sf_conn
)
```

---

## 📊 Migration Reports

The system generates comprehensive reports at each phase:

### Analysis Report (`analysis_report.json`)
```json
{
  "migration_id": "MIG_20260104153000",
  "summary": {
    "snowflake_views": 25,
    "fabric_models": 10,
    "dax_measures": 150,
    "mappings_created": 35
  },
  "recommended_actions": [
    "Review view-to-table mappings before conversion",
    "Identify high-complexity DAX measures for manual review"
  ]
}
```

### Conversion Report (`conversion_report.json`)
```json
{
  "total_conversions": 25,
  "successful": 24,
  "failed": 1,
  "total_rows_converted": 5000000
}
```

### Translation Report (`translation_report.json`)
```json
{
  "total_translations": 150,
  "successful": 145,
  "failed": 5,
  "needs_review": 12,
  "average_confidence": 0.92
}
```

### Final Report (`final_report_*.json`)
```json
{
  "migration_id": "MIG_20260104153000",
  "status": "completed",
  "outcomes": {
    "all_data_as_tables": true,
    "zero_dax_dependencies": true,
    "bidirectional_sync_active": true,
    "legacy_files_operational": true,
    "validation_passed": true
  },
  "statistics": {
    "snowflake_views_converted": 25,
    "fabric_models_converted": 10,
    "dax_measures_translated": 145,
    "files_migrated": 50
  }
}
```

---

## 🔄 Rollback

The system maintains rollback points for safe recovery:

```bash
# Rollback via CLI
python run_migration.py --rollback --migration-id MIG_20260104153000
```

```python
# Rollback via API
result = orchestrator.rollback_migration(to_phase='conversion')
```

---

## 🗓️ Scheduled Sync Tasks

After setup, these Snowflake tasks run automatically:

| Task | Schedule | Purpose |
|------|----------|---------|
| `TASK_BIDIRECTIONAL_SYNC` | Every 15 min | Process CDC changes |
| `TASK_SYNC_HEALTH_CHECK` | Every 30 min | Monitor sync health |
| `TASK_RETRY_FAILED_SYNCS` | Hourly | Retry failed records |

Monitor via:
```sql
-- Check sync status
CALL SP_SYNC_HEALTH_CHECK();

-- View pending changes
SELECT SYNC_STATUS, COUNT(*) 
FROM CDC_TRACKING.CDC_CHANGE_LOG 
GROUP BY SYNC_STATUS;
```

---

## 🚨 Troubleshooting

### Common Issues

1. **Connection Failures**
   - Verify `.env` credentials
   - Check network connectivity
   - Ensure IP whitelisting

2. **DAX Translation Failures**
   - Complex measures marked for manual review
   - Check `translation_report.json` for details
   - Use simpler DAX patterns where possible

3. **Sync Conflicts**
   - Default: latest timestamp wins
   - Configure via `ConflictResolution` enum
   - Check `CDC_CHANGE_LOG` for failed syncs

4. **Performance Issues**
   - Use incremental refresh for large tables
   - Add clustering keys
   - Increase `parallel` workers for conversion

### Logs

Migration logs are saved to:
- `migration.log` - Main migration log
- `semantic_sync.log` - Sync operations log
- `migration_workspace/*.json` - Phase reports

---

## 📝 License

This migration system is part of the Snowflake-Fabric Sync project.
