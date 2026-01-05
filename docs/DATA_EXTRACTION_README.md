# Data Extraction and Loading Module

## Overview

This module provides the **critical missing functionality** for extracting actual row-level data from Microsoft Fabric semantic models and loading it into Snowflake tables.

### The Problem This Solves

Previously, the sync process only created the table structures (schema) in Snowflake, but the tables remained **empty** or contained only placeholder metadata. This module fixes that by:

1. **Extracting actual data rows** from Fabric semantic models using DAX queries
2. **Loading the data** into existing Snowflake tables
3. **Handling pagination** for large datasets (>1000 rows)
4. **Preserving data types** during extraction and loading

## Quick Start

### Compare Row Counts (Diagnostic)

First, check which tables need data sync:

```bash
python fabric_snowflake_sync.py --compare-rows
```

This will show:
- Tables with matching row counts (in sync)
- Tables with mismatched row counts (need sync)
- Empty Snowflake tables (need sync)

### Validate Existing Data

Check what data currently exists:

```bash
python fabric_snowflake_sync.py --validate-data
```

### Run Full Sync WITH Data

Sync both schema AND actual data:

```bash
python fabric_snowflake_sync.py --sync-data
```

Or with force mode to reload even if counts match:

```bash
python fabric_snowflake_sync.py --sync-data --force
```

## Command-Line Options

| Option | Description |
|--------|-------------|
| `--sync-data` / `--with-data` | Extract and load actual row-level data |
| `--compare-rows` | Compare row counts between Fabric and Snowflake |
| `--validate-data` | Validate existing data in Snowflake tables |
| `--data-mode` | Sync mode: `full_refresh`, `incremental`, or `append` |
| `--force` | Force sync even if row counts match |

### Data Sync Modes

1. **full_refresh** (default): TRUNCATE table and reload all data
2. **incremental**: Use MERGE to update existing and insert new rows
3. **append**: Only INSERT new rows (no updates)

Example:
```bash
python fabric_snowflake_sync.py --sync-data --data-mode incremental
```

## Architecture

### Data Flow

```
┌─────────────────────┐         ┌─────────────────────┐
│   Microsoft Fabric  │         │     Snowflake       │
│   Semantic Models   │         │     Database        │
├─────────────────────┤         ├─────────────────────┤
│  Sales Model        │────────→│ TBL_FABRIC_SALES_*  │
│  - Sales table      │  DAX    │                     │
│  - Customer table   │  Query  │ SV_FABRIC_SALES     │
│  - Product table    │         │ (view)              │
├─────────────────────┤         ├─────────────────────┤
│  Table Model        │────────→│ TBL_FABRIC_TABLE_*  │
│                     │         │                     │
├─────────────────────┤         ├─────────────────────┤
│  Day Model          │────────→│ TBL_FABRIC_DAY_DAY  │
│  (date dimension)   │         │                     │
│                     │         │ SV_FABRIC_DIM_DAY   │
│                     │         │ (view)              │
└─────────────────────┘         └─────────────────────┘
```

### Key Components

1. **FabricDataExtractor** (`data_extractor.py`)
   - Authenticates with Fabric API
   - Executes DAX queries to extract data
   - Handles pagination for large datasets
   - Preserves data types

2. **SnowflakeDataLoader** (`data_extractor.py`)
   - Connects to Snowflake
   - Loads data using INSERT/MERGE
   - Supports different sync modes
   - Batch processing for performance

3. **DataSyncOrchestrator** (`data_extractor.py`)
   - Coordinates extraction and loading
   - Compares row counts for smart sync
   - Handles errors gracefully
   - Provides comprehensive logging

## Understanding the Three Models

### Sales Model
- Contains: Sales transactions, customers, products
- Tables: TBL_FABRIC_SALES_SALES, TBL_FABRIC_SALES_CUSTOMER, etc.
- Views: SV_FABRIC_SALESANALYTICS

### Table Model
- Contains: General business data
- Tables: TBL_FABRIC_TABLE_* 
- Views: SV_FABRIC_TABLE

### Day Model (Date Dimension)
- Contains: Date dimension data
- Table: TBL_FABRIC_DAY_DAY
- View: SV_FABRIC_DIM_DAY
- Note: "day" is a reserved keyword, so it's sanitized to "DIM_DAY"

## Logging

All data sync operations are logged to:
- `semantic_sync.log` - Main sync log
- `data_extraction.log` - Data extraction details
- `data_sync_results.json` - JSON output of sync results

### Sample Log Output

```
================================================================================
DATA SYNC - Extracting Data from Fabric and Loading to Snowflake
================================================================================
Mode: full_refresh
Force: False
================================================================================
📊 Extracting Fabric semantic models...
   Found 3 models
   - Sales: 3 table(s)
   - Table: 1 table(s)
   - day: 1 table(s)

📥 Starting data extraction and loading...
📊 Extracting data from Sales.Sales...
   Table has 1500 rows
   ✅ Extracted 1500 rows from Sales in 2340ms
📥 Loading 1500 rows into TBL_FABRIC_SALES_SALES...
   Truncated table: TBL_FABRIC_SALES_SALES
   ✅ Loaded 1500 rows (full refresh) in 890ms

================================================================================
DATA SYNC RESULTS
================================================================================
Status: SUCCESS
Models Processed: 3
Tables Processed: 5
Rows Extracted: 8500
Rows Loaded: 8500
Extraction Successes: 5
Extraction Failures: 0
Load Successes: 5
Load Failures: 0
================================================================================
```

## Validation

After running data sync, verify success by:

1. **Check row counts**:
```sql
SELECT 'TBL_FABRIC_SALES_SALES' as table_name, COUNT(*) as rows FROM SEMANTIC_LAYER.TBL_FABRIC_SALES_SALES
UNION ALL
SELECT 'TBL_FABRIC_DAY_DAY', COUNT(*) FROM SEMANTIC_LAYER.TBL_FABRIC_DAY_DAY
-- etc.
```

2. **Sample data check**:
```sql
SELECT * FROM SEMANTIC_LAYER.SV_FABRIC_SALESANALYTICS LIMIT 10;
SELECT * FROM SEMANTIC_LAYER.SV_FABRIC_DIM_DAY LIMIT 10;
```

3. **Use the validation command**:
```bash
python fabric_snowflake_sync.py --validate-data
```

## Troubleshooting

### Empty Tables After Sync

1. Check if Fabric authentication is working:
   ```bash
   python fabric_snowflake_sync.py --compare-rows
   ```

2. Run with force and verbose mode:
   ```bash
   python fabric_snowflake_sync.py --sync-data --force --verbose
   ```

3. Check `data_extraction.log` for detailed errors

### DAX Query Failures

- The Fabric API may rate-limit requests
- Large tables may timeout - check batch size settings
- Ensure your Fabric workspace has the correct permissions

### Snowflake Load Failures

- Check table exists and has correct schema
- Verify Snowflake credentials in `.env`
- Check for data type mismatches

## API Reference

### Python API

```python
from fabric_snowflake_sync import run_data_sync, SemanticSyncEngine, SyncDirection
from data_extractor import DataSyncOrchestrator, SyncMode

# Run data sync directly
results = run_data_sync(force=True, sync_mode="full_refresh")

# Or use the sync engine
engine = SemanticSyncEngine(SyncDirection.FABRIC_TO_SNOWFLAKE)
models = engine.extract_fabric_models()
data_results = engine.sync_data_to_snowflake(models, force=True)

# Compare row counts
comparison = engine.compare_row_counts(models)
print(f"Tables needing sync: {len(comparison['needs_sync'])}")
```

### Standalone Orchestrator

```python
from data_extractor import DataSyncOrchestrator, SyncMode

orchestrator = DataSyncOrchestrator(
    sync_mode=SyncMode.FULL_REFRESH,
    batch_size=1000
)

# Validate existing data
validation = orchestrator.validate_sync()
print(f"All tables have data: {validation['all_valid']}")
```
