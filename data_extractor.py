"""
Data Extractor and Loader Module for Fabric to Snowflake Sync

This module handles the extraction of actual row-level data from Microsoft Fabric 
semantic models and loading it into Snowflake tables. This is the critical piece
that was missing - previously only metadata was synced, not actual business data.

Key Features:
- Extracts actual data rows from Fabric semantic models using DAX queries
- Handles pagination for large datasets (1000+ rows)
- Preserves data types during extraction
- Loads data into existing Snowflake tables using INSERT/MERGE
- Supports full refresh (TRUNCATE + reload) and incremental sync
- Comprehensive logging with row counts
"""

import os
import json
import logging
import time
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

import requests
import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("data_extractor")
logger.setLevel(logging.DEBUG)

# Add handlers if not already added
if not logger.handlers:
    file_handler = logging.FileHandler("data_extraction.log", mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


class SyncMode(Enum):
    """Sync mode enumeration."""
    FULL_REFRESH = "full_refresh"  # TRUNCATE and reload all data
    INCREMENTAL = "incremental"    # MERGE with unique keys
    APPEND_ONLY = "append_only"    # Only INSERT new records


@dataclass
class ExtractionResult:
    """Result of a data extraction operation."""
    model_name: str
    table_name: str
    rows_extracted: int
    columns: List[str]
    data: List[Dict[str, Any]]
    extraction_time_ms: float
    success: bool
    error_message: str = ""
    
    def __repr__(self):
        return (f"ExtractionResult(model='{self.model_name}', table='{self.table_name}', "
                f"rows={self.rows_extracted}, success={self.success})")


@dataclass  
class LoadResult:
    """Result of a data loading operation."""
    table_name: str
    rows_loaded: int
    rows_updated: int
    rows_deleted: int
    load_time_ms: float
    success: bool
    error_message: str = ""
    
    def __repr__(self):
        return (f"LoadResult(table='{self.table_name}', loaded={self.rows_loaded}, "
                f"success={self.success})")


@dataclass
class SyncStats:
    """Statistics for a complete sync operation."""
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    models_processed: int = 0
    tables_processed: int = 0
    total_rows_extracted: int = 0
    total_rows_loaded: int = 0
    extraction_successes: int = 0
    extraction_failures: int = 0
    load_successes: int = 0
    load_failures: int = 0
    extractions: List[ExtractionResult] = field(default_factory=list)
    loads: List[LoadResult] = field(default_factory=list)
    
    def finalize(self):
        """Finalize stats after sync completes."""
        self.end_time = datetime.now()
        
    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.now() - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "models_processed": self.models_processed,
            "tables_processed": self.tables_processed,
            "total_rows_extracted": self.total_rows_extracted,
            "total_rows_loaded": self.total_rows_loaded,
            "extraction_successes": self.extraction_successes,
            "extraction_failures": self.extraction_failures,
            "load_successes": self.load_successes,
            "load_failures": self.load_failures,
        }


class FabricDataExtractor:
    """
    Extracts actual row-level data from Microsoft Fabric semantic models.
    
    This class uses the Fabric REST API to execute DAX queries against
    semantic models and retrieve the actual data rows, not just metadata.
    """
    
    # Batch size for pagination
    BATCH_SIZE = 1000
    MAX_ROWS_PER_TABLE = 100000  # Safety limit
    
    def __init__(self):
        """Initialize the Fabric data extractor."""
        self.tenant_id: str = os.getenv("FABRIC_TENANT_ID", "")
        self.client_id: str = os.getenv("FABRIC_CLIENT_ID", "")
        self.client_secret: str = os.getenv("FABRIC_CLIENT_SECRET", "")
        self.workspace_id: str = os.getenv("FABRIC_WORKSPACE_ID", "")
        
        self.base_url: str = "https://api.fabric.microsoft.com"
        self.powerbi_url: str = "https://api.powerbi.com"
        self.auth_url: str = (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        )
        
        self.access_token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None
        
        self.session: requests.Session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        
        self.max_retries: int = 3
        self.retry_delay: float = 2.0
        self.timeout: int = 60
        
        logger.info("FabricDataExtractor initialized")
    
    def authenticate(self) -> bool:
        """Authenticate with Azure AD to obtain access token."""
        logger.info("🔐 Authenticating with Fabric API for data extraction...")
        
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        }
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.post(
                    self.auth_url,
                    data=payload,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=self.timeout,
                )
                
                if response.status_code == 200:
                    token_data = response.json()
                    self.access_token = token_data.get("access_token")
                    expires_in = token_data.get("expires_in", 3600)
                    
                    from datetime import timedelta
                    self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)
                    
                    logger.info("✅ Fabric authentication successful for data extraction")
                    return True
                    
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                    
                else:
                    logger.error(
                        f"Authentication failed: [{response.status_code}] {response.text}"
                    )
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Authentication request failed: {e}")
                time.sleep(self.retry_delay * (attempt + 1))
                
        logger.error("❌ Authentication failed after all retries")
        return False
    
    def _ensure_token(self) -> bool:
        """Ensure a valid access token is available."""
        if self.access_token is None:
            return self.authenticate()
        
        if self.token_expiry is None or datetime.now() >= self.token_expiry:
            logger.info("Token expired, re-authenticating...")
            return self.authenticate()
        
        return True
    
    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers for API requests."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
    
    def execute_dax_query(
        self, 
        dataset_id: str, 
        dax_query: str,
        max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        Execute a DAX query against a Fabric semantic model.
        
        This is the key method that retrieves actual data rows from Fabric.
        
        Args:
            dataset_id: The semantic model (dataset) ID.
            dax_query: The DAX query to execute.
            max_retries: Number of retries on failure.
            
        Returns:
            Query result dictionary with rows and columns, or None on error.
        """
        if not self._ensure_token():
            return None
        
        # Use Power BI API for DAX queries
        url = f"{self.powerbi_url}/v1.0/myorg/datasets/{dataset_id}/executeQueries"
        
        payload = {
            "queries": [
                {
                    "query": dax_query
                }
            ],
            "serializerSettings": {
                "includeNulls": True
            }
        }
        
        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    url,
                    headers=self._get_headers(),
                    json=payload,
                    timeout=self.timeout * 2,  # Longer timeout for data queries
                )
                
                if response.status_code == 200:
                    return response.json()
                    
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 30))
                    logger.warning(f"Rate limited on DAX query. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                    
                else:
                    logger.error(
                        f"DAX query failed: [{response.status_code}] {response.text[:500]}"
                    )
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"DAX query request failed: {e}")
                time.sleep(self.retry_delay * (attempt + 1))
        
        return None
    
    def get_table_columns(
        self, 
        dataset_id: str, 
        table_name: str
    ) -> List[Dict[str, str]]:
        """
        Get column information for a table using DMV query.
        
        Args:
            dataset_id: The semantic model ID.
            table_name: The table name to get columns for.
            
        Returns:
            List of column dictionaries with name and data type.
        """
        # DAX query to get column info from model metadata
        dax_query = f"""
        EVALUATE
        SELECTCOLUMNS(
            INFO.COLUMNS(),
            "TableName", [TableName],
            "ColumnName", [Name],
            "DataType", [DataType]
        )
        """
        
        result = self.execute_dax_query(dataset_id, dax_query)
        
        if result and "results" in result:
            try:
                rows = result["results"][0]["tables"][0]["rows"]
                columns = [
                    {
                        "name": row.get("[ColumnName]", ""),
                        "data_type": row.get("[DataType]", "string")
                    }
                    for row in rows
                    if row.get("[TableName]", "").lower() == table_name.lower()
                ]
                return columns
            except (IndexError, KeyError):
                pass
        
        return []
    
    def get_table_row_count(
        self, 
        dataset_id: str, 
        table_name: str
    ) -> int:
        """
        Get the row count for a table.
        
        Args:
            dataset_id: The semantic model ID.
            table_name: Table name to count rows for.
            
        Returns:
            Row count, or -1 on error.
        """
        # Sanitize table name for DAX
        safe_table = table_name.replace("'", "''")
        
        dax_query = f"""
        EVALUATE
        ROW("RowCount", COUNTROWS('{safe_table}'))
        """
        
        result = self.execute_dax_query(dataset_id, dax_query)
        
        if result and "results" in result:
            try:
                rows = result["results"][0]["tables"][0]["rows"]
                if rows:
                    return int(rows[0].get("[RowCount]", 0))
            except (IndexError, KeyError, ValueError):
                pass
        
        return -1
    
    def extract_table_data(
        self,
        dataset_id: str,
        model_name: str,
        table_name: str,
        columns: Optional[List[str]] = None,
        batch_size: int = 1000,
        max_rows: Optional[int] = None
    ) -> ExtractionResult:
        """
        Extract all data from a table in a Fabric semantic model.
        
        This method handles pagination for large datasets and preserves data types.
        
        Args:
            dataset_id: The semantic model (dataset) ID.
            model_name: Name of the model (for logging).
            table_name: Name of the table to extract.
            columns: Optional list of specific columns to extract. If None, extracts all.
            batch_size: Number of rows per batch for pagination.
            max_rows: Maximum rows to extract. None = no limit (up to MAX_ROWS_PER_TABLE).
            
        Returns:
            ExtractionResult with the extracted data.
        """
        start_time = time.time()
        logger.info(f"📊 Extracting data from {model_name}.{table_name}...")
        
        # Get row count first
        total_rows = self.get_table_row_count(dataset_id, table_name)
        if total_rows > 0:
            logger.info(f"   Table has {total_rows} rows")
        
        # Build column list for DAX query
        if columns:
            column_list = ", ".join([f"'{table_name}'[{col}]" for col in columns])
        else:
            column_list = ""  # Will use SELECTCOLUMNS with all columns
        
        # Sanitize table name for DAX
        safe_table = table_name.replace("'", "''")
        
        all_rows: List[Dict[str, Any]] = []
        extracted_columns: List[str] = []
        
        # Determine effective max rows
        effective_max = min(
            max_rows or self.MAX_ROWS_PER_TABLE,
            self.MAX_ROWS_PER_TABLE
        )
        
        # Generate DAX query - use TOPN for pagination
        offset = 0
        has_more = True
        
        while has_more and len(all_rows) < effective_max:
            current_batch_size = min(batch_size, effective_max - len(all_rows))
            
            # DAX query with TOPN for pagination
            # Note: DAX doesn't have a direct OFFSET, so we use TOPNSKIP or ORDER BY + TOPN
            if column_list:
                dax_query = f"""
                EVALUATE
                TOPN({current_batch_size}, 
                    SELECTCOLUMNS(
                        '{safe_table}',
                        {column_list}
                    )
                )
                """
            else:
                # Get all columns
                dax_query = f"""
                EVALUATE
                TOPN({current_batch_size + offset}, '{safe_table}')
                """
            
            result = self.execute_dax_query(dataset_id, dax_query)
            
            if not result or "results" not in result:
                if len(all_rows) == 0:
                    # First batch failed - report error
                    return ExtractionResult(
                        model_name=model_name,
                        table_name=table_name,
                        rows_extracted=0,
                        columns=[],
                        data=[],
                        extraction_time_ms=(time.time() - start_time) * 1000,
                        success=False,
                        error_message="Failed to execute DAX query"
                    )
                else:
                    # Subsequent batch failed - return what we have
                    break
            
            try:
                table_data = result["results"][0]["tables"][0]
                rows = table_data.get("rows", [])
                
                # Get column names from first batch
                if offset == 0 and rows:
                    # Clean up column names (remove DAX table prefix and brackets)
                    # DAX returns columns like "Table[Column]" - we want just "COLUMN"
                    extracted_columns = [
                        self._clean_dax_column_name(col) 
                        for col in rows[0].keys()
                    ]
                
                # Process rows
                batch_rows = []
                for row in rows[offset:] if offset > 0 else rows:
                    clean_row = {}
                    is_header_row = False
                    
                    for key, value in row.items():
                        # Clean column name using the same method
                        clean_key = self._clean_dax_column_name(key)
                        # Convert value types
                        converted_value = self._convert_value(value)
                        clean_row[clean_key] = converted_value
                        
                        # Check if this value looks like a header (matches column name patterns)
                        # This happens when DAX returns header rows as data
                        if isinstance(converted_value, str):
                            # Check if value looks like a column name/header
                            str_val = str(converted_value).strip().upper()
                            # If value matches column name or is a common header pattern, flag as header
                            if str_val in [clean_key, clean_key.replace('_', ''), clean_key.replace('_', ' ')]:
                                is_header_row = True
                            # Check for common header patterns like 'id', 'ID', 'column1', etc.
                            elif str_val.lower() in ['id', 'name', 'date', 'value', 'column', 'employeeid', 
                                                       'column1', 'column2', 'column3', 'column4', 'column5',
                                                       'column6', 'column7', 'col1', 'col2', 'col3']:
                                is_header_row = True
                    
                    # Skip header rows - they contain column names as values
                    if is_header_row:
                        logger.debug(f"   Skipping header row: {list(clean_row.values())[:3]}...")
                        continue
                        
                    batch_rows.append(clean_row)
                
                if not batch_rows:
                    has_more = False
                else:
                    all_rows.extend(batch_rows)
                    offset += len(batch_rows)
                    
                    if len(batch_rows) < current_batch_size:
                        has_more = False
                    
                    logger.debug(f"   Extracted {len(all_rows)} rows so far...")
                    
            except (IndexError, KeyError) as e:
                logger.error(f"Error parsing DAX result: {e}")
                if len(all_rows) == 0:
                    return ExtractionResult(
                        model_name=model_name,
                        table_name=table_name,
                        rows_extracted=0,
                        columns=[],
                        data=[],
                        extraction_time_ms=(time.time() - start_time) * 1000,
                        success=False,
                        error_message=f"Error parsing result: {e}"
                    )
                break
            
            # Safety: Break if we've fetched too much
            if len(all_rows) >= effective_max:
                logger.warning(f"   Reached max rows limit ({effective_max})")
                has_more = False
        
        duration_ms = (time.time() - start_time) * 1000
        
        logger.info(
            f"   ✅ Extracted {len(all_rows)} rows from {table_name} "
            f"in {duration_ms:.0f}ms"
        )
        
        return ExtractionResult(
            model_name=model_name,
            table_name=table_name,
            rows_extracted=len(all_rows),
            columns=extracted_columns,
            data=all_rows,
            extraction_time_ms=duration_ms,
            success=True
        )
    
    def _convert_value(self, value: Any) -> Any:
        """
        Convert a value from DAX result to Python type.
        
        Preserves data types during extraction.
        """
        if value is None:
            return None
        
        if isinstance(value, (int, float, bool)):
            return value
        
        if isinstance(value, str):
            # Try to parse dates
            if len(value) >= 10 and value[4:5] == "-" and value[7:8] == "-":
                try:
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    pass
            
            # Try to parse numbers
            try:
                if "." in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                pass
            
            return value
        
        return str(value)
    
    def _clean_dax_column_name(self, dax_column: str) -> str:
        """
        Clean a DAX column name to get just the column name.
        
        DAX returns column names in formats like:
        - "Table[ColumnName]" 
        - "Sales[Revenue]"
        - "[ColumnName]"
        
        This extracts just "ColumnName" and makes it safe for Snowflake.
        
        Args:
            dax_column: The DAX-formatted column name.
            
        Returns:
            Clean column name suitable for Snowflake.
        """
        import re
        
        # Handle formats like "Table[Column]" or "[Column]"
        match = re.search(r'\[([^\]]+)\]', dax_column)
        if match:
            col_name = match.group(1)
        else:
            col_name = dax_column
        
        # Make safe for Snowflake: uppercase, replace special chars
        safe_name = col_name.upper().replace(" ", "_").replace("-", "_").replace(".", "_")
        
        # Remove any remaining invalid characters
        safe_name = re.sub(r'[^A-Z0-9_]', '', safe_name)
        
        # Ensure it doesn't start with a number
        if safe_name and safe_name[0].isdigit():
            safe_name = f"COL_{safe_name}"
        
        return safe_name or "UNKNOWN_COL"


class SnowflakeDataLoader:
    """
    Loads data into Snowflake tables.
    
    Handles INSERT, TRUNCATE+INSERT, and MERGE operations for loading
    data extracted from Fabric into existing Snowflake tables.
    """
    
    def __init__(self):
        """Initialize the Snowflake data loader."""
        self.account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
        self.user: str = os.getenv("SNOWFLAKE_USER", "")
        self.password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
        self.warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "")
        self.database: str = os.getenv("SNOWFLAKE_DATABASE", "")
        self.schema: str = os.getenv("SNOWFLAKE_SCHEMA", "")
        
        self.connection: Optional[snowflake.connector.SnowflakeConnection] = None
        
        logger.info("SnowflakeDataLoader initialized")
    
    def connect(self) -> bool:
        """Establish connection to Snowflake."""
        logger.info(f"🔌 Connecting to Snowflake: {self.account}...")
        
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )
            logger.info(f"✅ Connected to Snowflake: {self.account}")
            return True
            
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"❌ Snowflake connection failed: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close the Snowflake connection."""
        if self.connection is not None:
            try:
                self.connection.close()
                logger.info("🔌 Disconnected from Snowflake")
            except Exception as e:
                logger.warning(f"Error closing Snowflake connection: {e}")
            finally:
                self.connection = None
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get the row count for a Snowflake table.
        
        Args:
            table_name: Name of the table.
            
        Returns:
            Row count, or -1 if table doesn't exist or error.
        """
        if not self.connection:
            return -1
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{table_name}")
            result = cursor.fetchone()
            cursor.close()
            return result[0] if result else 0
        except Exception as e:
            logger.debug(f"Could not get row count for {table_name}: {e}")
            return -1
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in Snowflake."""
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) FROM {self.database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{self.schema}'
                AND TABLE_NAME = '{table_name.upper()}'
            """)
            result = cursor.fetchone()
            cursor.close()
            return result[0] > 0 if result else False
        except Exception:
            return False
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate a table (delete all rows).
        
        Args:
            table_name: Name of the table to truncate.
            
        Returns:
            True if successful.
        """
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"TRUNCATE TABLE {self.schema}.{table_name}")
            cursor.close()
            logger.info(f"   Truncated table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to truncate {table_name}: {e}")
            return False
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get the column names of a Snowflake table.
        
        Args:
            table_name: Name of the table.
            
        Returns:
            List of column names (uppercase).
        """
        if not self.connection:
            return []
        
        try:
            cursor = self.connection.cursor(DictCursor)
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{self.schema}'
                AND TABLE_NAME = '{table_name.upper()}'
                ORDER BY ORDINAL_POSITION
            """)
            results = cursor.fetchall()
            cursor.close()
            return [row["COLUMN_NAME"] for row in results]
        except Exception as e:
            logger.error(f"Failed to get columns for {table_name}: {e}")
            return []
    
    def columns_match(self, table_name: str, data_columns: List[str]) -> bool:
        """
        Check if existing table columns match the data columns.
        
        Args:
            table_name: Snowflake table name.
            data_columns: Column names from the extracted data.
            
        Returns:
            True if columns match, False otherwise.
        """
        existing_columns = set(c.upper() for c in self.get_table_columns(table_name))
        data_cols_upper = set(c.upper() for c in data_columns)
        
        # Check if data columns are a subset of table columns
        # (table may have extra columns but must have all data columns)
        if data_cols_upper <= existing_columns:
            return True
        
        missing = data_cols_upper - existing_columns
        logger.warning(f"   Table {table_name} is missing columns: {missing}")
        return False
    
    def recreate_table_for_data(
        self, 
        table_name: str, 
        columns: List[str], 
        sample_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Drop and recreate a table with columns based on extracted data.
        
        This is needed when the table was created with synthetic/placeholder
        columns that don't match the actual Fabric data.
        
        Args:
            table_name: Table name to recreate.
            columns: List of column names.
            sample_data: Optional sample row to infer data types.
            
        Returns:
            True if table was successfully recreated.
        """
        if not self.connection:
            return False
        
        logger.info(f"   🔄 Recreating table {table_name} with actual columns...")
        
        try:
            cursor = self.connection.cursor()
            
            # Drop existing table
            cursor.execute(f"DROP TABLE IF EXISTS {self.schema}.{table_name}")
            
            # Build column definitions - use VARCHAR(16777216) as default
            # In future, could infer types from sample_data
            col_defs = []
            for col in columns:
                safe_col = col.upper().replace(" ", "_").replace("-", "_")
                # Reserved word handling
                if safe_col in {"DATE", "DAY", "MONTH", "YEAR", "TIME", "TABLE", "VALUE", "KEY", "ORDER", "GROUP", "SELECT", "FROM", "WHERE"}:
                    safe_col = f'"{safe_col}"'
                
                # Try to infer type from sample data
                col_type = "VARCHAR(16777216)"
                if sample_data and col in sample_data:
                    val = sample_data[col]
                    if isinstance(val, bool):
                        col_type = "BOOLEAN"
                    elif isinstance(val, int):
                        col_type = "NUMBER(19,0)"
                    elif isinstance(val, float):
                        col_type = "FLOAT"
                    elif isinstance(val, datetime):
                        col_type = "TIMESTAMP_NTZ"
                    elif isinstance(val, date):
                        col_type = "DATE"
                
                col_defs.append(f"{safe_col} {col_type}")
            
            create_sql = f"""
                CREATE TABLE {self.schema}.{table_name} (
                    {", ".join(col_defs)}
                )
            """
            cursor.execute(create_sql)
            cursor.close()
            
            logger.info(f"   ✅ Recreated table {table_name} with {len(columns)} columns")
            return True
            
        except Exception as e:
            logger.error(f"Failed to recreate table {table_name}: {e}")
            return False

    
    def load_data(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        columns: List[str],
        mode: SyncMode = SyncMode.FULL_REFRESH,
        unique_key: Optional[str] = None,
        batch_size: int = 1000
    ) -> LoadResult:
        """
        Load data into a Snowflake table.
        
        Args:
            table_name: Target table name.
            data: List of dictionaries with the data to load.
            columns: List of column names.
            mode: Sync mode (FULL_REFRESH, INCREMENTAL, APPEND_ONLY).
            unique_key: Column name of the unique key for MERGE operations.
            batch_size: Number of rows per batch insert.
            
        Returns:
            LoadResult with the operation results.
        """
        start_time = time.time()
        logger.info(f"📥 Loading {len(data)} rows into {table_name}...")
        
        if not self.connection:
            return LoadResult(
                table_name=table_name,
                rows_loaded=0,
                rows_updated=0,
                rows_deleted=0,
                load_time_ms=0,
                success=False,
                error_message="Not connected to Snowflake"
            )
        
        if not data:
            return LoadResult(
                table_name=table_name,
                rows_loaded=0,
                rows_updated=0,
                rows_deleted=0,
                load_time_ms=(time.time() - start_time) * 1000,
                success=True,
                error_message="No data to load"
            )
        
        # Check if table exists
        if not self.table_exists(table_name):
            # Table doesn't exist - create it with the data columns
            logger.info(f"   Table {table_name} doesn't exist, creating...")
            sample_row = data[0] if data else None
            if not self.recreate_table_for_data(table_name, columns, sample_row):
                return LoadResult(
                    table_name=table_name,
                    rows_loaded=0,
                    rows_updated=0,
                    rows_deleted=0,
                    load_time_ms=(time.time() - start_time) * 1000,
                    success=False,
                    error_message=f"Failed to create table {table_name}"
                )
        else:
            # Table exists - check if columns match
            if not self.columns_match(table_name, columns):
                logger.info(f"   Column mismatch detected, recreating table {table_name}...")
                sample_row = data[0] if data else None
                if not self.recreate_table_for_data(table_name, columns, sample_row):
                    return LoadResult(
                        table_name=table_name,
                        rows_loaded=0,
                        rows_updated=0,
                        rows_deleted=0,
                        load_time_ms=(time.time() - start_time) * 1000,
                        success=False,
                        error_message=f"Failed to recreate table {table_name} with matching columns"
                    )
        
        try:
            # Handle different sync modes
            if mode == SyncMode.FULL_REFRESH:
                return self._load_full_refresh(
                    table_name, data, columns, batch_size, start_time
                )
            elif mode == SyncMode.INCREMENTAL and unique_key:
                return self._load_incremental(
                    table_name, data, columns, unique_key, batch_size, start_time
                )
            else:
                return self._load_append(
                    table_name, data, columns, batch_size, start_time
                )
                
        except Exception as e:
            logger.error(f"Failed to load data into {table_name}: {e}")
            return LoadResult(
                table_name=table_name,
                rows_loaded=0,
                rows_updated=0,
                rows_deleted=0,
                load_time_ms=(time.time() - start_time) * 1000,
                success=False,
                error_message=str(e)
            )
    
    def _load_full_refresh(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        columns: List[str],
        batch_size: int,
        start_time: float
    ) -> LoadResult:
        """Load data using TRUNCATE + INSERT (full refresh)."""
        # Truncate the table first
        if not self.truncate_table(table_name):
            return LoadResult(
                table_name=table_name,
                rows_loaded=0,
                rows_updated=0,
                rows_deleted=0,
                load_time_ms=(time.time() - start_time) * 1000,
                success=False,
                error_message="Failed to truncate table"
            )
        
        # Insert all data
        rows_loaded = self._batch_insert(table_name, data, columns, batch_size)
        
        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            f"   ✅ Loaded {rows_loaded} rows into {table_name} "
            f"(full refresh) in {duration_ms:.0f}ms"
        )
        
        return LoadResult(
            table_name=table_name,
            rows_loaded=rows_loaded,
            rows_updated=0,
            rows_deleted=0,
            load_time_ms=duration_ms,
            success=rows_loaded == len(data)
        )
    
    def _load_incremental(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        columns: List[str],
        unique_key: str,
        batch_size: int,
        start_time: float
    ) -> LoadResult:
        """Load data using MERGE (incremental sync)."""
        if not self.connection:
            return LoadResult(
                table_name=table_name,
                rows_loaded=0,
                rows_updated=0,
                rows_deleted=0,
                load_time_ms=0,
                success=False
            )
        
        # Create temp table
        temp_table = f"TEMP_{table_name}_{int(time.time())}"
        
        try:
            cursor = self.connection.cursor()
            
            # Create temp table like target
            cursor.execute(f"""
                CREATE TEMPORARY TABLE {temp_table} LIKE {self.schema}.{table_name}
            """)
            
            # Insert data into temp table
            self._batch_insert(temp_table, data, columns, batch_size, is_temp=True)
            
            # Perform MERGE
            update_cols = [c for c in columns if c.upper() != unique_key.upper()]
            update_clause = ", ".join([
                f"target.{c} = source.{c}" for c in update_cols
            ])
            insert_cols = ", ".join(columns)
            source_cols = ", ".join([f"source.{c}" for c in columns])
            
            merge_sql = f"""
                MERGE INTO {self.schema}.{table_name} target
                USING {temp_table} source
                ON target.{unique_key} = source.{unique_key}
                WHEN MATCHED THEN UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({source_cols})
            """
            
            cursor.execute(merge_sql)
            merge_result = cursor.fetchone()
            
            rows_updated = merge_result[0] if merge_result else 0
            
            # Drop temp table
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
            cursor.close()
            
            duration_ms = (time.time() - start_time) * 1000
            logger.info(
                f"   ✅ Merged {len(data)} rows into {table_name} "
                f"(incremental) in {duration_ms:.0f}ms"
            )
            
            return LoadResult(
                table_name=table_name,
                rows_loaded=len(data),
                rows_updated=rows_updated,
                rows_deleted=0,
                load_time_ms=duration_ms,
                success=True
            )
            
        except Exception as e:
            logger.error(f"MERGE failed: {e}")
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except:
                pass
            return LoadResult(
                table_name=table_name,
                rows_loaded=0,
                rows_updated=0,
                rows_deleted=0,
                load_time_ms=(time.time() - start_time) * 1000,
                success=False,
                error_message=str(e)
            )
    
    def _load_append(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        columns: List[str],
        batch_size: int,
        start_time: float
    ) -> LoadResult:
        """Load data using INSERT only (append mode)."""
        rows_loaded = self._batch_insert(table_name, data, columns, batch_size)
        
        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            f"   ✅ Appended {rows_loaded} rows to {table_name} "
            f"in {duration_ms:.0f}ms"
        )
        
        return LoadResult(
            table_name=table_name,
            rows_loaded=rows_loaded,
            rows_updated=0,
            rows_deleted=0,
            load_time_ms=duration_ms,
            success=rows_loaded == len(data)
        )
    
    def _batch_insert(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        columns: List[str],
        batch_size: int,
        is_temp: bool = False
    ) -> int:
        """
        Insert data in batches.
        
        Returns:
            Number of rows successfully inserted.
        """
        if not self.connection or not data:
            return 0
        
        target_table = table_name if is_temp else f"{self.schema}.{table_name}"
        
        # Sanitize column names
        safe_columns = [f'"{c.upper()}"' for c in columns]
        columns_sql = ", ".join(safe_columns)
        
        # Create placeholders
        placeholders = ", ".join(["%s"] * len(columns))
        
        insert_sql = f"INSERT INTO {target_table} ({columns_sql}) VALUES ({placeholders})"
        
        cursor = self.connection.cursor()
        total_inserted = 0
        
        # Process in batches
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            # Prepare rows
            rows = []
            for row in batch:
                row_values = []
                for col in columns:
                    value = row.get(col)
                    # Convert Python types to Snowflake-compatible
                    if isinstance(value, datetime):
                        value = value.strftime("%Y-%m-%d %H:%M:%S")
                    elif isinstance(value, date):
                        value = value.strftime("%Y-%m-%d")
                    elif isinstance(value, Decimal):
                        value = float(value)
                    row_values.append(value)
                rows.append(tuple(row_values))
            
            try:
                cursor.executemany(insert_sql, rows)
                total_inserted += len(rows)
            except Exception as e:
                logger.error(f"Batch insert failed: {e}")
                # Continue with next batch
        
        cursor.close()
        return total_inserted


class DataSyncOrchestrator:
    """
    Orchestrates the complete data sync process from Fabric to Snowflake.
    
    This is the main class that coordinates extraction from Fabric and
    loading into Snowflake, with comprehensive logging and error handling.
    """
    
    def __init__(
        self,
        sync_mode: SyncMode = SyncMode.FULL_REFRESH,
        batch_size: int = 1000
    ):
        """
        Initialize the data sync orchestrator.
        
        Args:
            sync_mode: Default sync mode for all tables.
            batch_size: Default batch size for data operations.
        """
        self.extractor = FabricDataExtractor()
        self.loader = SnowflakeDataLoader()
        self.sync_mode = sync_mode
        self.batch_size = batch_size
        self.stats = SyncStats()
        
        logger.info(
            f"DataSyncOrchestrator initialized with mode={sync_mode.value}, "
            f"batch_size={batch_size}"
        )
    
    def sync_model_data(
        self,
        model_id: str,
        model_name: str,
        tables: List[Dict[str, Any]],
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Sync all data from a Fabric model to Snowflake.
        
        Args:
            model_id: The Fabric semantic model ID.
            model_name: Name of the model (for logging/table naming).
            tables: List of table definitions with name and column info.
            force: If True, reload data even if row counts match.
            
        Returns:
            Dictionary with sync results for this model.
        """
        logger.info(f"🔄 Syncing data for model: {model_name}")
        
        model_results = {
            "model_name": model_name,
            "model_id": model_id,
            "tables_synced": 0,
            "tables_failed": 0,
            "total_rows_extracted": 0,
            "total_rows_loaded": 0,
            "table_results": []
        }
        
        for table in tables:
            table_name = table.get("name", "")
            if not table_name:
                continue
            
            # Generate Snowflake table name
            snowflake_table = f"TBL_FABRIC_{model_name}_{table_name}".upper()
            snowflake_table = snowflake_table.replace(" ", "_").replace("-", "_")
            # Remove any double underscores
            import re
            snowflake_table = re.sub(r'_+', '_', snowflake_table)
            snowflake_table = snowflake_table.strip('_')
            
            logger.info(f"   Processing table: {table_name} -> {snowflake_table}")
            
            table_needs_creation = not self.loader.table_exists(snowflake_table)
            if table_needs_creation:
                logger.info(f"   📋 Table {snowflake_table} does not exist, will create after extracting data")
            
            # Get current row counts for comparison
            fabric_row_count = self.extractor.get_table_row_count(model_id, table_name)
            snowflake_row_count = self.loader.get_table_row_count(snowflake_table) if not table_needs_creation else 0
            
            logger.info(
                f"   Row counts - Fabric: {fabric_row_count}, "
                f"Snowflake: {snowflake_row_count}"
            )
            
            # Determine if sync is needed
            needs_sync = force or table_needs_creation  # Always sync if table doesn't exist
            sync_reason = "Force sync requested" if force else "Table creation required"
            
            if not force and not table_needs_creation:
                if snowflake_row_count == 0:
                    needs_sync = True
                    sync_reason = "Snowflake table is empty"
                elif snowflake_row_count == -1:
                    needs_sync = True
                    sync_reason = "Could not verify Snowflake table state"
                elif fabric_row_count > 0 and fabric_row_count != snowflake_row_count:
                    needs_sync = True
                    sync_reason = f"Row count mismatch (Fabric: {fabric_row_count}, Snowflake: {snowflake_row_count})"
                elif fabric_row_count == snowflake_row_count and snowflake_row_count > 0:
                    needs_sync = False
                    sync_reason = f"Row counts match ({snowflake_row_count})"
                elif fabric_row_count <= 0:
                    # Fabric table might be empty or unreachable, try to sync anyway
                    needs_sync = True
                    sync_reason = "Fabric row count unknown, syncing to verify"
            
            if not needs_sync:
                logger.info(f"   ⏭️ Skipping: {sync_reason}")
                continue
            
            logger.info(f"   🔄 Syncing: {sync_reason}")
            
            # Extract data from Fabric
            extraction_result = self.extractor.extract_table_data(
                dataset_id=model_id,
                model_name=model_name,
                table_name=table_name,
                columns=table.get("columns"),
                batch_size=self.batch_size
            )
            
            self.stats.extractions.append(extraction_result)
            
            if not extraction_result.success:
                logger.error(
                    f"   ❌ Extraction failed: {extraction_result.error_message}"
                )
                self.stats.extraction_failures += 1
                model_results["tables_failed"] += 1
                continue
            
            self.stats.extraction_successes += 1
            self.stats.total_rows_extracted += extraction_result.rows_extracted
            model_results["total_rows_extracted"] += extraction_result.rows_extracted
            
            # Load data into Snowflake
            load_result = self.loader.load_data(
                table_name=snowflake_table,
                data=extraction_result.data,
                columns=extraction_result.columns,
                mode=self.sync_mode,
                batch_size=self.batch_size
            )
            
            self.stats.loads.append(load_result)
            
            if not load_result.success:
                logger.error(f"   ❌ Load failed: {load_result.error_message}")
                self.stats.load_failures += 1
                model_results["tables_failed"] += 1
            else:
                self.stats.load_successes += 1
                self.stats.total_rows_loaded += load_result.rows_loaded
                model_results["total_rows_loaded"] += load_result.rows_loaded
                model_results["tables_synced"] += 1
            
            # Add table result
            model_results["table_results"].append({
                "fabric_table": table_name,
                "snowflake_table": snowflake_table,
                "rows_extracted": extraction_result.rows_extracted,
                "rows_loaded": load_result.rows_loaded,
                "success": extraction_result.success and load_result.success,
                "sync_reason": sync_reason
            })
        
        self.stats.models_processed += 1
        self.stats.tables_processed += len(tables)
        
        return model_results
    
    def run_full_sync(
        self,
        models: List[Dict[str, Any]],
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Run a full data sync for all models.
        
        Args:
            models: List of model dictionaries with id, name, and tables.
            force: Force sync even if row counts match.
            
        Returns:
            Complete sync results.
        """
        logger.info("=" * 80)
        logger.info("DATA SYNC: Extracting data from Fabric and loading to Snowflake")
        logger.info("=" * 80)
        
        self.stats = SyncStats()  # Reset stats
        
        # Connect to both systems
        if not self.extractor.authenticate():
            return {
                "status": "FAILED",
                "error": "Failed to authenticate with Fabric",
                "stats": self.stats.to_dict()
            }
        
        if not self.loader.connect():
            return {
                "status": "FAILED", 
                "error": "Failed to connect to Snowflake",
                "stats": self.stats.to_dict()
            }
        
        all_results = []
        
        try:
            for model in models:
                model_id = model.get("id", "")
                model_name = model.get("name", model.get("displayName", "Unknown"))
                tables = model.get("tables", [])
                
                if not model_id or not tables:
                    logger.warning(f"Skipping model {model_name}: No ID or tables")
                    continue
                
                result = self.sync_model_data(
                    model_id=model_id,
                    model_name=model_name,
                    tables=tables,
                    force=force
                )
                all_results.append(result)
            
            self.stats.finalize()
            
            # Generate summary
            status = "SUCCESS"
            if self.stats.load_failures > 0:
                status = "PARTIAL_SUCCESS" if self.stats.load_successes > 0 else "FAILED"
            
            return {
                "status": status,
                "stats": self.stats.to_dict(),
                "model_results": all_results,
                "summary": {
                    "models_processed": self.stats.models_processed,
                    "tables_processed": self.stats.tables_processed,
                    "total_rows_extracted": self.stats.total_rows_extracted,
                    "total_rows_loaded": self.stats.total_rows_loaded,
                    "extraction_successes": self.stats.extraction_successes,
                    "extraction_failures": self.stats.extraction_failures,
                    "load_successes": self.stats.load_successes,
                    "load_failures": self.stats.load_failures,
                }
            }
            
        finally:
            self.loader.disconnect()
    
    def validate_sync(self) -> Dict[str, Any]:
        """
        Validate the sync by checking row counts and sample data.
        
        Returns:
            Validation results with row count comparisons.
        """
        logger.info("=" * 80)
        logger.info("VALIDATION: Checking sync results")
        logger.info("=" * 80)
        
        validation_results = {
            "timestamp": datetime.now().isoformat(),
            "tables": [],
            "all_valid": True
        }
        
        if not self.loader.connect():
            return {
                "status": "FAILED",
                "error": "Could not connect to Snowflake for validation"
            }
        
        try:
            # Get all TBL_FABRIC tables
            cursor = self.loader.connection.cursor(DictCursor)
            cursor.execute(f"""
                SELECT TABLE_NAME 
                FROM {self.loader.database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{self.loader.schema}'
                AND TABLE_NAME LIKE 'TBL_FABRIC_%'
            """)
            tables = cursor.fetchall()
            cursor.close()
            
            for table_row in tables:
                table_name = table_row.get("TABLE_NAME", "")
                row_count = self.loader.get_table_row_count(table_name)
                
                # Get sample data
                sample_data = []
                try:
                    cursor = self.loader.connection.cursor(DictCursor)
                    cursor.execute(
                        f"SELECT * FROM {self.loader.schema}.{table_name} LIMIT 5"
                    )
                    sample_data = cursor.fetchall()
                    cursor.close()
                except Exception as e:
                    logger.warning(f"Could not get sample data for {table_name}: {e}")
                
                is_valid = row_count > 0
                if not is_valid:
                    validation_results["all_valid"] = False
                
                validation_results["tables"].append({
                    "table_name": table_name,
                    "row_count": row_count,
                    "has_data": row_count > 0,
                    "sample_rows": len(sample_data),
                    "valid": is_valid
                })
                
                status_icon = "✅" if is_valid else "❌"
                logger.info(
                    f"   {status_icon} {table_name}: {row_count} rows"
                )
            
        finally:
            self.loader.disconnect()
        
        return validation_results


# Main function for standalone execution
def main():
    """Main entry point for data extraction and loading."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fabric to Snowflake Data Extractor and Loader"
    )
    parser.add_argument(
        "--mode", "-m",
        choices=["full_refresh", "incremental", "append"],
        default="full_refresh",
        help="Sync mode (default: full_refresh)"
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force sync even if row counts match"
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate existing data, don't sync"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for data operations (default: 1000)"
    )
    
    args = parser.parse_args()
    
    # Map mode string to enum
    mode_map = {
        "full_refresh": SyncMode.FULL_REFRESH,
        "incremental": SyncMode.INCREMENTAL,
        "append": SyncMode.APPEND_ONLY,
    }
    sync_mode = mode_map.get(args.mode, SyncMode.FULL_REFRESH)
    
    orchestrator = DataSyncOrchestrator(
        sync_mode=sync_mode,
        batch_size=args.batch_size
    )
    
    if args.validate_only:
        results = orchestrator.validate_sync()
    else:
        # This would typically get models from the main sync engine
        # For standalone testing, we'll just validate
        logger.info("Use this module with fabric_snowflake_sync.py for full sync")
        logger.info("Running validation instead...")
        results = orchestrator.validate_sync()
    
    # Output results
    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)
    print(json.dumps(results, indent=2, default=str))
    
    # Save results
    with open("data_sync_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nResults saved to data_sync_results.json")


if __name__ == "__main__":
    main()
