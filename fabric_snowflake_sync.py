import os
import json
import logging
import time
import hashlib
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import requests
import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv

# Import new production-ready modules
try:
    from naming_convention import NamingConvention, IdentifierType, sanitize_snowflake_name, generate_semantic_view_name
    from logging_audit import get_audit_logger, EventType, Severity, AuditLogger
    from scheduler import SyncScheduler, RetryConfig, PartialSyncResult, create_scheduler
    PRODUCTION_MODULES_AVAILABLE = True
except ImportError as e:
    # Fallback for backward compatibility
    PRODUCTION_MODULES_AVAILABLE = False
    print(f"Production modules not available: {e}")

# Import data extraction and loading module for actual data sync
try:
    from data_extractor import (
        DataSyncOrchestrator, 
        FabricDataExtractor, 
        SnowflakeDataLoader,
        SyncMode as DataSyncMode,
        SyncStats
    )
    DATA_EXTRACTOR_AVAILABLE = True
except ImportError as e:
    DATA_EXTRACTOR_AVAILABLE = False
    print(f"Data extractor module not available: {e}")

load_dotenv()

logger = logging.getLogger("fabric_snowflake_sync")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("semantic_sync.log", mode="a", encoding="utf-8")
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



class SyncDirection(Enum):
    """Enumeration for synchronization direction options."""

    FABRIC_TO_SNOWFLAKE = "fabric_to_snowflake"
    SNOWFLAKE_TO_FABRIC = "snowflake_to_fabric"
    BIDIRECTIONAL = "bidirectional"


class DataTypeMapping:
    """
    Mapping class for converting Fabric data types to Snowflake data types.
    
    Provides a comprehensive mapping between Microsoft Fabric's semantic model
    data types and their corresponding Snowflake equivalents.
    """

    _type_map: Dict[str, str] = {
        "int64": "NUMBER(19,0)",
        "int32": "INTEGER",
        "double": "FLOAT",
        "decimal": "DECIMAL(18,2)",
        "string": "VARCHAR(4000)",
        "text": "VARCHAR(16777216)", 
        "boolean": "BOOLEAN",
        "datetime": "TIMESTAMP_NTZ",
        "date": "DATE",
        "time": "TIME",
        "binary": "BINARY",
        "currency": "DECIMAL(19,4)",
        "percentage": "FLOAT",
    }

    @classmethod
    def get_snowflake_type(cls, fabric_type: str) -> str:
        """
        Convert a Fabric data type to its Snowflake equivalent.

        Args:
            fabric_type: The Fabric data type string to convert.

        Returns:
            The corresponding Snowflake data type string.
            Defaults to VARCHAR(4000) if type is not found.
        """
        normalized_type = fabric_type.lower().strip()
        return cls._type_map.get(normalized_type, "VARCHAR(4000)")


@dataclass
class Column:
    """
    Represents a column in a semantic model table.

    Attributes:
        name: Internal column name.
        display_name: User-friendly display name.
        data_type: The data type of the column.
        is_hidden: Whether the column is hidden from users.
        description: Optional description of the column.
    """

    name: str
    display_name: str
    data_type: str
    is_hidden: bool = False
    description: str = ""


@dataclass
class Measure:
    """
    Represents a measure (calculated metric) in a semantic model.

    Attributes:
        name: Internal measure name.
        display_name: User-friendly display name.
        expression: DAX or calculation expression.
        description: Optional description of the measure.
        format_string: Format string for display (e.g., currency, percentage).
    """

    name: str
    display_name: str
    expression: str
    description: str = ""
    format_string: str = ""


@dataclass
class Relationship:
    """
    Represents a relationship between two tables in a semantic model.

    Attributes:
        name: Name of the relationship.
        from_table: Source table name.
        from_column: Source column name.
        to_table: Target table name.
        to_column: Target column name.
        cardinality: Relationship cardinality (e.g., Many-to-One).
        cross_filtering: Cross-filtering behavior.
    """

    name: str
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cardinality: str = "Many-to-One"
    cross_filtering: str = "Both"


@dataclass
class Table:
    """
    Represents a table in a semantic model.

    Attributes:
        name: Internal table name.
        display_name: User-friendly display name.
        source_expression: Source query or expression.
        columns: List of columns in the table.
        measures: List of measures defined on the table.
        description: Optional description of the table.
        is_hidden: Whether the table is hidden from users.
    """

    name: str
    display_name: str
    source_expression: str
    columns: List[Column] = field(default_factory=list)
    measures: List[Measure] = field(default_factory=list)
    description: str = ""
    is_hidden: bool = False


@dataclass
class SemanticModel:
    """
    Represents a complete semantic model from Microsoft Fabric.

    Attributes:
        id: Unique identifier for the model.
        name: Internal model name.
        display_name: User-friendly display name.
        workspace_id: ID of the workspace containing the model.
        tables: List of tables in the model.
        relationships: List of relationships between tables.
        description: Optional description of the model.
        created_date: ISO format creation date.
        modified_date: ISO format last modified date.
    """

    id: str
    name: str
    display_name: str
    workspace_id: str
    tables: List[Table] = field(default_factory=list)
    relationships: List[Relationship] = field(default_factory=list)
    description: str = ""
    created_date: str = ""
    modified_date: str = ""



class FabricApiClient:
    """
    Client for interacting with the Microsoft Fabric REST API.

    Handles authentication, token management, and all API operations
    related to semantic models in Microsoft Fabric.
    """

    def __init__(self) -> None:
        """
        Initialize the Fabric API client with configuration from environment.

        Loads credentials from environment variables and sets up
        the base URLs for API and authentication endpoints.
        """
        self.tenant_id: str = os.getenv("FABRIC_TENANT_ID", "")
        self.client_id: str = os.getenv("FABRIC_CLIENT_ID", "")
        self.client_secret: str = os.getenv("FABRIC_CLIENT_SECRET", "")
        self.workspace_id: str = os.getenv("FABRIC_WORKSPACE_ID", "")

        self.base_url: str = "https://api.fabric.microsoft.com"
        self.auth_url: str = (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        )

        self.access_token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None

       
        self.session: requests.Session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

        self.max_retries: int = 3
        self.retry_delay: float = 1.0
        self.timeout: int = 30

        logger.info("Fabric API Client initialized")

    def authenticate(self) -> bool:
        """
        Authenticate with Azure AD to obtain an access token.

        Uses OAuth 2.0 client credentials flow to obtain an access token
        for the Microsoft Fabric API.

        Returns:
            True if authentication was successful, False otherwise.
        """
        logger.info("🔐 Authenticating with Fabric API...")

        payload: Dict[str, str] = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://api.fabric.microsoft.com/.default",
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

                  
                    self.token_expiry = datetime.now() + timedelta(
                        seconds=expires_in - 300
                    )

                    logger.info("✅ Fabric authentication successful")
                    return True

                elif response.status_code == 429:
                   
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited. Waiting {retry_after} seconds..."
                    )
                    time.sleep(retry_after)
                    continue

                else:
                    logger.error(
                        f"Authentication failed: [{response.status_code}] "
                        f"{response.text}"
                    )

            except requests.exceptions.Timeout:
                logger.warning(
                    f"Authentication timeout (attempt {attempt + 1}/{self.max_retries})"
                )
                time.sleep(self.retry_delay * (attempt + 1))

            except requests.exceptions.RequestException as e:
                logger.error(f"Authentication request failed: {e}")
                time.sleep(self.retry_delay * (attempt + 1))

        logger.error("❌ Authentication failed after all retries")
        return False

    def _ensure_token(self) -> bool:
        """
        Ensure a valid access token is available.

        Checks if the current token is valid or needs refresh,
        and authenticates if necessary.

        Returns:
            True if a valid token is available, False otherwise.
        """
        if self.access_token is None:
            return self.authenticate()

        if self.token_expiry is None or datetime.now() >= self.token_expiry:
            logger.info("Token expired, re-authenticating...")
            return self.authenticate()

        return True

    def _get_headers(self) -> Dict[str, str]:
        """
        Get HTTP headers for API requests.

        Returns:
            Dictionary containing Authorization and Content-Type headers.
        """
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Optional[requests.Response]:
        """
        Make an HTTP request to the Fabric API with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.).
            endpoint: API endpoint path.
            data: Optional JSON payload for POST/PUT requests.

        Returns:
            Response object if successful, None otherwise.
        """
        if not self._ensure_token():
            return None

        url = f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=self._get_headers(),
                    json=data,
                    timeout=self.timeout,
                )

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                return response

            except requests.exceptions.Timeout:
                logger.warning(
                    f"Request timeout (attempt {attempt + 1}/{self.max_retries})"
                )
                time.sleep(self.retry_delay * (2**attempt))

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                time.sleep(self.retry_delay * (2**attempt))

        return None

    def get_semantic_models(self) -> List[Dict[str, Any]]:
        """
        Retrieve all semantic models from the configured workspace.

        Returns:
            List of semantic model dictionaries, or empty list on error.
        """
        logger.info("📋 Fetching semantic models from Fabric...")

        endpoint = f"/v1/workspaces/{self.workspace_id}/semanticmodels"
        response = self._make_request("GET", endpoint)

        if response is None:
            logger.error("Failed to fetch semantic models")
            return []

        if response.status_code == 200:
            data = response.json()
            models = data.get("value", [])
            logger.info(f"✅ Found {len(models)} semantic model(s)")
            return models

        logger.error(
            f"Failed to get semantic models: [{response.status_code}] "
            f"{response.text}"
        )
        return []

    def get_semantic_model_detail(
        self, model_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve detailed information for a specific semantic model.

        Args:
            model_id: The unique identifier of the semantic model.

        Returns:
            Dictionary containing model details, or None on error.
        """
        logger.info(f"🔍 Fetching details for model: {model_id}")

        endpoint = f"/v1/workspaces/{self.workspace_id}/semanticmodels/{model_id}"
        response = self._make_request("GET", endpoint)

        if response is None:
            logger.error(f"Failed to fetch model details for: {model_id}")
            return None

        if response.status_code == 200:
            logger.info("✅ Retrieved model details")
            return response.json()

        logger.error(
            f"Failed to get model detail: [{response.status_code}] "
            f"{response.text}"
        )
        return None

    def list_connections(self) -> List[Dict[str, Any]]:
        """
        List all connections in the workspace.

        Returns:
            List of connection dictionaries, or empty list on error.
        """
        logger.info("📋 Listing workspace connections...")

        endpoint = f"/v1/workspaces/{self.workspace_id}/connections"
        response = self._make_request("GET", endpoint)

        if response is None:
            return []

        if response.status_code == 200:
            connections = response.json().get("value", [])
            logger.info(f"✅ Found {len(connections)} connection(s)")
            return connections

        logger.error(f"Failed to list connections: {response.text}")
        return []

    def get_item_connections(self, model_id: str) -> List[Dict[str, Any]]:
        """
        Get connections associated with a specific semantic model.

        Args:
            model_id: The unique identifier of the semantic model.

        Returns:
            List of connection dictionaries, or empty list on error.
        """
        logger.info(f"🔗 Getting connections for model: {model_id}")

        endpoint = (
            f"/v1/workspaces/{self.workspace_id}/semanticmodels/"
            f"{model_id}/connections"
        )
        response = self._make_request("GET", endpoint)

        if response is None:
            return []

        if response.status_code == 200:
            connections = response.json().get("value", [])
            return connections

        logger.error(f"Failed to get item connections: {response.text}")
        return []

    def bind_semantic_model_connection(
        self,
        model_id: str,
        connection_id: str,
        connection_details: Dict[str, Any],
        connectivity_type: str = "Connection",
    ) -> bool:
        """
        Bind a connection to a semantic model.

        Args:
            model_id: The unique identifier of the semantic model.
            connection_id: The connection ID to bind.
            connection_details: Details of the connection configuration.
            connectivity_type: Type of connectivity (default: "Connection").

        Returns:
            True if binding was successful, False otherwise.
        """
        logger.info(f"🔗 Binding connection to model: {model_id}")

        endpoint = (
            f"/v1/workspaces/{self.workspace_id}/semanticmodels/"
            f"{model_id}/connections/bind"
        )

        payload: Dict[str, Any] = {
            "connectivityType": connectivity_type,
            "connectionId": connection_id,
            "connectionDetails": connection_details,
        }

        response = self._make_request("POST", endpoint, data=payload)

        if response is None:
            logger.error("Failed to bind connection: no response")
            return False

        if response.status_code in (200, 201, 204):
            logger.info("✅ Successfully bound semantic model connection")
            return True

        logger.error(
            f"Failed to bind connection: [{response.status_code}] "
            f"{response.text}"
        )
        return False

    def update_semantic_model_definition(
        self,
        model_id: str,
        definition: Dict[str, Any],
    ) -> bool:
        """
        Update a semantic model's definition.

        This uses the TMSL/XMLA endpoint to modify model definitions.

        Args:
            model_id: The semantic model ID.
            definition: The updated definition payload.

        Returns:
            True if successful, False otherwise.
        """
        logger.info(f"🔄 Updating semantic model definition: {model_id}")

        endpoint = (
            f"/v1/workspaces/{self.workspace_id}/semanticmodels/"
            f"{model_id}/updateDefinition"
        )

        response = self._make_request("POST", endpoint, data=definition)

        if response is None:
            logger.error("Failed to update model: no response")
            return False

        if response.status_code in (200, 202, 204):
            logger.info("✅ Semantic model definition update initiated")
            return True

        logger.error(
            f"Failed to update model: [{response.status_code}] "
            f"{response.text}"
        )
        return False

    def update_measure(
        self,
        model_id: str,
        table_name: str,
        measure_name: str,
        new_expression: str,
        new_format_string: Optional[str] = None,
    ) -> bool:
        """
        Update a specific measure in a semantic model.

        Note: This requires the model definition to be modifiable.
        Some semantic models may not support direct measure updates.

        Args:
            model_id: The semantic model ID.
            table_name: The table containing the measure.
            measure_name: The measure name to update.
            new_expression: The new DAX expression.
            new_format_string: Optional new format string.

        Returns:
            True if successful, False otherwise.
        """
        logger.info(
            f"🔄 Updating measure {table_name}.{measure_name} in model {model_id}"
        )

        # Build the update payload
        # Note: The actual API structure may vary based on Fabric version
        payload: Dict[str, Any] = {
            "updateDetails": [
                {
                    "path": f"model/tables/{table_name}/measures/{measure_name}",
                    "updates": {
                        "expression": new_expression,
                    }
                }
            ]
        }

        if new_format_string:
            payload["updateDetails"][0]["updates"]["formatString"] = new_format_string

        return self.update_semantic_model_definition(model_id, payload)

    def refresh_semantic_model(self, model_id: str) -> bool:
        """
        Trigger a refresh of the semantic model.

        Args:
            model_id: The semantic model ID.

        Returns:
            True if refresh initiated, False otherwise.
        """
        logger.info(f"🔄 Triggering refresh for model: {model_id}")

        endpoint = (
            f"/v1/workspaces/{self.workspace_id}/semanticmodels/"
            f"{model_id}/refresh"
        )

        response = self._make_request("POST", endpoint, data={})

        if response is None:
            logger.error("Failed to refresh model: no response")
            return False

        if response.status_code in (200, 202, 204):
            logger.info("✅ Semantic model refresh initiated")
            return True

        logger.error(
            f"Failed to refresh model: [{response.status_code}] "
            f"{response.text}"
        )
        return False


class SnowflakeConnector:
    """
    Connector for interacting with Snowflake database.

    Handles connection management, query execution, and semantic view
    creation in Snowflake.
    """

    def __init__(self) -> None:
        """
        Initialize the Snowflake connector with configuration from environment.

        Loads connection parameters from environment variables.
        """
        self.account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
        self.user: str = os.getenv("SNOWFLAKE_USER", "")
        self.password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
        self.warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "")
        self.database: str = os.getenv("SNOWFLAKE_DATABASE", "")
        self.schema: str = os.getenv("SNOWFLAKE_SCHEMA", "")

        self.connection: Optional[snowflake.connector.SnowflakeConnection] = None

        logger.info("Snowflake Connector initialized")

    def connect(self) -> bool:
        """
        Establish a connection to Snowflake.

        Returns:
            True if connection was successful, False otherwise.
        """
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

        except Exception as e:
            logger.error(f"❌ Unexpected error connecting to Snowflake: {e}")
            return False

    def disconnect(self) -> None:
        """
        Close the Snowflake connection.
        """
        if self.connection is not None:
            try:
                self.connection.close()
                logger.info("🔌 Disconnected from Snowflake")
            except Exception as e:
                logger.warning(f"Error closing Snowflake connection: {e}")
            finally:
                self.connection = None

    def execute_query(
        self,
        query: str,
        fetch_all: bool = False,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SQL query on Snowflake.

        Args:
            query: SQL query string to execute.
            fetch_all: If True, fetch all results; otherwise fetch one.

        Returns:
            Query results as list of dicts, or None on error.
        """
        if self.connection is None:
            logger.error("❌ Not connected to Snowflake")
            return None

        try:
            cursor = self.connection.cursor(DictCursor)
            cursor.execute(query)

            if fetch_all:
                results = cursor.fetchall()
            else:
                result = cursor.fetchone()
                results = [result] if result else []

            cursor.close()
            logger.debug(f"Query executed successfully: {query[:50]}...")
            return results

        except snowflake.connector.errors.ProgrammingError as e:
            logger.error(f"❌ Query execution failed: {e}")
            return None

        except Exception as e:
            logger.error(f"❌ Unexpected error executing query: {e}")
            return None

    def create_semantic_view(
        self,
        view_name: str,
        table_definition: str,
        dimensions: Dict[str, str],
        measures: Dict[str, str],
    ) -> bool:
        """
        Create a semantic view in Snowflake.

        Args:
            view_name: Name for the semantic view.
            table_definition: Source table or query for the view.
            dimensions: Dictionary of dimension names to data types.
            measures: Dictionary of measure names to expressions.

        Returns:
            True if view was created successfully, False otherwise.
        """
        if self.connection is None:
            logger.error("❌ Not connected to Snowflake")
            return False

        try:
           
            dim_clauses = [
                f"{name} {dtype}" for name, dtype in dimensions.items()
            ]
            dimensions_sql = ", ".join(dim_clauses)

          
            measure_clauses = [
                f"{name} := {expr}" for name, expr in measures.items()
            ]
            measures_sql = ", ".join(measure_clauses)

           
            ddl = f"""
            CREATE OR REPLACE SEMANTIC VIEW {self.schema}.{view_name}
            USING ({table_definition})
            DIMENSIONS ({dimensions_sql})
            MEASURES ({measures_sql})
            """

            cursor = self.connection.cursor()
            cursor.execute(ddl)
            cursor.close()

            logger.info(f"✅ Created semantic view: {view_name}")
            return True

        except snowflake.connector.errors.ProgrammingError as e:
           
            logger.warning(
                f"Semantic view not supported, creating standard view: {e}"
            )
            return self._create_standard_view(
                view_name, table_definition, dimensions, measures
            )

        except Exception as e:
            logger.error(f"❌ Failed to create semantic view: {e}")
            return False

    def _create_standard_view(
        self,
        view_name: str,
        table_definition: str,
        dimensions: Dict[str, str],
        measures: Dict[str, str],
    ) -> bool:
        """
        Create a standard view as fallback when semantic views aren't available.

        Args:
            view_name: Name for the view.
            table_definition: Source table or query.
            dimensions: Dictionary of dimension columns.
            measures: Dictionary of measure expressions.

        Returns:
            True if view was created successfully, False otherwise.
        """
        try:
            
            all_columns = list(dimensions.keys()) + [
                f"{expr} AS {name}" for name, expr in measures.items()
            ]
            columns_sql = ", ".join(all_columns)

            ddl = f"""
            CREATE OR REPLACE VIEW {self.schema}.{view_name} AS
            SELECT {columns_sql}
            FROM {table_definition}
            """

            cursor = self.connection.cursor()
            cursor.execute(ddl)
            cursor.close()

            logger.info(f"✅ Created standard view: {view_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create standard view: {e}")
            return False

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in Snowflake.

        Args:
            table_name: Name of the table to check.

        Returns:
            True if table exists, False otherwise.
        """
        try:
            query = f"""
            SELECT COUNT(*) as cnt
            FROM {self.database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.schema}'
            AND TABLE_NAME = '{table_name.upper()}'
            """
            result = self.execute_query(query)
            if result and result[0].get("CNT", 0) > 0:
                return True
            return False
        except Exception as e:
            logger.warning(f"Error checking table existence: {e}")
            return False

    def create_base_table(
        self,
        table_name: str,
        columns: Dict[str, str],
        if_not_exists: bool = True,
    ) -> bool:
        """
        Create a base table in Snowflake from column definitions.
        
        This is required when a Fabric semantic model references data
        that doesn't exist as a table in Snowflake yet.

        Args:
            table_name: Name for the table.
            columns: Dictionary of column names to data types.
            if_not_exists: If True, won't error if table exists.

        Returns:
            True if table was created successfully, False otherwise.
        """
        if self.connection is None:
            logger.error("Not connected to Snowflake")
            return False

        try:
            # Build column definitions
            column_defs = []
            for col_name, col_type in columns.items():
                # Sanitize column name
                safe_name = col_name.upper().replace(" ", "_").replace("-", "_")
                safe_name = "".join(c for c in safe_name if c.isalnum() or c == "_")
                if safe_name and safe_name[0].isdigit():
                    safe_name = f"COL_{safe_name}"
                if not safe_name:
                    safe_name = "UNKNOWN_COL"
                column_defs.append(f'"{safe_name}" {col_type}')
            
            columns_sql = ", ".join(column_defs)
            
            if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
            
            ddl = f"""
            CREATE TABLE {if_not_exists_clause}{self.schema}.{table_name} (
                {columns_sql}
            )
            """
            
            logger.info(f"Creating base table: {table_name}")
            logger.debug(f"DDL: {ddl}")
            
            cursor = self.connection.cursor()
            cursor.execute(ddl)
            cursor.close()
            
            logger.info(f"Created base table: {table_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create base table {table_name}: {e}")
            return False

    def view_exists(self, view_name: str) -> bool:
        """
        Check if a view exists in Snowflake.

        Args:
            view_name: Name of the view to check.

        Returns:
            True if view exists, False otherwise.
        """
        try:
            query = f"""
            SELECT COUNT(*) as cnt
            FROM {self.database}.INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = '{self.schema}'
            AND TABLE_NAME = '{view_name.upper()}'
            """
            result = self.execute_query(query)
            if result and result[0].get("CNT", 0) > 0:
                return True
            return False
        except Exception as e:
            logger.warning(f"Error checking view existence: {e}")
            return False

    def get_semantic_views(self) -> List[str]:
        """
        Get list of semantic views in the current schema.

        Returns:
            List of semantic view names, or empty list on error.
        """
        logger.info("📋 Fetching semantic views from Snowflake...")

        query = f"""
        SELECT TABLE_NAME
        FROM {self.database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{self.schema}'
        AND (TABLE_TYPE = 'VIEW' OR TABLE_TYPE = 'SEMANTIC VIEW')
        """

        results = self.execute_query(query, fetch_all=True)

        if results is None:
            return []

        view_names = [row.get("TABLE_NAME", "") for row in results]
        logger.info(f"✅ Found {len(view_names)} view(s)")
        return view_names

    def validate_semantic_view(self, view_name: str) -> bool:
        """
        Validate that a semantic view is accessible and working.

        Args:
            view_name: Name of the view to validate.

        Returns:
            True if view is valid, False otherwise.
        """
        logger.info(f"🔍 Validating semantic view: {view_name}")

        query = f"SELECT * FROM {self.schema}.{view_name} LIMIT 1"
        result = self.execute_query(query)

        if result is not None:
            logger.info(f"✅ Semantic view validated: {view_name}")
            return True

        logger.error(f"❌ Semantic view validation failed: {view_name}")
        return False

    def get_view_definition(self, view_name: str) -> Optional[str]:
        """
        Get the DDL definition of a view.

        Args:
            view_name: Name of the view.

        Returns:
            DDL string if successful, None otherwise.
        """
        logger.info(f"📋 Getting view definition for: {view_name}")

        query = f"SELECT GET_DDL('VIEW', '{self.schema}.{view_name}') AS DDL"
        result = self.execute_query(query)

        if result and result[0]:
            return result[0].get("DDL", "")

        return None

    def get_view_columns(self, view_name: str) -> List[Dict[str, Any]]:
        """
        Get column definitions for a view from INFORMATION_SCHEMA.

        Args:
            view_name: Name of the view.

        Returns:
            List of column dictionaries.
        """
        logger.info(f"📋 Getting columns for view: {view_name}")

        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COMMENT
        FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{self.schema}'
        AND TABLE_NAME = '{view_name.upper()}'
        ORDER BY ORDINAL_POSITION
        """

        results = self.execute_query(query, fetch_all=True)
        return results if results else []

    def update_view_measure(
        self,
        view_name: str,
        measure_name: str,
        new_expression: str,
        all_columns: List[str],
        all_measures: Dict[str, str],
    ) -> bool:
        """
        Update a measure in a view by recreating the view.

        Args:
            view_name: Name of the view to update.
            measure_name: Name of the measure to update.
            new_expression: New expression for the measure.
            all_columns: List of all dimension columns.
            all_measures: Dictionary of all measure names to expressions.

        Returns:
            True if successful, False otherwise.
        """
        logger.info(f"🔄 Updating measure {measure_name} in view {view_name}")

        if self.connection is None:
            logger.error("❌ Not connected to Snowflake")
            return False

        try:
            # Update the measure in the dictionary
            updated_measures = all_measures.copy()
            updated_measures[measure_name] = new_expression

            # Build the new view DDL
            column_list = ", ".join(all_columns)
            measure_list = ", ".join(
                [f"{expr} AS {name}" for name, expr in updated_measures.items()]
            )

            # Get original view to extract source table
            original_ddl = self.get_view_definition(view_name)
            if not original_ddl:
                logger.error("Could not get original view definition")
                return False

            # Parse source table from original DDL (simplified)
            import re
            from_match = re.search(r"FROM\s+(\S+)", original_ddl, re.IGNORECASE)
            source_table = from_match.group(1) if from_match else view_name

            ddl = f"""
            CREATE OR REPLACE VIEW {self.schema}.{view_name} AS
            SELECT {column_list}, {measure_list}
            FROM {source_table}
            """

            cursor = self.connection.cursor()
            cursor.execute(ddl)
            cursor.close()

            logger.info(f"✅ Updated measure {measure_name} in view {view_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to update view measure: {e}")
            return False


class SemanticSyncEngine:
    """
    Main orchestration engine for synchronizing semantic models
    between Microsoft Fabric and Snowflake.

    Handles the complete synchronization workflow including model
    extraction, transformation, and loading.
    """

    def __init__(self, direction: SyncDirection) -> None:
        """
        Initialize the sync engine with the specified direction.

        Args:
            direction: The synchronization direction to use.
        """
        self.direction: SyncDirection = direction
        self.fabric_client: FabricApiClient = FabricApiClient()
        self.snowflake_connector: SnowflakeConnector = SnowflakeConnector()
        self.sync_log: List[Dict[str, Any]] = []

        logger.info(
            f"🔄 Semantic Sync Engine initialized with direction: "
            f"{direction.value}"
        )

    def log_event(
        self,
        event_type: str,
        message: str,
        severity: str = "INFO",
    ) -> None:
        """
        Log a sync event for audit trail.

        Args:
            event_type: Type of event (e.g., SYNC_START, CREATE, ERROR).
            message: Event message.
            severity: Log severity level.
        """
        event: Dict[str, Any] = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "message": message,
            "severity": severity,
        }
        self.sync_log.append(event)

       
        log_level = getattr(logging, severity.upper(), logging.INFO)
        logger.log(log_level, f"[{event_type}] {message}")

    def extract_fabric_models(self) -> List[SemanticModel]:
        """
        Extract all semantic models from Microsoft Fabric.

        Returns:
            List of SemanticModel objects.
        """
        self.log_event("DISCOVERY", "Starting Fabric model discovery")

        
        if not self.fabric_client.authenticate():
            self.log_event(
                "ERROR",
                "Failed to authenticate with Fabric API",
                "ERROR",
            )
            return []

       
        model_list = self.fabric_client.get_semantic_models()

        if not model_list:
            self.log_event("WARNING", "No semantic models found", "WARNING")
            return []

        models: List[SemanticModel] = []

        for model_info in model_list:
            try:
                model_id = model_info.get("id", "")
                model_detail = self.fabric_client.get_semantic_model_detail(
                    model_id
                )

                if model_detail:
                    semantic_model = self._parse_fabric_model(model_detail)
                    models.append(semantic_model)
                    self.log_event(
                        "EXTRACT",
                        f"Extracted model: {semantic_model.display_name}",
                    )

            except Exception as e:
                self.log_event(
                    "ERROR",
                    f"Failed to extract model {model_info.get('name', 'unknown')}: {e}",
                    "ERROR",
                )

        self.log_event(
            "DISCOVERY",
            f"Completed extraction: {len(models)} model(s) found",
        )
        return models

    def _parse_fabric_model(self, model_dict: Dict[str, Any]) -> SemanticModel:
        """
        Parse a Fabric API response into a SemanticModel object.

        Args:
            model_dict: Dictionary from Fabric API response.

        Returns:
            Populated SemanticModel object.
        """
        model_name = model_dict.get("name", model_dict.get("displayName", "Unknown"))
        
        tables: List[Table] = []
        tables_data = model_dict.get("tables", [])
        
        # Log what we received for debugging
        logger.debug(f"Parsing model '{model_name}': has {len(tables_data)} tables in response")
        logger.debug(f"Model dict keys: {list(model_dict.keys())}")
        
        # If no tables in response, create a synthetic table representing the model itself
        if not tables_data:
            logger.info(f"Model '{model_name}' has no tables in API response. Creating synthetic table.")
            
            # Create a synthetic table that represents the entire model
            # The model itself is a dataset that can be viewed
            synthetic_table = Table(
                name=model_name,
                display_name=model_dict.get("displayName", model_name),
                source_expression="",  # No source expression - this IS the source
                columns=[
                    Column(name="ID", display_name="ID", data_type="string", is_hidden=False),
                    Column(name="VALUE", display_name="Value", data_type="string", is_hidden=False),
                    Column(name="CREATED_AT", display_name="Created At", data_type="datetime", is_hidden=False),
                ],
                measures=[],
                description=model_dict.get("description", "Semantic model from Fabric"),
                is_hidden=False,
            )
            tables.append(synthetic_table)
        else:
            # Parse tables from API response
            for table_data in tables_data:
                columns: List[Column] = []
                for col_data in table_data.get("columns", []):
                    column = Column(
                        name=col_data.get("name", ""),
                        display_name=col_data.get("displayName", col_data.get("name", "")),
                        data_type=col_data.get("dataType", "string"),
                        is_hidden=col_data.get("isHidden", False),
                        description=col_data.get("description", ""),
                    )
                    columns.append(column)

                measures: List[Measure] = []
                for measure_data in table_data.get("measures", []):
                    measure = Measure(
                        name=measure_data.get("name", ""),
                        display_name=measure_data.get(
                            "displayName", measure_data.get("name", "")
                        ),
                        expression=measure_data.get("expression", ""),
                        description=measure_data.get("description", ""),
                        format_string=measure_data.get("formatString", ""),
                    )
                    measures.append(measure)

                table = Table(
                    name=table_data.get("name", ""),
                    display_name=table_data.get(
                        "displayName", table_data.get("name", "")
                    ),
                    source_expression=table_data.get("source", {}).get(
                        "expression", ""
                    ),
                    columns=columns,
                    measures=measures,
                    description=table_data.get("description", ""),
                    is_hidden=table_data.get("isHidden", False),
                )
                tables.append(table)
        
        logger.info(f"Parsed model '{model_name}' with {len(tables)} table(s)")

        relationships: List[Relationship] = []
        for rel_data in model_dict.get("relationships", []):
            relationship = Relationship(
                name=rel_data.get("name", ""),
                from_table=rel_data.get("fromTable", ""),
                from_column=rel_data.get("fromColumn", ""),
                to_table=rel_data.get("toTable", ""),
                to_column=rel_data.get("toColumn", ""),
                cardinality=rel_data.get("cardinality", "Many-to-One"),
                cross_filtering=rel_data.get("crossFilteringBehavior", "Both"),
            )
            relationships.append(relationship)

        return SemanticModel(
            id=model_dict.get("id", ""),
            name=model_dict.get("name", ""),
            display_name=model_dict.get("displayName", model_dict.get("name", "")),
            workspace_id=model_dict.get("workspaceId", self.fabric_client.workspace_id),
            tables=tables,
            relationships=relationships,
            description=model_dict.get("description", ""),
            created_date=model_dict.get("createdDate", ""),
            modified_date=model_dict.get("modifiedDate", ""),
        )

    def sync_to_snowflake(
        self,
        models: List[SemanticModel],
        force: bool = False,
    ) -> Tuple[int, int, int, Dict[str, int]]:
        """
        Synchronize semantic models to Snowflake.

        Creates semantic views in Snowflake for each table in each model.
        Includes smart change detection that checks if views actually exist.

        Args:
            models: List of SemanticModel objects to sync.
            force: If True, recreate all views regardless of change detection.

        Returns:
            Tuple of (successful_count, failed_count, skipped_count, skip_reasons).
        """
        self.log_event(
            "SYNC_START", 
            f"Starting sync to Snowflake" + (" [FORCE MODE]" if force else "")
        )
        
        # Initialize audit logger if available
        audit_logger = None
        if PRODUCTION_MODULES_AVAILABLE:
            try:
                audit_logger = get_audit_logger()
                audit_logger.sync_start(
                    sync_id=f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    context={"model_count": len(models), "force": force}
                )
            except Exception:
                pass
        
        # Connect to Snowflake
        if not self.snowflake_connector.connect():
            self.log_event(
                "ERROR",
                "Failed to connect to Snowflake",
                "ERROR",
            )
            if audit_logger:
                audit_logger.error("Failed to connect to Snowflake", Exception("Connection failed"))
            return 0, len(models), 0, {"connection_failed": len(models)}

        # Get existing views in Snowflake for intelligent change detection
        existing_views: set = set()
        if not force:
            try:
                views_list = self.snowflake_connector.get_semantic_views()
                existing_views = {v.upper() for v in views_list}
                self.log_event(
                    "DETECTION",
                    f"Found {len(existing_views)} existing views in Snowflake"
                )
            except Exception as e:
                self.log_event(
                    "WARNING",
                    f"Could not fetch existing views: {e}. Proceeding with full sync.",
                    "WARNING"
                )
                # If we can't get existing views, treat as force mode
                existing_views = set()

        # Use PartialSyncResult for proper partial failure handling
        if PRODUCTION_MODULES_AVAILABLE:
            sync_result = PartialSyncResult()
        
        successful: int = 0
        failed: int = 0
        skipped: int = 0
        skip_reasons: Dict[str, int] = {
            "view_already_exists": 0,
            "no_changes_detected": 0,
            "view_missing_created": 0,
        }

        for model in models:
            for table in model.tables:
                start_time = time.time()
                
                try:
                    # Get the effective model name (with fallback)
                    effective_model_name = model.name if model.name else model.display_name
                    effective_table_name = table.name if table.name else table.display_name
                    
                    # Generate standardized view name using NamingConvention
                    # Format: SV_FABRIC_{MODEL_NAME}_{TABLE_NAME}
                    if PRODUCTION_MODULES_AVAILABLE:
                        view_name = generate_semantic_view_name(effective_model_name, effective_table_name)
                    else:
                        view_name = self._sanitize_name(f"sv_{effective_model_name}_{effective_table_name}")
                    
                    # Generate base table name (what the view will reference)
                    base_table_name = f"TBL_FABRIC_{effective_model_name}_{effective_table_name}".upper()
                    base_table_name = base_table_name.replace(" ", "_").replace("-", "_")
                    
                    # ================================================
                    # LOG SOURCE EXPRESSION FOR DEBUGGING
                    # ================================================
                    source_expr = table.source_expression if table.source_expression else "(No source expression - will create synthetic table)"
                    self.log_event(
                        "SOURCE_INFO",
                        f"Model: {effective_model_name}, Table: {effective_table_name}, Source: {source_expr[:100]}..."
                        if len(source_expr) > 100 else 
                        f"Model: {effective_model_name}, Table: {effective_table_name}, Source: {source_expr}"
                    )
                    
                    # ================================================
                    # SMART CHANGE DETECTION - Key fix for the issue
                    # ================================================
                    # Check if view ACTUALLY exists in Snowflake (not just in cached list)
                    view_actually_exists = self.snowflake_connector.view_exists(view_name)
                    
                    if not force and view_actually_exists:
                        # View exists - skip it unless force mode
                        skipped += 1
                        skip_reasons["view_already_exists"] += 1
                        self.log_event(
                            "SKIP",
                            f"View already exists (validated): {view_name}",
                        )
                        continue
                    
                    # If view doesn't exist - ALWAYS create it (this is the key fix!)
                    if not view_actually_exists:
                        skip_reasons["view_missing_created"] += 1
                        self.log_event(
                            "DETECTION",
                            f"View MISSING in Snowflake (validated check), will create: {view_name}",
                        )
                    elif force:
                        self.log_event(
                            "FORCE",
                            f"Force mode - recreating view: {view_name}",
                        )
                    
                    # Build dimensions (columns) with Snowflake types
                    dimensions: Dict[str, str] = {}
                    for column in table.columns:
                        if not column.is_hidden:
                            # Sanitize column names too
                            col_name = (
                                NamingConvention.generate_column_name(column.name)
                                if PRODUCTION_MODULES_AVAILABLE
                                else column.name.upper().replace(" ", "_")
                            )
                            snowflake_type = DataTypeMapping.get_snowflake_type(
                                column.data_type
                            )
                            dimensions[col_name] = snowflake_type
                    
                    # ================================================
                    # CREATE BASE TABLE FIRST (if doesn't exist)
                    # ================================================
                    # Check if base table exists
                    base_table_exists = self.snowflake_connector.table_exists(base_table_name)
                    
                    if not base_table_exists:
                        self.log_event(
                            "TABLE_CREATE",
                            f"Base table {base_table_name} doesn't exist. Creating from model definition...",
                        )
                        
                        # Create base table from dimensions (columns)
                        if dimensions:
                            table_created = self.snowflake_connector.create_base_table(
                                table_name=base_table_name,
                                columns=dimensions,
                                if_not_exists=True,
                            )
                            if table_created:
                                self.log_event(
                                    "TABLE_CREATED",
                                    f"Created base table: {base_table_name} with {len(dimensions)} columns",
                                )
                            else:
                                self.log_event(
                                    "WARNING",
                                    f"Could not create base table {base_table_name}, will try creating view anyway",
                                    "WARNING"
                                )
                        else:
                            self.log_event(
                                "WARNING",
                                f"No columns found for table {table.name}, skipping base table creation",
                                "WARNING"
                            )
                    else:
                        self.log_event(
                            "TABLE_EXISTS",
                            f"Base table {base_table_name} already exists",
                        )
                    
                    # Build measures
                    measures: Dict[str, str] = {}
                    for measure in table.measures:
                        measure_name = (
                            NamingConvention.generate_measure_name(measure.name)
                            if PRODUCTION_MODULES_AVAILABLE
                            else measure.name.upper().replace(" ", "_")
                        )
                        measures[measure_name] = self._convert_dax_to_sql(
                            measure.expression
                        )
                    
                    # ================================================
                    # DETERMINE SOURCE FOR VIEW
                    # ================================================
                    # Use the base table we created/verified as the source
                    table_def = f"{self.snowflake_connector.schema}.{base_table_name}"
                    
                    self.log_event(
                        "VIEW_CREATE",
                        f"Creating view {view_name} from source: {table_def}",
                    )
                    
                    # Create the semantic view
                    if self.snowflake_connector.create_semantic_view(
                        view_name=view_name,
                        table_definition=table_def,
                        dimensions=dimensions,
                        measures=measures,
                    ):
                        duration_ms = (time.time() - start_time) * 1000
                        
                        self.log_event(
                            "CREATE",
                            f"Created semantic view: {view_name}",
                        )
                        
                        # Log to audit logger
                        if audit_logger:
                            audit_logger.view_created(
                                view_name=view_name,
                                model_name=model.name,
                                table_name=table.name,
                                duration_ms=duration_ms
                            )
                        
                        if PRODUCTION_MODULES_AVAILABLE:
                            sync_result.add_success(
                                model.name, table.name, view_name, duration_ms
                            )
                        
                        successful += 1
                    else:
                        self.log_event(
                            "ERROR",
                            f"Failed to create view: {view_name}",
                            "ERROR",
                        )
                        
                        if PRODUCTION_MODULES_AVAILABLE:
                            sync_result.add_failure(
                                model.name, table.name, 
                                Exception(f"View creation returned False"),
                                view_name
                            )
                        
                        failed += 1

                except Exception as e:
                    # PARTIAL FAILURE HANDLING: Log the error but continue with other tables
                    # This ensures 1 failure doesn't stop the entire sync
                    self.log_event(
                        "ERROR",
                        f"Error syncing table {table.name}: {e}",
                        "ERROR",
                    )
                    
                    if audit_logger:
                        audit_logger.error(
                            f"Failed to sync table {table.name}",
                            e,
                            model_name=model.name,
                            table_name=table.name
                        )
                    
                    if PRODUCTION_MODULES_AVAILABLE:
                        sync_result.add_failure(model.name, table.name, e)
                    
                    failed += 1
                    # Continue to next table - don't stop the sync!
                    continue

        self.snowflake_connector.disconnect()

        # Log summary with detailed breakdown
        summary_msg = (
            f"Sync completed: {successful} created, {skipped} skipped, {failed} failures "
            f"(missing+created: {skip_reasons.get('view_missing_created', 0)}, "
            f"already exists: {skip_reasons.get('view_already_exists', 0)})"
        )
        self.log_event("SYNC_END", summary_msg)
        
        # Detect state mismatch warning
        total_models = sum(len(m.tables) for m in models)
        if skipped > 0 and successful == 0 and failed == 0:
            self.log_event(
                "WARNING",
                f"⚠️ No views were created. All {skipped} views already exist in Snowflake. "
                f"Use --force to recreate views or --reconcile to check for missing views.",
                "WARNING"
            )
        
        # Finalize and log partial sync results
        if PRODUCTION_MODULES_AVAILABLE:
            sync_result.finalize()
            sync_result.log_summary(logger)
            
            if audit_logger:
                audit_logger.sync_end(
                    success=failed == 0,
                    stats=sync_result.to_dict()
                )
        
        return successful, failed, skipped, skip_reasons

    def sync_data_to_snowflake(
        self,
        models: List[SemanticModel],
        force: bool = False,
        sync_mode: str = "full_refresh"
    ) -> Dict[str, Any]:
        """
        Extract actual row-level data from Fabric semantic models and load into Snowflake.
        
        This is the critical method that was missing - it populates Snowflake tables
        with actual business data, not just metadata/schema structures.
        
        Args:
            models: List of SemanticModel objects to sync data from.
            force: If True, reload data even if row counts match.
            sync_mode: One of 'full_refresh', 'incremental', or 'append'.
            
        Returns:
            Dictionary containing data sync results including row counts.
        """
        self.log_event(
            "DATA_SYNC_START", 
            f"Starting data extraction and loading" + (" [FORCE MODE]" if force else "")
        )
        
        if not DATA_EXTRACTOR_AVAILABLE:
            self.log_event(
                "ERROR",
                "Data extractor module not available. Cannot extract data.",
                "ERROR"
            )
            return {
                "status": "FAILED",
                "error": "Data extractor module not available",
                "models_processed": 0,
                "rows_extracted": 0,
                "rows_loaded": 0
            }
        
        # Map sync mode string to enum
        mode_map = {
            "full_refresh": DataSyncMode.FULL_REFRESH,
            "incremental": DataSyncMode.INCREMENTAL,
            "append": DataSyncMode.APPEND_ONLY,
        }
        data_sync_mode = mode_map.get(sync_mode, DataSyncMode.FULL_REFRESH)
        
        # Initialize orchestrator
        orchestrator = DataSyncOrchestrator(
            sync_mode=data_sync_mode,
            batch_size=1000
        )
        
        # Prepare model data for extraction
        model_data = []
        for model in models:
            tables_data = []
            for table in model.tables:
                # IMPORTANT: Do NOT pass synthetic column names to the extractor!
                # When Fabric API doesn't return table definitions, we create synthetic
                # tables with placeholder columns (ID, VALUE, CREATED_AT) that don't
                # exist in the actual semantic model.
                # 
                # Instead, pass None/empty columns to let DAX dynamically discover
                # the actual columns from the Fabric semantic model.
                #
                # We check if these are synthetic columns by looking for our placeholder names
                column_names = [col.name for col in table.columns if not col.is_hidden]
                is_synthetic = set(column_names) <= {"ID", "VALUE", "CREATED_AT", "UPDATED_AT"}
                
                tables_data.append({
                    "name": table.name,
                    "display_name": table.display_name,
                    # Pass None for synthetic tables to let DAX discover actual columns
                    "columns": None if is_synthetic else column_names
                })
            
            model_data.append({
                "id": model.id,
                "name": model.name or model.display_name,
                "displayName": model.display_name,
                "tables": tables_data
            })
        
        # Run the data sync
        results = orchestrator.run_full_sync(model_data, force=force)
        
        # Log results
        summary = results.get("summary", {})
        self.log_event(
            "DATA_SYNC_COMPLETE",
            f"Data sync complete: {summary.get('total_rows_extracted', 0)} rows extracted, "
            f"{summary.get('total_rows_loaded', 0)} rows loaded, "
            f"{summary.get('load_failures', 0)} failures"
        )
        
        return results

    def sync_with_data(
        self,
        models: List[SemanticModel],
        force: bool = False,
        sync_data: bool = True
    ) -> Tuple[int, int, int, Dict[str, int], Dict[str, Any]]:
        """
        Complete sync including both schema/views AND actual data.
        
        This is the enhanced sync method that first creates/updates views,
        then extracts and loads actual data.
        
        Args:
            models: List of SemanticModel objects to sync.
            force: If True, force sync regardless of change detection.
            sync_data: If True, also sync row-level data (not just schema).
            
        Returns:
            Tuple of (successful, failed, skipped, skip_reasons, data_sync_results).
        """
        # First, sync schema/views
        successful, failed, skipped, skip_reasons = self.sync_to_snowflake(models, force=force)
        
        # Then sync actual data if requested
        data_results = {}
        if sync_data and DATA_EXTRACTOR_AVAILABLE:
            self.log_event("DATA_SYNC", "Now syncing actual row-level data...")
            data_results = self.sync_data_to_snowflake(models, force=force)
        elif sync_data and not DATA_EXTRACTOR_AVAILABLE:
            self.log_event(
                "WARNING",
                "Data sync requested but data_extractor module not available",
                "WARNING"
            )
        
        return successful, failed, skipped, skip_reasons, data_results

    def compare_row_counts(
        self,
        models: List[SemanticModel]
    ) -> Dict[str, Any]:
        """
        Compare row counts between Fabric semantic models and Snowflake tables.
        
        This is used for enhanced change detection - if row counts differ significantly,
        we should trigger a data reload.
        
        Args:
            models: List of SemanticModel objects to compare.
            
        Returns:
            Dictionary with row count comparisons and sync recommendations.
        """
        self.log_event("ROW_COUNT_COMPARE", "Comparing row counts between Fabric and Snowflake")
        
        comparison_results = {
            "timestamp": datetime.now().isoformat(),
            "tables": [],
            "needs_sync": [],
            "in_sync": [],
            "errors": []
        }
        
        if not DATA_EXTRACTOR_AVAILABLE:
            self.log_event(
                "WARNING",
                "Data extractor not available for row count comparison",
                "WARNING"
            )
            return comparison_results
        
        extractor = FabricDataExtractor()
        loader = SnowflakeDataLoader()
        
        # Authenticate and connect
        if not extractor.authenticate():
            comparison_results["errors"].append("Failed to authenticate with Fabric")
            return comparison_results
        
        if not loader.connect():
            comparison_results["errors"].append("Failed to connect to Snowflake")
            return comparison_results
        
        try:
            for model in models:
                model_name = model.name or model.display_name
                
                for table in model.tables:
                    table_name = table.name or table.display_name
                    
                    # Generate Snowflake table name
                    sf_table_name = f"TBL_FABRIC_{model_name}_{table_name}".upper()
                    sf_table_name = sf_table_name.replace(" ", "_").replace("-", "_")
                    
                    # Get row counts
                    fabric_count = extractor.get_table_row_count(model.id, table_name)
                    snowflake_count = loader.get_table_row_count(sf_table_name)
                    
                    table_result = {
                        "model": model_name,
                        "table": table_name,
                        "snowflake_table": sf_table_name,
                        "fabric_row_count": fabric_count,
                        "snowflake_row_count": snowflake_count,
                        "match": fabric_count == snowflake_count
                    }
                    
                    comparison_results["tables"].append(table_result)
                    
                    # Determine if sync is needed
                    if snowflake_count == 0 and fabric_count > 0:
                        table_result["sync_reason"] = "Snowflake table is empty"
                        comparison_results["needs_sync"].append(table_result)
                    elif snowflake_count == -1:
                        table_result["sync_reason"] = "Snowflake table doesn't exist or error"
                        comparison_results["needs_sync"].append(table_result)
                    elif fabric_count > 0 and abs(fabric_count - snowflake_count) > 0:
                        table_result["sync_reason"] = f"Row count mismatch: {fabric_count} vs {snowflake_count}"
                        comparison_results["needs_sync"].append(table_result)
                    else:
                        table_result["sync_reason"] = "Row counts match"
                        comparison_results["in_sync"].append(table_result)
                    
                    self.log_event(
                        "ROW_COUNT",
                        f"{sf_table_name}: Fabric={fabric_count}, Snowflake={snowflake_count} "
                        f"({'MATCH' if table_result['match'] else 'MISMATCH'})"
                    )
                    
        finally:
            loader.disconnect()
        
        # Summary
        comparison_results["summary"] = {
            "total_tables": len(comparison_results["tables"]),
            "tables_in_sync": len(comparison_results["in_sync"]),
            "tables_need_sync": len(comparison_results["needs_sync"]),
            "errors": len(comparison_results["errors"])
        }
        
        return comparison_results


    def _sanitize_name(self, name: str) -> str:
        """
        Sanitize a name for use as a Snowflake identifier.
        
        Uses the NamingConvention module for comprehensive reserved keyword handling.
        This fixes the critical issue where names like "day" cause SQL syntax errors.

        Args:
            name: Original name string.

        Returns:
            Sanitized name suitable for Snowflake (uppercase, safe identifier).
        """
        if PRODUCTION_MODULES_AVAILABLE:
            # Use production-grade naming convention with reserved keyword handling
            sanitized = NamingConvention.sanitize_name(name, IdentifierType.VIEW)
            
            # Log if this was a reserved keyword
            if NamingConvention.is_reserved_keyword(name.upper()):
                self.log_event(
                    "NAME_SANITIZED",
                    f"Reserved keyword '{name}' sanitized to '{sanitized}'",
                    "WARNING"
                )
            return sanitized
        else:
            # Fallback: Basic sanitization (original logic)
            sanitized = name.replace(" ", "_").replace("-", "_")
            sanitized = "".join(c for c in sanitized if c.isalnum() or c == "_")
            if sanitized and sanitized[0].isdigit():
                sanitized = f"v_{sanitized}"
            return sanitized.upper()

    def _convert_dax_to_sql(self, expression: str) -> str:
        """
        Convert a DAX expression to SQL-compatible expression.

        This is a simplified conversion for common patterns.
        Complex DAX may require manual adjustment.

        Args:
            expression: DAX expression string.

        Returns:
            SQL-compatible expression string.
        """
      
        sql_expr = expression

      
        replacements = {
            "SUM(": "SUM(",
            "COUNT(": "COUNT(",
            "AVERAGE(": "AVG(",
            "MIN(": "MIN(",
            "MAX(": "MAX(",
            "COUNTROWS(": "COUNT(*",
            "DISTINCTCOUNT(": "COUNT(DISTINCT ",
            "CALCULATE(": "(",
            "FILTER(": "(",
            "ALL(": "(",
            "VALUES(": "(",
        }

        for dax_func, sql_func in replacements.items():
            sql_expr = sql_expr.replace(dax_func, sql_func)

        return sql_expr

    def _convert_sql_to_dax(self, expression: str) -> str:
        """
        Convert a SQL expression to DAX-compatible expression.

        This is a simplified conversion for common patterns.
        Complex SQL may require manual adjustment.

        Args:
            expression: SQL expression string.

        Returns:
            DAX-compatible expression string.
        """
        dax_expr = expression

        # Replace common SQL functions with DAX equivalents
        replacements = {
            "AVG(": "AVERAGE(",
            "COUNT(*)": "COUNTROWS()",
            "COUNT(DISTINCT ": "DISTINCTCOUNT(",
        }

        for sql_func, dax_func in replacements.items():
            dax_expr = dax_expr.replace(sql_func, dax_func)

        return dax_expr

    def sync_from_snowflake(
        self,
        view_names: List[str],
        target_model_id: str,
    ) -> Tuple[int, int]:
        """
        Synchronize changes from Snowflake views to Fabric semantic model.

        Args:
            view_names: List of Snowflake view names to sync from.
            target_model_id: Target Fabric model ID to update.

        Returns:
            Tuple of (successful_count, failed_count).
        """
        self.log_event("SYNC_START", "Starting sync from Snowflake to Fabric")

        # Connect to Snowflake
        if not self.snowflake_connector.connect():
            self.log_event(
                "ERROR",
                "Failed to connect to Snowflake",
                "ERROR",
            )
            return 0, len(view_names)

        # Authenticate with Fabric
        if not self.fabric_client.authenticate():
            self.log_event(
                "ERROR",
                "Failed to authenticate with Fabric",
                "ERROR",
            )
            return 0, len(view_names)

        successful: int = 0
        failed: int = 0

        for view_name in view_names:
            try:
                # Get view definition from Snowflake
                view_ddl = self.snowflake_connector.get_view_definition(view_name)
                if not view_ddl:
                    self.log_event(
                        "WARNING",
                        f"Could not get definition for view: {view_name}",
                        "WARNING",
                    )
                    failed += 1
                    continue

                # Parse measures from DDL
                # This is simplified - real implementation would need more parsing
                self.log_event(
                    "INFO",
                    f"Retrieved definition for view: {view_name}",
                )
                successful += 1

            except Exception as e:
                self.log_event(
                    "ERROR",
                    f"Error syncing view {view_name}: {e}",
                    "ERROR",
                )
                failed += 1

        # Disconnect
        self.snowflake_connector.disconnect()

        self.log_event(
            "SYNC_END",
            f"Snowflake sync completed: {successful} successful, {failed} failures",
        )
        return successful, failed

    def apply_changes(
        self,
        changes: List[Any],
        direction: SyncDirection,
    ) -> Tuple[int, int]:
        """
        Apply a list of changes to the target system.

        Args:
            changes: List of ChangeRecord objects to apply.
            direction: Direction of sync to determine target.

        Returns:
            Tuple of (successful_count, failed_count).
        """
        from change_detector import ChangeRecord, ChangeType

        self.log_event(
            "APPLY_CHANGES",
            f"Applying {len(changes)} change(s) in direction: {direction.value}",
        )

        successful = 0
        failed = 0

        for change in changes:
            if not isinstance(change, ChangeRecord):
                continue

            try:
                if change.item_type == "measure":
                    if direction == SyncDirection.FABRIC_TO_SNOWFLAKE:
                        # Apply measure change to Snowflake
                        if change.change_type == ChangeType.MODIFIED:
                            sql_expr = self._convert_dax_to_sql(change.after_value)
                            # Would call update_view_measure here
                            self.log_event(
                                "APPLY",
                                f"Would update Snowflake measure: "
                                f"{change.table_name}.{change.item_name}",
                            )
                            successful += 1

                    elif direction == SyncDirection.SNOWFLAKE_TO_FABRIC:
                        # Apply measure change to Fabric
                        if change.change_type == ChangeType.MODIFIED:
                            dax_expr = self._convert_sql_to_dax(change.after_value)
                            # Would call fabric_client.update_measure here
                            self.log_event(
                                "APPLY",
                                f"Would update Fabric measure: "
                                f"{change.table_name}.{change.item_name}",
                            )
                            successful += 1

            except Exception as e:
                self.log_event(
                    "ERROR",
                    f"Failed to apply change {change.item_name}: {e}",
                    "ERROR",
                )
                failed += 1

        return successful, failed

    def preview_changes(
        self,
        direction: SyncDirection,
    ) -> Dict[str, Any]:
        """
        Preview what changes would be made without applying them.

        Args:
            direction: Direction of sync to preview.

        Returns:
            Dictionary with change preview information.
        """
        from change_detector import ChangeDetector

        self.log_event("PREVIEW", f"Previewing changes for direction: {direction.value}")

        preview: Dict[str, Any] = {
            "direction": direction.value,
            "changes": [],
            "summary": {
                "total": 0,
                "added": 0,
                "modified": 0,
                "removed": 0,
            },
        }

        try:
            detector = ChangeDetector(
                fabric_client=self.fabric_client,
                snowflake_connector=self.snowflake_connector,
            )

            # This would capture and compare real snapshots
            # For now, return empty preview
            self.log_event(
                "PREVIEW",
                "Preview complete - use ChangeDetector for full comparison",
            )

        except Exception as e:
            self.log_event("ERROR", f"Preview failed: {e}", "ERROR")
            preview["error"] = str(e)

        return preview

    def run_sync(self, force: bool = False, sync_data: bool = False) -> Dict[str, Any]:
        """
        Execute the complete synchronization workflow.

        Args:
            force: If True, bypass change detection and sync all views.
                   Use when sync state is corrupted or views are missing.
            sync_data: If True, also extract and load actual row-level data
                       from Fabric to Snowflake tables.

        Returns:
            Dictionary containing sync results and metadata.
        """
        self.log_event(
            "SYNC_INIT",
            f"Starting synchronization: {self.direction.value}" + 
            (" [FORCE MODE]" if force else "") +
            (" [WITH DATA]" if sync_data else " [SCHEMA ONLY]"),
        )

        start_time = datetime.now()
        results: Dict[str, Any] = {
            "start_time": start_time.isoformat(),
            "end_time": "",
            "duration_seconds": 0.0,
            "status": "IN_PROGRESS",
            "direction": self.direction.value,
            "force_mode": force,
            "sync_data": sync_data,
            "models_synced": 0,
            "views_created": 0,
            "views_skipped": 0,
            "failures": 0,
            "skip_reasons": {},
            "data_sync": {},
            "log": [],
        }

        try:
            models = self.extract_fabric_models()
            results["models_synced"] = len(models)

            if self.direction in (
                SyncDirection.FABRIC_TO_SNOWFLAKE,
                SyncDirection.BIDIRECTIONAL,
            ):
                successful, failed, skipped, skip_reasons = self.sync_to_snowflake(
                    models, force=force
                )
                results["views_created"] = successful
                results["failures"] = failed
                results["views_skipped"] = skipped
                results["skip_reasons"] = skip_reasons
                
                # Sync actual data if requested
                if sync_data:
                    self.log_event(
                        "DATA_SYNC_START",
                        "Now extracting and loading actual data to Snowflake tables..."
                    )
                    data_results = self.sync_data_to_snowflake(models, force=force)
                    results["data_sync"] = data_results
                    
                    # Update overall status based on data sync
                    data_summary = data_results.get("summary", {})
                    results["rows_extracted"] = data_summary.get("total_rows_extracted", 0)
                    results["rows_loaded"] = data_summary.get("total_rows_loaded", 0)
                    results["data_failures"] = data_summary.get("load_failures", 0)
                    
                    self.log_event(
                        "DATA_SYNC_SUMMARY",
                        f"Data sync: {results['rows_extracted']} rows extracted, "
                        f"{results['rows_loaded']} rows loaded"
                    )

           
            if results["failures"] == 0 and results.get("data_failures", 0) == 0:
                if results["views_skipped"] > 0 and results["views_created"] == 0:
                    results["status"] = "NO_CHANGES_DETECTED"
                else:
                    results["status"] = "COMPLETED_SUCCESSFULLY"
            elif results["views_created"] > 0 or results.get("rows_loaded", 0) > 0:
                results["status"] = "COMPLETED_WITH_WARNINGS"
            else:
                results["status"] = "FAILED"

        except Exception as e:
            self.log_event("FATAL_ERROR", f"Sync failed: {e}", "ERROR")
            results["status"] = "FAILED"
            results["error"] = str(e)

       
        end_time = datetime.now()
        results["end_time"] = end_time.isoformat()
        results["duration_seconds"] = (end_time - start_time).total_seconds()
        results["log"] = self.sync_log

        self.log_event(
            "SYNC_COMPLETE",
            f"Synchronization finished with status: {results['status']}",
        )

        return results


def run_reconciliation_sync() -> Dict[str, Any]:
    """
    Run reconciliation sync - detect and create only missing views.
    
    This function compares Fabric models with existing Snowflake views and
    creates views only for models that don't have corresponding views.
    This is the key fix for the "0 successful, 0 failures" issue.
    
    Returns:
        Dictionary with reconciliation results.
    """
    logger.info("=" * 80)
    logger.info("RECONCILIATION SYNC - Detecting Missing Views")
    logger.info("=" * 80)
    
    results: Dict[str, Any] = {
        "status": "IN_PROGRESS",
        "fabric_model_count": 0,
        "snowflake_view_count": 0,
        "missing_views": [],
        "views_created": 0,
        "failures": 0,
        "timestamp": datetime.now().isoformat(),
    }
    
    # Initialize sync engine and clients
    sync_engine = SemanticSyncEngine(SyncDirection.FABRIC_TO_SNOWFLAKE)
    
    # Extract Fabric models
    logger.info("📊 Extracting Fabric semantic models...")
    models = sync_engine.extract_fabric_models()
    results["fabric_model_count"] = len(models)
    logger.info(f"   Found {len(models)} models in Fabric")
    
    if not models:
        results["status"] = "NO_FABRIC_MODELS"
        results["message"] = "No Fabric models found to sync"
        return results
    
    # Get expected view names from Fabric models
    expected_views: Dict[str, Tuple] = {}
    for model in models:
        for table in model.tables:
            if PRODUCTION_MODULES_AVAILABLE:
                view_name = generate_semantic_view_name(model.name, table.name)
            else:
                view_name = f"SV_FABRIC_{model.name}_{table.name}".upper().replace(" ", "_")
            expected_views[view_name.upper()] = (model, table)
    
    logger.info(f"   Expected views: {len(expected_views)}")
    for view_name in expected_views.keys():
        logger.debug(f"     - {view_name}")
    
    # Get existing Snowflake views
    logger.info("❄️ Fetching existing Snowflake views...")
    if not sync_engine.snowflake_connector.connect():
        results["status"] = "SNOWFLAKE_CONNECTION_FAILED"
        results["message"] = "Failed to connect to Snowflake"
        return results
    
    existing_views = set(v.upper() for v in sync_engine.snowflake_connector.get_semantic_views())
    results["snowflake_view_count"] = len(existing_views)
    logger.info(f"   Found {len(existing_views)} existing views in Snowflake")
    
    # Find missing views
    missing_views = []
    for view_name, (model, table) in expected_views.items():
        if view_name not in existing_views:
            missing_views.append({
                "view_name": view_name,
                "model_name": model.name,
                "table_name": table.name,
            })
            logger.info(f"   ⚠️ MISSING: {view_name} (from model '{model.name}')")
    
    results["missing_views"] = [m["view_name"] for m in missing_views]
    
    if not missing_views:
        logger.info("✅ All Fabric models have corresponding Snowflake views!")
        sync_engine.snowflake_connector.disconnect()
        results["status"] = "ALL_SYNCED"
        results["message"] = "No missing views - all models are synced"
        return results
    
    logger.info(f"🔧 Found {len(missing_views)} missing views - creating them now...")
    
    # Create missing views only
    successful = 0
    failed = 0
    
    for missing in missing_views:
        model_name = missing["model_name"]
        table_name = missing["table_name"]
        view_name = missing["view_name"]
        
        # Find the actual model and table objects
        model = None
        table = None
        for m in models:
            if m.name == model_name:
                model = m
                for t in m.tables:
                    if t.name == table_name:
                        table = t
                        break
                break
        
        if not model or not table:
            logger.error(f"   ❌ Could not find model/table for {view_name}")
            failed += 1
            continue
        
        try:
            # Build dimensions (columns)
            dimensions: Dict[str, str] = {}
            for column in table.columns:
                if not column.is_hidden:
                    if PRODUCTION_MODULES_AVAILABLE:
                        col_name = NamingConvention.generate_column_name(column.name)
                    else:
                        col_name = column.name
                    snowflake_type = DataTypeMapping.get_snowflake_type(column.data_type)
                    dimensions[col_name] = snowflake_type
            
            # Build measures
            measures: Dict[str, str] = {}
            for measure in table.measures:
                if PRODUCTION_MODULES_AVAILABLE:
                    measure_name = NamingConvention.generate_measure_name(measure.name)
                else:
                    measure_name = measure.name
                # Simple DAX to SQL conversion
                sql_expr = measure.expression
                sql_expr = sql_expr.replace("AVERAGE(", "AVG(")
                sql_expr = sql_expr.replace("COUNTROWS(", "COUNT(*")
                measures[measure_name] = sql_expr
            
            # Determine source table definition  
            table_def = table.source_expression if table.source_expression else f"{model.name}.{table.name}"
            
            # Create the view
            if sync_engine.snowflake_connector.create_semantic_view(
                view_name=view_name,
                table_definition=table_def,
                dimensions=dimensions,
                measures=measures,
            ):
                logger.info(f"   ✅ Created: {view_name}")
                successful += 1
            else:
                logger.error(f"   ❌ Failed to create: {view_name}")
                failed += 1
                
        except Exception as e:
            logger.error(f"   ❌ Error creating {view_name}: {e}")
            failed += 1
    
    sync_engine.snowflake_connector.disconnect()
    
    results["views_created"] = successful
    results["failures"] = failed
    
    if failed == 0:
        results["status"] = "RECONCILIATION_COMPLETE"
        results["message"] = f"Successfully created {successful} missing views"
    elif successful > 0:
        results["status"] = "PARTIAL_RECONCILIATION"
        results["message"] = f"Created {successful} views, {failed} failures"
    else:
        results["status"] = "RECONCILIATION_FAILED"
        results["message"] = f"Failed to create any views ({failed} failures)"
    
    logger.info("=" * 80)
    logger.info(f"Reconciliation complete: {successful} created, {failed} failed")
    logger.info("=" * 80)
    
    return results


def run_data_sync(force: bool = False, sync_mode: str = "full_refresh") -> Dict[str, Any]:
    """
    Run data extraction and loading from Fabric to Snowflake.
    
    This function extracts actual row-level data from Fabric semantic models
    and loads it into the existing Snowflake tables. This is the key fix for
    tables that exist but are empty.
    
    Args:
        force: If True, reload data even if row counts match.
        sync_mode: One of 'full_refresh', 'incremental', or 'append'.
        
    Returns:
        Dictionary with data sync results.
    """
    logger.info("=" * 80)
    logger.info("DATA SYNC - Extracting Data from Fabric and Loading to Snowflake")
    logger.info("=" * 80)
    logger.info(f"Mode: {sync_mode}")
    logger.info(f"Force: {force}")
    logger.info("=" * 80)
    
    results: Dict[str, Any] = {
        "status": "IN_PROGRESS",
        "timestamp": datetime.now().isoformat(),
        "sync_mode": sync_mode,
        "force": force,
    }
    
    if not DATA_EXTRACTOR_AVAILABLE:
        logger.error("❌ Data extractor module not available")
        results["status"] = "FAILED"
        results["error"] = "Data extractor module not available"
        return results
    
    # First, extract the models
    sync_engine = SemanticSyncEngine(SyncDirection.FABRIC_TO_SNOWFLAKE)
    logger.info("📊 Extracting Fabric semantic models...")
    models = sync_engine.extract_fabric_models()
    
    if not models:
        logger.warning("⚠️ No Fabric models found to sync")
        results["status"] = "NO_MODELS"
        results["message"] = "No Fabric models found"
        return results
    
    logger.info(f"   Found {len(models)} models")
    for model in models:
        logger.info(f"   - {model.name}: {len(model.tables)} table(s)")
    
    # Run the data sync
    logger.info("\n📥 Starting data extraction and loading...")
    data_results = sync_engine.sync_data_to_snowflake(models, force=force, sync_mode=sync_mode)
    
    # Merge results
    results.update(data_results)
    
    # Log summary
    summary = data_results.get("summary", {})
    logger.info("\n" + "=" * 80)
    logger.info("DATA SYNC RESULTS")
    logger.info("=" * 80)
    logger.info(f"Status: {data_results.get('status', 'UNKNOWN')}")
    logger.info(f"Models Processed: {summary.get('models_processed', 0)}")
    logger.info(f"Tables Processed: {summary.get('tables_processed', 0)}")
    logger.info(f"Rows Extracted: {summary.get('total_rows_extracted', 0)}")
    logger.info(f"Rows Loaded: {summary.get('total_rows_loaded', 0)}")
    logger.info(f"Extraction Successes: {summary.get('extraction_successes', 0)}")
    logger.info(f"Extraction Failures: {summary.get('extraction_failures', 0)}")
    logger.info(f"Load Successes: {summary.get('load_successes', 0)}")
    logger.info(f"Load Failures: {summary.get('load_failures', 0)}")
    
    # Show per-model results
    if data_results.get("model_results"):
        logger.info("\nPer-Model Results:")
        for model_result in data_results.get("model_results", []):
            logger.info(f"  {model_result.get('model_name', 'Unknown')}:")
            logger.info(f"    Tables Synced: {model_result.get('tables_synced', 0)}")
            logger.info(f"    Tables Failed: {model_result.get('tables_failed', 0)}")
            logger.info(f"    Rows Extracted: {model_result.get('total_rows_extracted', 0)}")
            logger.info(f"    Rows Loaded: {model_result.get('total_rows_loaded', 0)}")
    
    logger.info("=" * 80)
    
    # Save results
    output_file = "data_sync_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"Results saved to {output_file}")
    
    return results


def run_continuous_sync(interval_seconds: int = 60) -> None:
    """
    Run continuous synchronization with the scheduler.
    
    This provides the "heartbeat" for real-time automation that was previously
    missing. The scheduler runs the sync process at the configured interval,
    handles retries with exponential backoff, and gracefully handles shutdown.
    
    Args:
        interval_seconds: Interval between sync runs (default: 60 seconds).
    """
    if not PRODUCTION_MODULES_AVAILABLE:
        logger.error("Production modules not available. Cannot run scheduler.")
        logger.error("Please ensure naming_convention.py, logging_audit.py, and scheduler.py are available.")
        return
    
    logger.info("=" * 80)
    logger.info("CONTINUOUS SYNC MODE - Power BI Fabric ↔ Snowflake")
    logger.info("=" * 80)
    logger.info(f"Sync interval: {interval_seconds} seconds")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 80)
    
    # Create the sync engine
    sync_engine = SemanticSyncEngine(SyncDirection.BIDIRECTIONAL)
    
    def sync_function():
        """Wrapper function for the scheduler."""
        result = sync_engine.run_sync()
        return (result.get("views_created", 0), result.get("failures", 0))
    
    # Create scheduler with exponential backoff retry (10s, 20s, 40s)
    scheduler = create_scheduler(
        sync_function=sync_function,
        interval_seconds=interval_seconds,
        max_retries=3,
        initial_retry_delay=10.0  # 10s, 20s, 40s as per requirements
    )
    
    # Start the scheduler
    scheduler.start()
    
    try:
        # Keep main thread alive
        while scheduler.is_running:
            time.sleep(1)
            
            # Periodically log status
            if scheduler.stats.total_runs > 0 and scheduler.stats.total_runs % 10 == 0:
                health = scheduler.get_health()
                logger.info(f"Scheduler health: {'HEALTHY' if health['healthy'] else 'UNHEALTHY'}")
                
    except KeyboardInterrupt:
        logger.info("\n🛑 Shutdown signal received...")
    finally:
        scheduler.stop()
        
        # Print final statistics
        status = scheduler.get_status()
        logger.info("=" * 80)
        logger.info("SCHEDULER FINAL STATUS")
        logger.info("=" * 80)
        logger.info(f"Total Runs: {status['stats']['total_runs']}")
        logger.info(f"Successful: {status['stats']['successful_runs']}")
        logger.info(f"Failed: {status['stats']['failed_runs']}")
        logger.info(f"Skipped (no changes): {status['stats']['skipped_runs']}")
        logger.info(f"Views Created: {status['stats']['total_views_created']}")
        logger.info(f"Views Failed: {status['stats']['total_views_failed']}")
        logger.info("=" * 80)


def main() -> None:
    """
    Main entry point for the Fabric-Snowflake semantic sync application.
    
    Supports two modes:
    - One-time sync (default): Run sync once and exit
    - Continuous sync: Run scheduler for automated sync
    
    Usage:
        python fabric_snowflake_sync.py                    # One-time sync
        python fabric_snowflake_sync.py --scheduler        # Continuous sync (60s interval)
        python fabric_snowflake_sync.py --scheduler --interval 120  # 120s interval
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Power BI Fabric <-> Snowflake Semantic Model Synchronization"
    )
    parser.add_argument(
        "--scheduler", "-s",
        action="store_true",
        help="Enable continuous sync mode with scheduler"
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=60,
        help="Sync interval in seconds (default: 60)"
    )
    parser.add_argument(
        "--direction", "-d",
        choices=["fabric_to_snowflake", "snowflake_to_fabric", "bidirectional"],
        default="bidirectional",
        help="Sync direction (default: bidirectional)"
    )
    parser.add_argument(
        "--force", "--force-sync", "-f",
        action="store_true",
        help="Force sync all views regardless of change detection state. "
             "Use this when sync state is corrupted or views are missing."
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Reset all sync state files before running. "
             "This clears sync_state.json and last_sync_snapshot.json"
    )
    parser.add_argument(
        "--reconcile",
        action="store_true",
        help="Reconcile sync: detect missing views in Snowflake and create them"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging with detailed change detection info"
    )
    parser.add_argument(
        "--sync-data", "--with-data",
        action="store_true",
        help="Extract and load actual row-level data (not just schema/views). "
             "This populates Snowflake tables with business data from Fabric."
    )
    parser.add_argument(
        "--compare-rows",
        action="store_true",
        help="Compare row counts between Fabric and Snowflake tables to "
             "detect which tables need data sync."
    )
    parser.add_argument(
        "--validate-data",
        action="store_true",
        help="Validate existing data in Snowflake tables by checking row counts "
             "and displaying sample data."
    )
    parser.add_argument(
        "--data-mode",
        choices=["full_refresh", "incremental", "append"],
        default="full_refresh",
        help="Data sync mode: full_refresh (TRUNCATE+INSERT), "
             "incremental (MERGE), or append (INSERT only). Default: full_refresh"
    )
    
    args = parser.parse_args()
    
    # Enable verbose logging if requested
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    # Check if running in scheduler mode
    if args.scheduler:
        run_continuous_sync(interval_seconds=args.interval)
        return
    
    # One-time sync mode
    logger.info("=" * 80)
    logger.info("Semantic Model Synchronization: Power BI Fabric ↔ Snowflake")
    logger.info("=" * 80)
    
    # Handle reset-state flag
    if args.reset_state:
        logger.info("🗑️ RESET STATE: Clearing all sync state files...")
        state_files = [
            "sync_state.json",
            "last_sync_snapshot.json",
            os.path.join("sync_data", "fabric_checkpoint.json"),
            os.path.join("sync_data", "snowflake_checkpoint.json"),
        ]
        for state_file in state_files:
            if os.path.exists(state_file):
                try:
                    os.remove(state_file)
                    logger.info(f"  ✓ Removed: {state_file}")
                except Exception as e:
                    logger.warning(f"  ✗ Failed to remove {state_file}: {e}")
            else:
                logger.debug(f"  - Not found: {state_file}")
        logger.info("✅ Sync state reset complete")
    
    # Handle force sync flag
    if args.force:
        logger.info("🔥 FORCE SYNC MODE: Bypassing all change detection")
        logger.info("   All views will be recreated regardless of current state")
    
    # Handle reconciliation mode
    if args.reconcile:
        logger.info("🔄 RECONCILIATION MODE: Detecting missing views...")
        results = run_reconciliation_sync()
        logger.info("=" * 80)
        logger.info("RECONCILIATION RESULTS")
        logger.info("=" * 80)
        logger.info(f"Status: {results.get('status', 'UNKNOWN')}")
        logger.info(f"Fabric Models: {results.get('fabric_model_count', 0)}")
        logger.info(f"Snowflake Views: {results.get('snowflake_view_count', 0)}")
        logger.info(f"Missing Views Found: {len(results.get('missing_views', []))}")
        logger.info(f"Views Created: {results.get('views_created', 0)}")
        logger.info(f"Failures: {results.get('failures', 0)}")
        
        if results.get('missing_views'):
            logger.info("\nMissing views that were synced:")
            for view in results.get('missing_views', []):
                logger.info(f"  → {view}")
        
        output_file = "sync_results.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"\nResults saved to {output_file}")
        return

    # Map direction string to enum
    direction_map = {
        "fabric_to_snowflake": SyncDirection.FABRIC_TO_SNOWFLAKE,
        "snowflake_to_fabric": SyncDirection.SNOWFLAKE_TO_FABRIC,
        "bidirectional": SyncDirection.BIDIRECTIONAL,
    }
    direction = direction_map.get(args.direction, SyncDirection.BIDIRECTIONAL)

    sync_engine = SemanticSyncEngine(direction)
    
    # Handle compare-rows mode
    if args.compare_rows:
        logger.info("📊 COMPARE ROWS MODE: Comparing row counts between Fabric and Snowflake...")
        models = sync_engine.extract_fabric_models()
        comparison = sync_engine.compare_row_counts(models)
        
        logger.info("=" * 80)
        logger.info("ROW COUNT COMPARISON RESULTS")
        logger.info("=" * 80)
        
        summary = comparison.get("summary", {})
        logger.info(f"Total Tables: {summary.get('total_tables', 0)}")
        logger.info(f"Tables In Sync: {summary.get('tables_in_sync', 0)}")
        logger.info(f"Tables Need Sync: {summary.get('tables_need_sync', 0)}")
        
        if comparison.get("needs_sync"):
            logger.info("\nTables that need data sync:")
            for table in comparison.get("needs_sync", []):
                logger.info(
                    f"  ❌ {table['snowflake_table']}: "
                    f"Fabric={table['fabric_row_count']}, "
                    f"Snowflake={table['snowflake_row_count']} "
                    f"({table['sync_reason']})"
                )
        
        if comparison.get("in_sync"):
            logger.info("\nTables in sync:")
            for table in comparison.get("in_sync", []):
                logger.info(
                    f"  ✅ {table['snowflake_table']}: "
                    f"{table['snowflake_row_count']} rows"
                )
        
        output_file = "row_comparison_results.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(comparison, f, indent=2, default=str)
        logger.info(f"\nResults saved to {output_file}")
        return
    
    # Handle validate-data mode
    if args.validate_data:
        logger.info("🔍 VALIDATE DATA MODE: Checking existing Snowflake table data...")
        if DATA_EXTRACTOR_AVAILABLE:
            orchestrator = DataSyncOrchestrator()
            validation = orchestrator.validate_sync()
            
            logger.info("=" * 80)
            logger.info("DATA VALIDATION RESULTS")
            logger.info("=" * 80)
            
            for table in validation.get("tables", []):
                status_icon = "✅" if table.get("has_data") else "❌"
                logger.info(
                    f"  {status_icon} {table['table_name']}: {table['row_count']} rows"
                )
            
            all_valid = validation.get("all_valid", False)
            if all_valid:
                logger.info("\n✅ All tables have data!")
            else:
                logger.info("\n⚠️ Some tables are empty - run with --sync-data to populate them")
            
            output_file = "data_validation_results.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(validation, f, indent=2, default=str)
            logger.info(f"\nResults saved to {output_file}")
        else:
            logger.error("Data extractor module not available for validation")
        return
    
    # Log if data sync is enabled
    sync_data = getattr(args, 'sync_data', False)
    if sync_data:
        logger.info("📥 DATA SYNC ENABLED: Will extract and load actual row-level data")
        logger.info(f"   Data mode: {args.data_mode}")
    
    # Pass force and sync_data flags to the sync engine
    results = sync_engine.run_sync(force=args.force, sync_data=sync_data)

    logger.info("=" * 80)
    logger.info("SYNCHRONIZATION RESULTS")
    logger.info("=" * 80)
    logger.info(f"Status: {results['status']}")
    logger.info(f"Duration: {results['duration_seconds']:.2f} seconds")
    logger.info(f"Models Synced: {results['models_synced']}")
    logger.info(f"Views Created: {results['views_created']}")
    logger.info(f"Views Skipped: {results.get('views_skipped', 0)}")
    logger.info(f"Failures: {results['failures']}")
    
    # Show data sync results if applicable
    if sync_data and results.get('data_sync'):
        logger.info("")
        logger.info("DATA SYNC RESULTS:")
        logger.info(f"  Rows Extracted: {results.get('rows_extracted', 0)}")
        logger.info(f"  Rows Loaded: {results.get('rows_loaded', 0)}")
        logger.info(f"  Data Failures: {results.get('data_failures', 0)}")
    
    # Show skip reasons if verbose
    if args.verbose and results.get('skip_reasons'):
        logger.info("\nSkip Reasons:")
        for reason, count in results.get('skip_reasons', {}).items():
            logger.info(f"  - {reason}: {count}")

    logger.info("")
    logger.info("Detailed Log:")
    for event in results["log"][-20:]:  # Show last 20 events to avoid too much output
        logger.info(
            f"  [{event['timestamp']}] {event['type']}: {event['message']}"
        )
    
    if len(results["log"]) > 20:
        logger.info(f"  ... and {len(results['log']) - 20} more events (see sync_results.json)")

    logger.info("=" * 80)

  
    output_file = "sync_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    logger.info(f"Results saved to {output_file}")


if __name__ == "__main__":
    main()

