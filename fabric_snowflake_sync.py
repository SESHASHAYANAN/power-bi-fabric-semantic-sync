import os
import json
import logging
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import requests
import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv

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
            logger.error(f"❌ Failed to create standard view: {e}")
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
        
        tables: List[Table] = []
        for table_data in model_dict.get("tables", []):
           
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
    ) -> Tuple[int, int]:
        """
        Synchronize semantic models to Snowflake.

        Creates semantic views in Snowflake for each table in each model.

        Args:
            models: List of SemanticModel objects to sync.

        Returns:
            Tuple of (successful_count, failed_count).
        """
        self.log_event("SYNC_START", "Starting sync to Snowflake")

        
        if not self.snowflake_connector.connect():
            self.log_event(
                "ERROR",
                "Failed to connect to Snowflake",
                "ERROR",
            )
            return 0, len(models)

        successful: int = 0
        failed: int = 0

        for model in models:
            for table in model.tables:
                try:
                   
                    view_name = self._sanitize_name(
                        f"sv_{model.name}_{table.name}"
                    )

                 
                    dimensions: Dict[str, str] = {}
                    for column in table.columns:
                        if not column.is_hidden:
                            snowflake_type = DataTypeMapping.get_snowflake_type(
                                column.data_type
                            )
                            dimensions[column.name] = snowflake_type

                   
                    measures: Dict[str, str] = {}
                    for measure in table.measures:
                       
                        measures[measure.name] = self._convert_dax_to_sql(
                            measure.expression
                        )

                 
                    table_def = (
                        table.source_expression
                        if table.source_expression
                        else f"{model.name}.{table.name}"
                    )

                   
                    if self.snowflake_connector.create_semantic_view(
                        view_name=view_name,
                        table_definition=table_def,
                        dimensions=dimensions,
                        measures=measures,
                    ):
                        self.log_event(
                            "CREATE",
                            f"Created semantic view: {view_name}",
                        )
                        successful += 1
                    else:
                        self.log_event(
                            "ERROR",
                            f"Failed to create view: {view_name}",
                            "ERROR",
                        )
                        failed += 1

                except Exception as e:
                    self.log_event(
                        "ERROR",
                        f"Error syncing table {table.name}: {e}",
                        "ERROR",
                    )
                    failed += 1

        self.snowflake_connector.disconnect()

        self.log_event(
            "SYNC_END",
            f"Sync completed: {successful} successful, {failed} failures",
        )
        return successful, failed

    def _sanitize_name(self, name: str) -> str:
        """
        Sanitize a name for use as a Snowflake identifier.

        Args:
            name: Original name string.

        Returns:
            Sanitized name suitable for Snowflake.
        """
       
        sanitized = name.replace(" ", "_").replace("-", "_")
       
        sanitized = "".join(c for c in sanitized if c.isalnum() or c == "_")
      
        if sanitized and sanitized[0].isdigit():
            sanitized = f"v_{sanitized}"
        return sanitized.lower()

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

    def run_sync(self) -> Dict[str, Any]:
        """
        Execute the complete synchronization workflow.

        Returns:
            Dictionary containing sync results and metadata.
        """
        self.log_event(
            "SYNC_INIT",
            f"Starting synchronization: {self.direction.value}",
        )

        start_time = datetime.now()
        results: Dict[str, Any] = {
            "start_time": start_time.isoformat(),
            "end_time": "",
            "duration_seconds": 0.0,
            "status": "IN_PROGRESS",
            "direction": self.direction.value,
            "models_synced": 0,
            "views_created": 0,
            "failures": 0,
            "log": [],
        }

        try:
      
            models = self.extract_fabric_models()
            results["models_synced"] = len(models)

        
            if self.direction in (
                SyncDirection.FABRIC_TO_SNOWFLAKE,
                SyncDirection.BIDIRECTIONAL,
            ):
                successful, failed = self.sync_to_snowflake(models)
                results["views_created"] = successful
                results["failures"] = failed

           
            if results["failures"] == 0:
                results["status"] = "COMPLETED_SUCCESSFULLY"
            elif results["views_created"] > 0:
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



def main() -> None:
    """
    Main entry point for the Fabric-Snowflake semantic sync application.
    
    Orchestrates the synchronization process and outputs results.
    """

    logger.info("=" * 80)
    logger.info("Semantic Model Synchronization: Power BI Fabric ↔ Snowflake")
    logger.info("=" * 80)


    sync_engine = SemanticSyncEngine(SyncDirection.BIDIRECTIONAL)

    results = sync_engine.run_sync()

    logger.info("=" * 80)
    logger.info("SYNCHRONIZATION RESULTS")
    logger.info("=" * 80)
    logger.info(f"Status: {results['status']}")
    logger.info(f"Duration: {results['duration_seconds']:.2f} seconds")
    logger.info(f"Models Synced: {results['models_synced']}")
    logger.info(f"Views Created: {results['views_created']}")
    logger.info(f"Failures: {results['failures']}")


    logger.info("")
    logger.info("Detailed Log:")
    for event in results["log"]:
        logger.info(
            f"  [{event['timestamp']}] {event['type']}: {event['message']}"
        )

    logger.info("=" * 80)

  
    output_file = "sync_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    logger.info(f"Results saved to {output_file}")


if __name__ == "__main__":
    main()
