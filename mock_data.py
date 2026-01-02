"""
Mock Data Module for Testing Fabric-Snowflake Sync.

Provides sample data and helper functions for testing without live connections.
"""

from typing import Any, Dict, List
from datetime import datetime


# =============================================================================
# SAMPLE FABRIC API RESPONSES
# =============================================================================


def get_sample_fabric_model() -> Dict[str, Any]:
    """
    Get a sample Fabric semantic model API response.
    
    Returns:
        Dictionary mimicking Fabric API response.
    """
    return {
        "id": "model-001",
        "name": "SalesModel",
        "displayName": "Sales Analytics Model",
        "workspaceId": "ws-001",
        "description": "Sales analytics semantic model",
        "createdDate": "2025-01-01T00:00:00Z",
        "modifiedDate": datetime.now().isoformat(),
        "tables": [
            {
                "name": "Sales",
                "displayName": "Sales",
                "description": "Sales transactions table",
                "isHidden": False,
                "source": {
                    "expression": "Snowflake.Database('account.snowflakecomputing.com', 'ANALYTICS_DB', 'SALES')"
                },
                "columns": [
                    {
                        "name": "SalesID",
                        "displayName": "Sales ID",
                        "dataType": "int64",
                        "isHidden": False,
                        "description": "Primary key"
                    },
                    {
                        "name": "ProductID",
                        "displayName": "Product ID",
                        "dataType": "int64",
                        "isHidden": False,
                    },
                    {
                        "name": "CustomerID",
                        "displayName": "Customer ID",
                        "dataType": "int64",
                        "isHidden": False,
                    },
                    {
                        "name": "Amount",
                        "displayName": "Amount",
                        "dataType": "decimal",
                        "isHidden": False,
                    },
                    {
                        "name": "Quantity",
                        "displayName": "Quantity",
                        "dataType": "int32",
                        "isHidden": False,
                    },
                    {
                        "name": "SalesDate",
                        "displayName": "Sales Date",
                        "dataType": "datetime",
                        "isHidden": False,
                    },
                ],
                "measures": [
                    {
                        "name": "TotalRevenue",
                        "displayName": "Total Revenue",
                        "expression": "SUM([Amount])",
                        "formatString": "$#,##0.00",
                        "description": "Sum of all sales amounts"
                    },
                    {
                        "name": "TotalQuantity",
                        "displayName": "Total Quantity",
                        "expression": "SUM([Quantity])",
                        "formatString": "#,##0",
                        "description": "Sum of all quantities sold"
                    },
                    {
                        "name": "AverageOrderValue",
                        "displayName": "Average Order Value",
                        "expression": "AVERAGE([Amount])",
                        "formatString": "$#,##0.00",
                        "description": "Average amount per order"
                    },
                    {
                        "name": "OrderCount",
                        "displayName": "Order Count",
                        "expression": "COUNTROWS([Sales])",
                        "formatString": "#,##0",
                        "description": "Number of orders"
                    },
                ],
            },
            {
                "name": "Products",
                "displayName": "Products",
                "description": "Product catalog",
                "isHidden": False,
                "source": {
                    "expression": "Snowflake.Database('account.snowflakecomputing.com', 'ANALYTICS_DB', 'PRODUCTS')"
                },
                "columns": [
                    {
                        "name": "ProductID",
                        "displayName": "Product ID",
                        "dataType": "int64",
                        "isHidden": False,
                    },
                    {
                        "name": "ProductName",
                        "displayName": "Product Name",
                        "dataType": "string",
                        "isHidden": False,
                    },
                    {
                        "name": "Category",
                        "displayName": "Category",
                        "dataType": "string",
                        "isHidden": False,
                    },
                    {
                        "name": "UnitPrice",
                        "displayName": "Unit Price",
                        "dataType": "decimal",
                        "isHidden": False,
                    },
                ],
                "measures": [
                    {
                        "name": "ProductCount",
                        "displayName": "Product Count",
                        "expression": "COUNTROWS([Products])",
                        "formatString": "#,##0",
                    },
                ],
            },
        ],
        "relationships": [
            {
                "name": "Sales_Products",
                "fromTable": "Sales",
                "fromColumn": "ProductID",
                "toTable": "Products",
                "toColumn": "ProductID",
                "cardinality": "Many-to-One",
                "crossFilteringBehavior": "Both",
            },
        ],
    }


def get_sample_fabric_model_modified() -> Dict[str, Any]:
    """
    Get a modified version of the sample Fabric model.
    
    Changes:
    - TotalRevenue expression changed from SUM([Amount]) to SUM([Amount]) * 1.1
    - New measure added: RevenueWithTax
    
    Returns:
        Dictionary with modified model data.
    """
    model = get_sample_fabric_model()
    
    # Modify TotalRevenue expression
    for table in model["tables"]:
        if table["name"] == "Sales":
            for measure in table["measures"]:
                if measure["name"] == "TotalRevenue":
                    measure["expression"] = "SUM([Amount]) * 1.1"
                    measure["description"] = "Sum of all sales amounts with 10% markup"
            
            # Add new measure
            table["measures"].append({
                "name": "RevenueWithTax",
                "displayName": "Revenue With Tax",
                "expression": "SUM([Amount]) * 1.18",
                "formatString": "$#,##0.00",
                "description": "Revenue including 18% tax"
            })
    
    model["modifiedDate"] = datetime.now().isoformat()
    return model


# =============================================================================
# SAMPLE SNOWFLAKE RESPONSES
# =============================================================================


def get_sample_snowflake_view_data() -> Dict[str, Any]:
    """
    Get sample Snowflake semantic view data.
    
    Returns:
        Dictionary with view schema and measures.
    """
    return {
        "view_name": "SV_SALESMODEL_SALES",
        "columns": [
            {
                "name": "SALESID",
                "data_type": "NUMBER(19,0)",
                "is_hidden": False,
                "table_name": "SV_SALESMODEL_SALES",
            },
            {
                "name": "PRODUCTID",
                "data_type": "NUMBER(19,0)",
                "is_hidden": False,
                "table_name": "SV_SALESMODEL_SALES",
            },
            {
                "name": "CUSTOMERID",
                "data_type": "NUMBER(19,0)",
                "is_hidden": False,
                "table_name": "SV_SALESMODEL_SALES",
            },
            {
                "name": "AMOUNT",
                "data_type": "DECIMAL(18,2)",
                "is_hidden": False,
                "table_name": "SV_SALESMODEL_SALES",
            },
            {
                "name": "QUANTITY",
                "data_type": "INTEGER",
                "is_hidden": False,
                "table_name": "SV_SALESMODEL_SALES",
            },
            {
                "name": "SALESDATE",
                "data_type": "TIMESTAMP_NTZ",
                "is_hidden": False,
                "table_name": "SV_SALESMODEL_SALES",
            },
        ],
        "measures": [
            {
                "name": "TOTALREVENUE",
                "expression": "SUM(AMOUNT)",
                "format_string": "",
                "table_name": "SV_SALESMODEL_SALES",
            },
            {
                "name": "TOTALQUANTITY",
                "expression": "SUM(QUANTITY)",
                "format_string": "",
                "table_name": "SV_SALESMODEL_SALES",
            },
            {
                "name": "AVERAGEORDERVALUE",
                "expression": "AVG(AMOUNT)",
                "format_string": "",
                "table_name": "SV_SALESMODEL_SALES",
            },
            {
                "name": "ORDERCOUNT",
                "expression": "COUNT(*)",
                "format_string": "",
                "table_name": "SV_SALESMODEL_SALES",
            },
        ],
    }


def get_sample_snowflake_view_modified() -> Dict[str, Any]:
    """
    Get a modified version of the Snowflake view data.
    
    Changes:
    - TOTALREVENUE expression changed to SUM(AMOUNT) * 1.15
    - New measure added: DISCOUNTEDREVENUE
    
    Returns:
        Dictionary with modified view data.
    """
    view_data = get_sample_snowflake_view_data()
    
    # Modify TOTALREVENUE expression
    for measure in view_data["measures"]:
        if measure["name"] == "TOTALREVENUE":
            measure["expression"] = "SUM(AMOUNT) * 1.15"
    
    # Add new measure
    view_data["measures"].append({
        "name": "DISCOUNTEDREVENUE",
        "expression": "SUM(AMOUNT) * 0.9",
        "format_string": "",
        "table_name": "SV_SALESMODEL_SALES",
    })
    
    return view_data


def get_sample_snowflake_columns() -> List[Dict[str, Any]]:
    """
    Get sample Snowflake INFORMATION_SCHEMA.COLUMNS response.
    
    Returns:
        List of column dictionaries.
    """
    return [
        {"COLUMN_NAME": "SALESID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO", "COMMENT": None},
        {"COLUMN_NAME": "PRODUCTID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO", "COMMENT": None},
        {"COLUMN_NAME": "CUSTOMERID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "YES", "COMMENT": None},
        {"COLUMN_NAME": "AMOUNT", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "YES", "COMMENT": None},
        {"COLUMN_NAME": "QUANTITY", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "YES", "COMMENT": None},
        {"COLUMN_NAME": "SALESDATE", "DATA_TYPE": "TIMESTAMP_NTZ", "IS_NULLABLE": "YES", "COMMENT": None},
    ]


def get_sample_view_ddl() -> str:
    """
    Get sample Snowflake view DDL.
    
    Returns:
        DDL string for the semantic view.
    """
    return """
    CREATE OR REPLACE VIEW SEMANTIC_LAYER.SV_SALESMODEL_SALES AS
    SELECT
        SALESID,
        PRODUCTID,
        CUSTOMERID,
        AMOUNT,
        QUANTITY,
        SALESDATE,
        SUM(AMOUNT) AS TOTALREVENUE,
        SUM(QUANTITY) AS TOTALQUANTITY,
        AVG(AMOUNT) AS AVERAGEORDERVALUE,
        COUNT(*) AS ORDERCOUNT
    FROM ANALYTICS_DB.PUBLIC.SALES
    GROUP BY SALESID, PRODUCTID, CUSTOMERID, AMOUNT, QUANTITY, SALESDATE
    """


# =============================================================================
# MOCK CONNECTORS FOR TESTING
# =============================================================================


class MockFabricClient:
    """
    Mock Fabric API client for testing.
    
    Simulates Fabric API responses without requiring actual connection.
    """
    
    def __init__(self, use_modified: bool = False) -> None:
        """
        Initialize mock client.
        
        Args:
            use_modified: If True, return modified model data.
        """
        self.use_modified = use_modified
        self.authenticated = False
        self.workspace_id = "ws-mock-001"
    
    def authenticate(self) -> bool:
        """Simulate authentication."""
        self.authenticated = True
        return True
    
    def get_semantic_models(self) -> List[Dict[str, Any]]:
        """Return list of mock semantic models."""
        return [
            {
                "id": "model-001",
                "name": "SalesModel",
                "displayName": "Sales Analytics Model",
            }
        ]
    
    def get_semantic_model_detail(self, model_id: str) -> Dict[str, Any]:
        """Return mock model detail."""
        if self.use_modified:
            return get_sample_fabric_model_modified()
        return get_sample_fabric_model()
    
    def update_semantic_model_measure(
        self,
        model_id: str,
        table_name: str,
        measure_name: str,
        new_expression: str,
    ) -> bool:
        """Simulate updating a measure."""
        print(f"[MOCK] Updated measure {table_name}.{measure_name} = {new_expression}")
        return True


class MockSnowflakeConnector:
    """
    Mock Snowflake connector for testing.
    
    Simulates Snowflake responses without requiring actual connection.
    """
    
    def __init__(self, use_modified: bool = False) -> None:
        """
        Initialize mock connector.
        
        Args:
            use_modified: If True, return modified view data.
        """
        self.use_modified = use_modified
        self.connected = False
        self.schema = "SEMANTIC_LAYER"
        self.database = "ANALYTICS_DB"
    
    def connect(self) -> bool:
        """Simulate connection."""
        self.connected = True
        return True
    
    def disconnect(self) -> None:
        """Simulate disconnection."""
        self.connected = False
    
    def execute_query(
        self,
        query: str,
        fetch_all: bool = False,
    ) -> List[Dict[str, Any]]:
        """Return mock query results based on query type."""
        if "INFORMATION_SCHEMA.COLUMNS" in query:
            return get_sample_snowflake_columns()
        elif "GET_DDL" in query:
            return [{"DDL": get_sample_view_ddl()}]
        elif "INFORMATION_SCHEMA.TABLES" in query:
            return [{"TABLE_NAME": "SV_SALESMODEL_SALES"}]
        return []
    
    def get_semantic_views(self) -> List[str]:
        """Return list of mock semantic views."""
        return ["SV_SALESMODEL_SALES", "SV_SALESMODEL_PRODUCTS"]
    
    def create_semantic_view(
        self,
        view_name: str,
        table_definition: str,
        dimensions: Dict[str, str],
        measures: Dict[str, str],
    ) -> bool:
        """Simulate creating a semantic view."""
        print(f"[MOCK] Created view: {view_name}")
        return True
    
    def alter_view_measure(
        self,
        view_name: str,
        measure_name: str,
        new_expression: str,
    ) -> bool:
        """Simulate altering a view measure."""
        print(f"[MOCK] Updated view {view_name} measure {measure_name} = {new_expression}")
        return True


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def simulate_fabric_change(
    model_data: Dict[str, Any],
    table_name: str,
    measure_name: str,
    new_expression: str,
) -> Dict[str, Any]:
    """
    Simulate a measure change in Fabric model data.
    
    Args:
        model_data: Original model data.
        table_name: Table containing the measure.
        measure_name: Name of measure to modify.
        new_expression: New expression value.
    
    Returns:
        Modified model data.
    """
    import copy
    modified = copy.deepcopy(model_data)
    
    for table in modified["tables"]:
        if table["name"] == table_name:
            for measure in table["measures"]:
                if measure["name"] == measure_name:
                    measure["expression"] = new_expression
                    break
    
    modified["modifiedDate"] = datetime.now().isoformat()
    return modified


def simulate_snowflake_change(
    view_data: Dict[str, Any],
    measure_name: str,
    new_expression: str,
) -> Dict[str, Any]:
    """
    Simulate a measure change in Snowflake view data.
    
    Args:
        view_data: Original view data.
        measure_name: Name of measure to modify.
        new_expression: New expression value.
    
    Returns:
        Modified view data.
    """
    import copy
    modified = copy.deepcopy(view_data)
    
    for measure in modified["measures"]:
        if measure["name"] == measure_name:
            measure["expression"] = new_expression
            break
    
    return modified
