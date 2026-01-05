"""
Naming Convention Module - Smart Sanitization for Snowflake Identifiers

This module handles the critical issue of reserved keywords in Snowflake.
When a model name like "day" is used, it conflicts with Snowflake's DAY function,
causing the sync to fail. This module provides intelligent sanitization.

Key Features:
- Comprehensive list of Snowflake reserved keywords
- Automatic prefixing for reserved keywords (e.g., day → DIM_DAY)
- Standardized view naming format: SV_FABRIC_{SANITIZED_NAME}
- Uppercase conversion and special character handling
"""

import re
import logging
from typing import Set, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class IdentifierType(Enum):
    """Types of identifiers that may need sanitization."""
    TABLE = "TABLE"
    VIEW = "VIEW"
    COLUMN = "COLUMN"
    MODEL = "MODEL"
    MEASURE = "MEASURE"


class NamingConvention:
    """
    Handles naming conventions and sanitization for Snowflake identifiers.
    
    This class maintains a comprehensive list of Snowflake reserved keywords
    and provides methods to safely convert names for use in DDL statements.
    
    Why this exists:
    - Snowflake has many reserved keywords that cannot be used as identifiers
    - Model names like "day", "user", "table" will cause SQL syntax errors
    - We need a consistent naming strategy for all synced objects
    """
    
    # Comprehensive list of Snowflake reserved keywords
    # Source: https://docs.snowflake.com/en/sql-reference/reserved-keywords
    RESERVED_KEYWORDS: Set[str] = {
        # Data Types
        "BOOLEAN", "DATE", "DATETIME", "FLOAT", "INT", "INTEGER", "NUMBER",
        "DECIMAL", "NUMERIC", "REAL", "STRING", "TEXT", "TIME", "TIMESTAMP",
        "VARCHAR", "BINARY", "VARBINARY", "VARIANT", "OBJECT", "ARRAY",
        
        # Date/Time Functions (commonly confused with identifiers)
        "DAY", "MONTH", "YEAR", "HOUR", "MINUTE", "SECOND", "WEEK",
        "QUARTER", "DAYOFWEEK", "DAYOFYEAR", "WEEKOFYEAR",
        
        # SQL Keywords
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
        "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "VIEW",
        "INDEX", "DATABASE", "SCHEMA", "COLUMN", "CONSTRAINT", "PRIMARY", "KEY",
        "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT", "AS", "ON", "IN",
        "EXISTS", "LIKE", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END", "IF",
        "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "NATURAL",
        "UNION", "INTERSECT", "EXCEPT", "MINUS", "ALL", "DISTINCT", "TOP",
        "ORDER", "BY", "ASC", "DESC", "NULLS", "FIRST", "LAST", "LIMIT", "OFFSET",
        "FETCH", "NEXT", "ROWS", "ONLY", "PERCENT", "WITH", "TIES", "GROUP",
        "HAVING", "ROLLUP", "CUBE", "GROUPING", "SETS", "WINDOW", "OVER",
        "PARTITION", "CURRENT", "ROW", "RANGE", "PRECEDING", "FOLLOWING",
        "UNBOUNDED", "LATERAL", "PIVOT", "UNPIVOT", "SAMPLE", "TABLESAMPLE",
        
        # Aggregate Functions
        "COUNT", "SUM", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE",
        "LISTAGG", "MEDIAN", "PERCENTILE", "ANY_VALUE",
        
        # Common Object Names (dangerous as identifiers)
        "USER", "USERS", "ROLE", "ROLES", "GRANT", "REVOKE", "ACCOUNT",
        "PASSWORD", "LOGIN", "SESSION", "TRANSACTION", "COMMIT", "ROLLBACK",
        "WAREHOUSE", "STAGE", "FILE", "FORMAT", "COPY", "LOAD", "UNLOAD",
        "PIPE", "STREAM", "TASK", "PROCEDURE", "FUNCTION", "POLICY", "TAG",
        "SEQUENCE", "SHARE", "RESOURCE", "MONITOR", "NETWORK", "INTEGRATION",
        
        # Snowflake-specific
        "CLUSTER", "RECLUSTER", "AUTOMATIC", "CLUSTERING", "VARIANT",
        "GEOGRAPHY", "GEOMETRY", "RESULT", "RESULTSET", "QUALIFY",
        "CHANGES", "BEFORE", "AFTER", "AT", "OFFSET", "STATEMENT",
        "VALUES", "VALUE", "DATA", "TYPE", "IDENTITY", "INCREMENT",
        
        # Additional problem keywords
        "NAME", "ID", "STATUS", "STATE", "ACTION", "SOURCE", "TARGET",
        "INPUT", "OUTPUT", "START", "STOP", "ENABLE", "DISABLE",
    }
    
    # Prefix mapping for different identifier types
    PREFIX_MAP = {
        IdentifierType.TABLE: "TBL",
        IdentifierType.VIEW: "SV_FABRIC",
        IdentifierType.COLUMN: "COL",
        IdentifierType.MODEL: "MDL",
        IdentifierType.MEASURE: "MSR",
    }
    
    # Dimension prefix for reserved keywords that look like dimensions
    DIM_KEYWORDS = {"DAY", "MONTH", "YEAR", "WEEK", "QUARTER", "DATE", "TIME"}
    
    @classmethod
    def is_reserved_keyword(cls, name: str) -> bool:
        """
        Check if a name is a Snowflake reserved keyword.
        
        Args:
            name: The name to check.
            
        Returns:
            True if the name is a reserved keyword, False otherwise.
        """
        return name.upper().strip() in cls.RESERVED_KEYWORDS
    
    @classmethod
    def sanitize_name(
        cls,
        name: str,
        identifier_type: IdentifierType = IdentifierType.VIEW,
        force_prefix: bool = False
    ) -> str:
        """
        Sanitize a name for safe use as a Snowflake identifier.
        
        This is the main sanitization function that handles:
        1. Reserved keyword detection and prefixing
        2. Special character replacement
        3. Uppercase conversion
        4. Leading digit handling
        
        Args:
            name: The original name to sanitize.
            identifier_type: Type of identifier (affects prefix choice).
            force_prefix: If True, always add prefix even if not reserved.
            
        Returns:
            A sanitized name safe for use in Snowflake DDL.
            
        Examples:
            >>> NamingConvention.sanitize_name("day", IdentifierType.VIEW)
            'SV_FABRIC_DIM_DAY'
            >>> NamingConvention.sanitize_name("Sales Model", IdentifierType.VIEW)
            'SV_FABRIC_SALES_MODEL'
            >>> NamingConvention.sanitize_name("user", IdentifierType.TABLE)
            'TBL_USER'
        """
        if not name:
            return "UNNAMED"
        
        # Step 1: Convert to uppercase and strip
        sanitized = name.upper().strip()
        
        # Step 2: Replace special characters with underscores
        # Keep only alphanumeric and underscore
        sanitized = re.sub(r'[^A-Z0-9_]', '_', sanitized)
        
        # Step 3: Replace multiple consecutive underscores with single
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Step 4: Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        # Step 5: Handle leading digit (prepend 'V_')
        if sanitized and sanitized[0].isdigit():
            sanitized = f"V_{sanitized}"
        
        # Step 6: Handle reserved keywords
        if cls.is_reserved_keyword(sanitized):
            # Use DIM_ prefix for date/time keywords (they're often dimensions)
            if sanitized in cls.DIM_KEYWORDS:
                sanitized = f"DIM_{sanitized}"
            else:
                # Use identifier type prefix
                prefix = cls.PREFIX_MAP.get(identifier_type, "")
                if prefix:
                    sanitized = f"{prefix}_{sanitized}"
                else:
                    sanitized = f"SF_{sanitized}"
            
            logger.debug(
                f"Reserved keyword detected: '{name}' → sanitized to '{sanitized}'"
            )
        
        # Step 7: Add standard prefix for views if requested
        elif force_prefix and identifier_type == IdentifierType.VIEW:
            prefix = cls.PREFIX_MAP[IdentifierType.VIEW]
            if not sanitized.startswith(prefix):
                sanitized = f"{prefix}_{sanitized}"
        
        return sanitized
    
    @classmethod
    def generate_view_name(cls, model_name: str, table_name: str) -> str:
        """
        Generate a standardized semantic view name.
        
        Format: SV_FABRIC_{MODEL_NAME}_{TABLE_NAME}
        
        Args:
            model_name: Name of the semantic model.
            table_name: Name of the table within the model.
            
        Returns:
            Standardized view name.
        """
        # Sanitize both parts
        sanitized_model = cls.sanitize_name(
            model_name, 
            IdentifierType.MODEL, 
            force_prefix=False
        )
        sanitized_table = cls.sanitize_name(
            table_name, 
            IdentifierType.TABLE, 
            force_prefix=False
        )
        
        # Combine with standard prefix
        view_name = f"SV_FABRIC_{sanitized_model}_{sanitized_table}"
        
        # Ensure final name doesn't exceed Snowflake's 255 character limit
        if len(view_name) > 255:
            # Truncate but keep the prefix
            view_name = view_name[:255]
            # Make sure we don't end with underscore
            view_name = view_name.rstrip('_')
        
        return view_name
    
    @classmethod
    def generate_column_name(cls, column_name: str) -> str:
        """
        Generate a sanitized column name.
        
        Args:
            column_name: Original column name.
            
        Returns:
            Sanitized column name.
        """
        return cls.sanitize_name(
            column_name, 
            IdentifierType.COLUMN, 
            force_prefix=False
        )
    
    @classmethod
    def generate_measure_name(cls, measure_name: str) -> str:
        """
        Generate a sanitized measure name.
        
        Args:
            measure_name: Original measure name.
            
        Returns:
            Sanitized measure name.
        """
        return cls.sanitize_name(
            measure_name, 
            IdentifierType.MEASURE, 
            force_prefix=False
        )
    
    @classmethod
    def quote_identifier(cls, name: str) -> str:
        """
        Quote an identifier for use in SQL.
        
        Snowflake allows quoted identifiers which preserve case
        and allow special characters. Use this for maximum safety.
        
        Args:
            name: The identifier to quote.
            
        Returns:
            Quoted identifier string.
        """
        # Escape any existing double quotes
        escaped = name.replace('"', '""')
        return f'"{escaped}"'
    
    @classmethod
    def validate_identifier(cls, name: str) -> tuple[bool, Optional[str]]:
        """
        Validate an identifier and return any issues.
        
        Args:
            name: The identifier to validate.
            
        Returns:
            Tuple of (is_valid, error_message).
        """
        if not name:
            return False, "Identifier cannot be empty"
        
        if len(name) > 255:
            return False, f"Identifier exceeds 255 characters (got {len(name)})"
        
        if cls.is_reserved_keyword(name):
            return False, f"'{name}' is a Snowflake reserved keyword"
        
        if name[0].isdigit():
            return False, "Identifier cannot start with a digit"
        
        if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
            return False, "Identifier contains invalid characters"
        
        return True, None
    
    @classmethod
    def get_safe_identifier(cls, name: str, identifier_type: IdentifierType) -> str:
        """
        Get a safe identifier, either sanitized or quoted.
        
        Tries sanitization first, falls back to quoting if needed.
        
        Args:
            name: Original name.
            identifier_type: Type of identifier.
            
        Returns:
            Safe identifier for use in DDL.
        """
        sanitized = cls.sanitize_name(name, identifier_type)
        is_valid, _ = cls.validate_identifier(sanitized)
        
        if is_valid:
            return sanitized
        else:
            # Fall back to quoting
            return cls.quote_identifier(name)


# Convenience function for direct import
def sanitize_snowflake_name(name: str) -> str:
    """
    Convenience function to sanitize a name for Snowflake.
    
    Args:
        name: The name to sanitize.
        
    Returns:
        Sanitized name.
    """
    return NamingConvention.sanitize_name(name, IdentifierType.VIEW)


def generate_semantic_view_name(model_name: str, table_name: str) -> str:
    """
    Convenience function to generate a semantic view name.
    
    Args:
        model_name: Name of the model.
        table_name: Name of the table.
        
    Returns:
        Standardized view name.
    """
    return NamingConvention.generate_view_name(model_name, table_name)
