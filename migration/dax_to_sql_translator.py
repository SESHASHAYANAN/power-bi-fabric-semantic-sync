"""
DAX to SQL Translator - Phase 2 of Migration

Translates DAX measures and calculated columns to equivalent SQL statements:
- Aggregation functions (SUMX, AVERAGEX, COUNTX) → SQL aggregates
- CALCULATE/FILTER → WHERE clauses and CTEs
- Time intelligence (TOTALYTD, SAMEPERIODLASTYEAR) → SQL date functions
- RELATED/RELATEDTABLE → SQL JOINs
- ALL/VALUES → DISTINCT and GROUP BY

This enables removal of DAX dependencies and unified SQL-based logic.
"""

import re
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class DAXFunctionType(Enum):
    """Categories of DAX functions."""
    AGGREGATION = "aggregation"
    FILTER = "filter"
    TIME_INTELLIGENCE = "time_intelligence"
    RELATIONSHIP = "relationship"
    LOGICAL = "logical"
    TEXT = "text"
    MATH = "math"
    TABLE = "table"
    ITERATOR = "iterator"


@dataclass
class DAXMeasure:
    """Represents a DAX measure to be translated."""
    name: str
    expression: str
    table_name: str = ""
    format_string: str = ""
    description: str = ""
    dependencies: List[str] = field(default_factory=list)
    

@dataclass
class SQLExpression:
    """Represents the translated SQL expression."""
    sql: str
    requires_cte: bool = False
    cte_definitions: List[str] = field(default_factory=list)
    requires_joins: List[str] = field(default_factory=list)
    window_functions: bool = False
    comments: str = ""


@dataclass 
class TranslationResult:
    """Result of DAX to SQL translation."""
    original_dax: str
    translated_sql: str
    success: bool
    confidence: float = 1.0  # 0-1 confidence in translation
    warnings: List[str] = field(default_factory=list)
    manual_review_needed: bool = False
    sql_dialect: str = "snowflake"  # or 't-sql' for Fabric


class DAXToSQLTranslator:
    """
    Translates DAX expressions to equivalent SQL.
    
    Supports both Snowflake SQL and T-SQL (for Fabric Warehouse).
    Uses pattern matching and recursive parsing to handle complex expressions.
    """
    
    # DAX to Snowflake SQL function mappings
    SNOWFLAKE_FUNCTION_MAP = {
        # Simple aggregations
        'SUM': 'SUM',
        'AVERAGE': 'AVG',
        'COUNT': 'COUNT',
        'COUNTROWS': 'COUNT(*)',
        'MIN': 'MIN',
        'MAX': 'MAX',
        'DISTINCTCOUNT': 'COUNT(DISTINCT {0})',
        'COUNTBLANK': 'SUM(CASE WHEN {0} IS NULL THEN 1 ELSE 0 END)',
        'COUNTA': 'COUNT({0})',
        
        # Math functions
        'ABS': 'ABS',
        'ROUND': 'ROUND',
        'FLOOR': 'FLOOR',
        'CEILING': 'CEILING',
        'SQRT': 'SQRT',
        'POWER': 'POWER',
        'LOG': 'LOG',
        'LN': 'LN',
        'EXP': 'EXP',
        'MOD': 'MOD',
        'DIVIDE': 'DIV0NULL({0}, {1})',  # Snowflake-specific safe divide
        
        # Text functions
        'CONCATENATE': 'CONCAT',
        'LEFT': 'LEFT',
        'RIGHT': 'RIGHT',
        'MID': 'SUBSTR',
        'LEN': 'LENGTH',
        'UPPER': 'UPPER',
        'LOWER': 'LOWER',
        'TRIM': 'TRIM',
        'SUBSTITUTE': 'REPLACE',
        'FIND': 'POSITION',
        'REPLACE': 'REPLACE',
        'FORMAT': 'TO_VARCHAR',
        
        # Date functions
        'DATE': 'DATE_FROM_PARTS',
        'YEAR': 'YEAR',
        'MONTH': 'MONTH',
        'DAY': 'DAY',
        'WEEKDAY': 'DAYOFWEEK',
        'WEEKNUM': 'WEEKOFYEAR',
        'QUARTER': 'QUARTER',
        'TODAY': 'CURRENT_DATE()',
        'NOW': 'CURRENT_TIMESTAMP()',
        'DATEDIFF': 'DATEDIFF',
        'DATEADD': 'DATEADD',
        'EOMONTH': 'LAST_DAY',
        
        # Logical functions
        'IF': 'IFF',
        'AND': 'AND',
        'OR': 'OR',
        'NOT': 'NOT',
        'TRUE': 'TRUE',
        'FALSE': 'FALSE',
        'SWITCH': 'CASE',
        'BLANK': 'NULL',
        'ISBLANK': 'IS NULL',
        'COALESCE': 'COALESCE',
        'IFERROR': 'TRY_CAST'
    }
    
    # DAX to T-SQL (Fabric) function mappings
    TSQL_FUNCTION_MAP = {
        'SUM': 'SUM',
        'AVERAGE': 'AVG',
        'COUNT': 'COUNT',
        'COUNTROWS': 'COUNT(*)',
        'MIN': 'MIN',
        'MAX': 'MAX',
        'DISTINCTCOUNT': 'COUNT(DISTINCT {0})',
        'DIVIDE': 'CASE WHEN {1} = 0 THEN NULL ELSE {0} / {1} END',
        'IF': 'IIF',
        'CONCATENATE': 'CONCAT',
        'LEN': 'LEN',
        'TODAY': 'CAST(GETDATE() AS DATE)',
        'NOW': 'GETDATE()',
        'YEAR': 'YEAR',
        'MONTH': 'MONTH',
        'DAY': 'DAY',
        'DATEDIFF': 'DATEDIFF',
        'DATEADD': 'DATEADD',
        'EOMONTH': 'EOMONTH',
        'FORMAT': 'FORMAT',
        'ISBLANK': 'IS NULL',
        'BLANK': 'NULL',
        'SWITCH': 'CASE'
    }
    
    # Time Intelligence DAX to SQL patterns
    TIME_INTELLIGENCE_PATTERNS = {
        'TOTALYTD': {
            'pattern': r'TOTALYTD\s*\(\s*([^,]+)\s*,\s*([^,\)]+)\s*(?:,\s*"([^"]+)")?\)',
            'snowflake': '''
SUM({measure}) OVER (
    PARTITION BY YEAR({date_column})
    ORDER BY {date_column}
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)''',
            'tsql': '''
SUM({measure}) OVER (
    PARTITION BY YEAR({date_column})
    ORDER BY {date_column}
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)'''
        },
        'TOTALQTD': {
            'pattern': r'TOTALQTD\s*\(\s*([^,]+)\s*,\s*([^,\)]+)\s*\)',
            'snowflake': '''
SUM({measure}) OVER (
    PARTITION BY YEAR({date_column}), QUARTER({date_column})
    ORDER BY {date_column}
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)''',
            'tsql': '''
SUM({measure}) OVER (
    PARTITION BY YEAR({date_column}), DATEPART(QUARTER, {date_column})
    ORDER BY {date_column}
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)'''
        },
        'TOTALMTD': {
            'pattern': r'TOTALMTD\s*\(\s*([^,]+)\s*,\s*([^,\)]+)\s*\)',
            'snowflake': '''
SUM({measure}) OVER (
    PARTITION BY YEAR({date_column}), MONTH({date_column})
    ORDER BY {date_column}
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)''',
            'tsql': '''
SUM({measure}) OVER (
    PARTITION BY YEAR({date_column}), MONTH({date_column})
    ORDER BY {date_column}
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)'''
        },
        'SAMEPERIODLASTYEAR': {
            'pattern': r'SAMEPERIODLASTYEAR\s*\(\s*([^\)]+)\s*\)',
            'snowflake': "DATEADD(year, -1, {date_column})",
            'tsql': "DATEADD(year, -1, {date_column})"
        },
        'PARALLELPERIOD': {
            'pattern': r'PARALLELPERIOD\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\)]+)\s*\)',
            'snowflake': "DATEADD({interval}, -{offset}, {date_column})",
            'tsql': "DATEADD({interval}, -{offset}, {date_column})"
        },
        'PREVIOUSYEAR': {
            'pattern': r'PREVIOUSYEAR\s*\(\s*([^\)]+)\s*\)',
            'snowflake': "DATEADD(year, -1, {date_column})",
            'tsql': "DATEADD(year, -1, {date_column})"
        },
        'PREVIOUSMONTH': {
            'pattern': r'PREVIOUSMONTH\s*\(\s*([^\)]+)\s*\)',
            'snowflake': "DATEADD(month, -1, {date_column})",
            'tsql': "DATEADD(month, -1, {date_column})"
        },
        'PREVIOUSQUARTER': {
            'pattern': r'PREVIOUSQUARTER\s*\(\s*([^\)]+)\s*\)',
            'snowflake': "DATEADD(quarter, -1, {date_column})",
            'tsql': "DATEADD(quarter, -1, {date_column})"
        },
        'DATESINPERIOD': {
            'pattern': r'DATESINPERIOD\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^\)]+)\s*\)',
            'snowflake': "{date_column} BETWEEN DATEADD({interval}, -{count}, {end_date}) AND {end_date}",
            'tsql': "{date_column} BETWEEN DATEADD({interval}, -{count}, {end_date}) AND {end_date}"
        }
    }
    
    # Iterator function patterns (SUMX, AVERAGEX, etc.)
    ITERATOR_PATTERNS = {
        'SUMX': {
            'pattern': r'SUMX\s*\(\s*([^,]+)\s*,\s*(.+)\s*\)',
            'template': 'SUM({expression})',
            'requires_join': True
        },
        'AVERAGEX': {
            'pattern': r'AVERAGEX\s*\(\s*([^,]+)\s*,\s*(.+)\s*\)',
            'template': 'AVG({expression})',
            'requires_join': True
        },
        'COUNTX': {
            'pattern': r'COUNTX\s*\(\s*([^,]+)\s*,\s*(.+)\s*\)',
            'template': 'COUNT({expression})',
            'requires_join': True
        },
        'MAXX': {
            'pattern': r'MAXX\s*\(\s*([^,]+)\s*,\s*(.+)\s*\)',
            'template': 'MAX({expression})',
            'requires_join': True
        },
        'MINX': {
            'pattern': r'MINX\s*\(\s*([^,]+)\s*,\s*(.+)\s*\)',
            'template': 'MIN({expression})',
            'requires_join': True
        },
        'RANKX': {
            'pattern': r'RANKX\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*(?:,\s*([^,]+))?\s*(?:,\s*([^,]+))?\s*(?:,\s*([^\)]+))?\s*\)',
            'template': 'RANK() OVER (ORDER BY {expression} {order})',
            'requires_join': True
        },
        'TOPN': {
            'pattern': r'TOPN\s*\(\s*(\d+)\s*,\s*([^,]+)\s*,\s*([^\)]+)\s*\)',
            'template': 'SELECT TOP {n} * FROM {table} ORDER BY {order_by} DESC',
            'requires_subquery': True
        }
    }
    
    # CALCULATE/FILTER patterns
    CALCULATE_PATTERNS = {
        'CALCULATE': {
            'pattern': r'CALCULATE\s*\(\s*([^,]+)\s*(?:,\s*(.+))?\s*\)',
            'template': '''
WITH filtered_data AS (
    SELECT * FROM {base_table}
    WHERE {filter_conditions}
)
SELECT {measure_expression}
FROM filtered_data'''
        },
        'FILTER': {
            'pattern': r'FILTER\s*\(\s*([^,]+)\s*,\s*(.+)\s*\)',
            'template': 'SELECT * FROM {table} WHERE {condition}'
        },
        'ALL': {
            'pattern': r'ALL\s*\(\s*([^\)]+)\s*\)',
            'template': '/* ALL({columns}) - removes filter context, use original table */'
        },
        'ALLEXCEPT': {
            'pattern': r'ALLEXCEPT\s*\(\s*([^,]+)\s*,\s*(.+)\s*\)',
            'template': 'GROUP BY {keep_columns}'
        },
        'VALUES': {
            'pattern': r'VALUES\s*\(\s*([^\)]+)\s*\)',
            'template': 'SELECT DISTINCT {column} FROM {table}'
        },
        'DISTINCT': {
            'pattern': r'DISTINCT\s*\(\s*([^\)]+)\s*\)',
            'template': 'SELECT DISTINCT {column} FROM {table}'
        },
        'KEEPFILTERS': {
            'pattern': r'KEEPFILTERS\s*\(\s*(.+)\s*\)',
            'template': 'AND {filter_condition}'
        }
    }
    
    # Relationship functions
    RELATIONSHIP_PATTERNS = {
        'RELATED': {
            'pattern': r'RELATED\s*\(\s*([^\)]+)\s*\)',
            'template': '{lookup_table}.{column}',
            'join_type': 'LEFT JOIN'
        },
        'RELATEDTABLE': {
            'pattern': r'RELATEDTABLE\s*\(\s*([^\)]+)\s*\)',
            'template': 'FROM {related_table}',
            'join_type': 'INNER JOIN'
        },
        'USERELATIONSHIP': {
            'pattern': r'USERELATIONSHIP\s*\(\s*([^,]+)\s*,\s*([^\)]+)\s*\)',
            'template': 'JOIN {table1} ON {column1} = {column2}',
            'join_type': 'CUSTOM'
        },
        'CROSSJOIN': {
            'pattern': r'CROSSJOIN\s*\(\s*(.+)\s*\)',
            'template': 'CROSS JOIN',
            'join_type': 'CROSS JOIN'
        },
        'NATURALINNERJOIN': {
            'pattern': r'NATURALINNERJOIN\s*\(\s*([^,]+)\s*,\s*([^\)]+)\s*\)',
            'template': 'NATURAL INNER JOIN',
            'join_type': 'NATURAL INNER JOIN'
        }
    }
    
    def __init__(self, 
                 dialect: str = 'snowflake',
                 schema_info: Dict[str, Any] = None,
                 relationship_map: Dict[str, Dict] = None):
        """
        Initialize the translator.
        
        Args:
            dialect: SQL dialect ('snowflake' or 'tsql')
            schema_info: Schema information for table/column resolution
            relationship_map: Map of relationships between tables
        """
        self.dialect = dialect
        self.schema_info = schema_info or {}
        self.relationship_map = relationship_map or {}
        self.function_map = (self.SNOWFLAKE_FUNCTION_MAP if dialect == 'snowflake' 
                            else self.TSQL_FUNCTION_MAP)
        self.translation_log: List[TranslationResult] = []
        
    def translate_measure(self, measure: DAXMeasure) -> TranslationResult:
        """
        Translate a complete DAX measure to SQL.
        
        Args:
            measure: DAXMeasure to translate
            
        Returns:
            TranslationResult with SQL and metadata
        """
        dax = measure.expression.strip()
        
        result = TranslationResult(
            original_dax=dax,
            translated_sql="",
            success=False,
            sql_dialect=self.dialect
        )
        
        try:
            # Step 1: Normalize the DAX expression
            dax = self._normalize_dax(dax)
            
            # Step 2: Extract and translate components
            sql_expr = self._translate_expression(dax, measure.table_name)
            
            # Step 3: Build final SQL
            if sql_expr.requires_cte:
                cte_part = "\n".join(sql_expr.cte_definitions)
                result.translated_sql = f"WITH\n{cte_part}\n{sql_expr.sql}"
            else:
                result.translated_sql = sql_expr.sql
                
            # Add comments about joins if needed
            if sql_expr.requires_joins:
                result.warnings.append(
                    f"Requires JOINs: {', '.join(sql_expr.requires_joins)}"
                )
                
            result.success = True
            result.confidence = self._calculate_confidence(dax, result.translated_sql)
            
        except Exception as e:
            result.translated_sql = f"-- Translation failed: {str(e)}\n-- Original DAX: {dax}"
            result.warnings.append(str(e))
            result.manual_review_needed = True
            logger.error(f"Failed to translate DAX: {dax}, error: {e}")
            
        self.translation_log.append(result)
        return result
        
    def _normalize_dax(self, dax: str) -> str:
        """Normalize DAX expression for parsing."""
        # Remove comments
        dax = re.sub(r'//.*?$', '', dax, flags=re.MULTILINE)
        dax = re.sub(r'/\*.*?\*/', '', dax, flags=re.DOTALL)
        
        # Normalize whitespace
        dax = ' '.join(dax.split())
        
        # Standardize quotes
        dax = dax.replace('"', "'")
        
        return dax.strip()
        
    def _translate_expression(self, dax: str, table_context: str = "") -> SQLExpression:
        """
        Recursively translate a DAX expression to SQL.
        """
        sql_expr = SQLExpression(sql=dax)
        
        # Check for time intelligence functions first (highest priority)
        for func_name, pattern_info in self.TIME_INTELLIGENCE_PATTERNS.items():
            match = re.search(pattern_info['pattern'], dax, re.IGNORECASE)
            if match:
                sql_expr = self._translate_time_intelligence(
                    func_name, match, dax, pattern_info
                )
                return sql_expr
                
        # Check for iterator functions (SUMX, AVERAGEX, etc.)
        for func_name, pattern_info in self.ITERATOR_PATTERNS.items():
            match = re.search(pattern_info['pattern'], dax, re.IGNORECASE)
            if match:
                sql_expr = self._translate_iterator(
                    func_name, match, dax, pattern_info, table_context
                )
                return sql_expr
                
        # Check for CALCULATE/FILTER patterns
        for func_name, pattern_info in self.CALCULATE_PATTERNS.items():
            match = re.search(pattern_info['pattern'], dax, re.IGNORECASE)
            if match:
                sql_expr = self._translate_calculate(
                    func_name, match, dax, pattern_info, table_context
                )
                return sql_expr
                
        # Check for relationship functions
        for func_name, pattern_info in self.RELATIONSHIP_PATTERNS.items():
            match = re.search(pattern_info['pattern'], dax, re.IGNORECASE)
            if match:
                sql_expr = self._translate_relationship(
                    func_name, match, dax, pattern_info
                )
                return sql_expr
                
        # Translate simple functions
        sql_expr.sql = self._translate_simple_functions(dax)
        
        return sql_expr
        
    def _translate_time_intelligence(self, 
                                      func_name: str, 
                                      match: re.Match,
                                      dax: str,
                                      pattern_info: Dict) -> SQLExpression:
        """Translate time intelligence DAX to SQL window functions."""
        sql_expr = SQLExpression(sql="", window_functions=True)
        
        template = pattern_info.get(self.dialect, pattern_info.get('snowflake'))
        
        if func_name == 'TOTALYTD':
            measure = match.group(1).strip()
            date_column = match.group(2).strip()
            
            # Translate the inner measure first
            inner_sql = self._translate_simple_functions(measure)
            
            sql_expr.sql = template.format(
                measure=inner_sql,
                date_column=self._clean_column_ref(date_column)
            )
            
        elif func_name == 'SAMEPERIODLASTYEAR':
            date_column = match.group(1).strip()
            sql_expr.sql = template.format(
                date_column=self._clean_column_ref(date_column)
            )
            sql_expr.comments = "-- Use in a correlated subquery or JOIN to compare with previous year"
            
        elif func_name == 'PARALLELPERIOD':
            date_column = match.group(1).strip()
            offset = match.group(2).strip()
            interval = match.group(3).strip()
            
            sql_expr.sql = template.format(
                date_column=self._clean_column_ref(date_column),
                offset=offset,
                interval=self._dax_interval_to_sql(interval)
            )
            
        elif func_name in ['TOTALQTD', 'TOTALMTD']:
            measure = match.group(1).strip()
            date_column = match.group(2).strip()
            
            inner_sql = self._translate_simple_functions(measure)
            sql_expr.sql = template.format(
                measure=inner_sql,
                date_column=self._clean_column_ref(date_column)
            )
            
        elif func_name in ['PREVIOUSYEAR', 'PREVIOUSMONTH', 'PREVIOUSQUARTER']:
            date_column = match.group(1).strip()
            sql_expr.sql = template.format(
                date_column=self._clean_column_ref(date_column)
            )
            
        return sql_expr
        
    def _translate_iterator(self,
                            func_name: str,
                            match: re.Match,
                            dax: str,
                            pattern_info: Dict,
                            table_context: str) -> SQLExpression:
        """Translate iterator functions like SUMX, AVERAGEX."""
        sql_expr = SQLExpression(sql="")
        
        table = match.group(1).strip()
        expression = match.group(2).strip()
        
        # Clean up table reference
        table = self._clean_table_ref(table)
        
        # Translate the inner expression
        inner_sql = self._translate_simple_functions(expression)
        
        # Build the SQL
        if func_name == 'RANKX':
            order = 'DESC' if len(match.groups()) < 5 or match.group(5) is None else match.group(5)
            sql_expr.sql = f"RANK() OVER (ORDER BY {inner_sql} {order})"
            sql_expr.window_functions = True
        elif func_name == 'TOPN':
            n = match.group(1)
            order_by = self._translate_simple_functions(match.group(3).strip())
            sql_expr.sql = f"SELECT * FROM {table} ORDER BY {order_by} DESC LIMIT {n}"
        else:
            # SUMX, AVERAGEX, COUNTX, etc.
            template = pattern_info['template']
            sql_expr.sql = template.format(expression=inner_sql)
            
        sql_expr.requires_joins.append(table)
        sql_expr.comments = f"-- Iterates over {table}"
        
        return sql_expr
        
    def _translate_calculate(self,
                             func_name: str,
                             match: re.Match,
                             dax: str,
                             pattern_info: Dict,
                             table_context: str) -> SQLExpression:
        """Translate CALCULATE/FILTER expressions to CTEs and WHERE clauses."""
        sql_expr = SQLExpression(sql="")
        
        if func_name == 'CALCULATE':
            measure = match.group(1).strip()
            filters = match.group(2) if len(match.groups()) > 1 else None
            
            # Translate the measure
            measure_sql = self._translate_simple_functions(measure)
            
            if filters:
                # Parse filter conditions
                filter_conditions = self._parse_calculate_filters(filters)
                
                # Build CTE
                cte_name = f"calc_{hash(dax) % 10000}"
                cte_sql = f"""
{cte_name} AS (
    SELECT *
    FROM {table_context or 'base_table'}
    WHERE {filter_conditions}
)"""
                sql_expr.cte_definitions.append(cte_sql)
                sql_expr.requires_cte = True
                sql_expr.sql = f"SELECT {measure_sql} FROM {cte_name}"
            else:
                sql_expr.sql = measure_sql
                
        elif func_name == 'FILTER':
            table = match.group(1).strip()
            condition = match.group(2).strip()
            
            table = self._clean_table_ref(table)
            condition_sql = self._translate_condition(condition)
            
            sql_expr.sql = f"SELECT * FROM {table} WHERE {condition_sql}"
            
        elif func_name == 'ALL':
            columns = match.group(1).strip()
            sql_expr.sql = f"/* ALL({columns}) - context removed */"
            sql_expr.comments = "Use GROUP BY without these columns or subquery"
            
        elif func_name == 'VALUES':
            column = match.group(1).strip()
            parts = column.split('[')
            if len(parts) == 2:
                table = parts[0].strip()
                col = parts[1].rstrip(']')
                sql_expr.sql = f"SELECT DISTINCT {col} FROM {table}"
            else:
                sql_expr.sql = f"SELECT DISTINCT {self._clean_column_ref(column)}"
                
        return sql_expr
        
    def _translate_relationship(self,
                                func_name: str,
                                match: re.Match,
                                dax: str,
                                pattern_info: Dict) -> SQLExpression:
        """Translate RELATED/RELATEDTABLE to SQL JOINs."""
        sql_expr = SQLExpression(sql="")
        
        if func_name == 'RELATED':
            column_ref = match.group(1).strip()
            
            # Parse Table[Column] format
            parts = column_ref.split('[')
            if len(parts) == 2:
                lookup_table = parts[0].strip()
                column = parts[1].rstrip(']')
                
                sql_expr.sql = f"{lookup_table}.{column}"
                sql_expr.requires_joins.append(lookup_table)
                sql_expr.comments = f"-- Requires LEFT JOIN to {lookup_table}"
            else:
                sql_expr.sql = column_ref
                
        elif func_name == 'RELATEDTABLE':
            table_ref = match.group(1).strip()
            sql_expr.sql = f"FROM {table_ref}"
            sql_expr.requires_joins.append(table_ref)
            sql_expr.comments = f"-- Requires INNER JOIN to {table_ref}"
            
        elif func_name == 'USERELATIONSHIP':
            col1 = match.group(1).strip()
            col2 = match.group(2).strip()
            
            clean_col1 = self._clean_column_ref(col1)
            clean_col2 = self._clean_column_ref(col2)
            
            sql_expr.sql = f"JOIN ON {clean_col1} = {clean_col2}"
            sql_expr.comments = "-- Activates inactive relationship"
            
        return sql_expr
        
    def _translate_simple_functions(self, dax: str) -> str:
        """Translate simple DAX functions to SQL equivalents."""
        sql = dax
        
        # Handle column references: Table[Column] -> Table.Column
        sql = re.sub(r"(\w+)\[(\w+)\]", r"\1.\2", sql)
        
        # Translate each function
        for dax_func, sql_func in self.function_map.items():
            # Match function calls
            pattern = rf'\b{dax_func}\s*\('
            if re.search(pattern, sql, re.IGNORECASE):
                if '{0}' in sql_func:
                    # Function with positional arguments
                    sql = self._replace_function_with_template(sql, dax_func, sql_func)
                else:
                    # Direct replacement
                    sql = re.sub(
                        rf'\b{dax_func}\s*\(', 
                        f'{sql_func}(',
                        sql, 
                        flags=re.IGNORECASE
                    )
                    
        # Handle special cases
        sql = self._handle_special_cases(sql)
        
        return sql
        
    def _replace_function_with_template(self, sql: str, dax_func: str, template: str) -> str:
        """Replace a DAX function using a template with arguments."""
        pattern = rf'\b{dax_func}\s*\(([^)]+)\)'
        
        def replacer(match):
            args = [a.strip() for a in match.group(1).split(',')]
            result = template
            for i, arg in enumerate(args):
                result = result.replace(f'{{{i}}}', arg)
            return result
            
        return re.sub(pattern, replacer, sql, flags=re.IGNORECASE)
        
    def _handle_special_cases(self, sql: str) -> str:
        """Handle special DAX patterns that need custom translation."""
        
        # VAR assignments -> CTEs (simplified for inline)
        # VAR x = expr RETURN result 
        var_pattern = r'\bVAR\s+(\w+)\s*=\s*(.+?)\s+(VAR|RETURN)'
        matches = list(re.finditer(var_pattern, sql, re.IGNORECASE))
        for match in reversed(matches):
            var_name = match.group(1)
            var_expr = match.group(2)
            # Replace VAR with comment and inline
            sql = sql[:match.start()] + f"/* {var_name} = */ {var_expr} " + sql[match.end()-len(match.group(3)):]
            
        # RETURN statement
        sql = re.sub(r'\bRETURN\s+', '', sql, flags=re.IGNORECASE)
        
        # Handle TRUE/FALSE
        sql = re.sub(r'\bTRUE\(\)', 'TRUE', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bFALSE\(\)', 'FALSE', sql, flags=re.IGNORECASE)
        
        # Handle BLANK() -> NULL
        sql = re.sub(r'\bBLANK\(\)', 'NULL', sql, flags=re.IGNORECASE)
        
        # Handle OR/AND operators
        sql = re.sub(r'\|\|', 'OR', sql)
        sql = re.sub(r'&&', 'AND', sql)
        
        # Handle not equal operators
        sql = sql.replace('<>', '!=')
        
        return sql
        
    def _parse_calculate_filters(self, filters: str) -> str:
        """Parse CALCULATE filter arguments into SQL WHERE conditions."""
        conditions = []
        
        # Split by top-level commas (not inside parentheses)
        depth = 0
        current = ""
        for char in filters:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            elif char == ',' and depth == 0:
                if current.strip():
                    conditions.append(current.strip())
                current = ""
                continue
            current += char
            
        if current.strip():
            conditions.append(current.strip())
            
        # Translate each filter condition
        sql_conditions = []
        for cond in conditions:
            # Handle Table[Column] = Value patterns
            cond = re.sub(r"(\w+)\[(\w+)\]", r"\1.\2", cond)
            
            # Handle FILTER() inside CALCULATE
            if cond.upper().startswith('FILTER'):
                # Extract inner condition
                match = re.search(r'FILTER\s*\([^,]+,\s*(.+)\)', cond, re.IGNORECASE)
                if match:
                    cond = match.group(1)
                    
            # Translate remaining DAX functions
            cond = self._translate_simple_functions(cond)
            sql_conditions.append(cond)
            
        return ' AND '.join(sql_conditions) if sql_conditions else '1=1'
        
    def _translate_condition(self, condition: str) -> str:
        """Translate a DAX condition to SQL."""
        # Handle column references
        condition = re.sub(r"(\w+)\[(\w+)\]", r"\1.\2", condition)
        
        # Translate functions in condition
        condition = self._translate_simple_functions(condition)
        
        return condition
        
    def _clean_column_ref(self, ref: str) -> str:
        """Clean a DAX column reference to SQL format."""
        # Table[Column] -> Table.Column
        ref = re.sub(r"(\w+)\[(\w+)\]", r"\1.\2", ref)
        return ref.strip().strip("'\"")
        
    def _clean_table_ref(self, ref: str) -> str:
        """Clean a DAX table reference."""
        return ref.strip().strip("'\"")
        
    def _dax_interval_to_sql(self, interval: str) -> str:
        """Convert DAX interval to SQL DATEPART."""
        interval_map = {
            'YEAR': 'year',
            'QUARTER': 'quarter', 
            'MONTH': 'month',
            'DAY': 'day',
            'WEEK': 'week',
            'HOUR': 'hour',
            'MINUTE': 'minute',
            'SECOND': 'second'
        }
        return interval_map.get(interval.upper().strip(), 'day')
        
    def _calculate_confidence(self, original: str, translated: str) -> float:
        """Calculate confidence score for translation."""
        confidence = 1.0
        
        # Lower confidence if there are untranslated DAX functions
        dax_functions = ['CALCULATE', 'SUMX', 'FILTER', 'RELATED', 'TOTALYTD']
        for func in dax_functions:
            if func in translated.upper() and func not in ['SUM', 'AVG', 'COUNT']:
                confidence -= 0.15
                
        # Lower confidence for complex expressions
        if 'WITH' in translated.upper():
            confidence -= 0.1
            
        if 'OVER (' in translated.upper():
            confidence -= 0.05
            
        # Lower confidence for comments indicating issues
        if '--' in translated:
            confidence -= 0.2
            
        return max(0.1, min(1.0, confidence))
        
    # ==========================================
    # BATCH TRANSLATION
    # ==========================================
    
    def translate_all_measures(self, measures: List[DAXMeasure]) -> Dict[str, TranslationResult]:
        """
        Translate all measures from a semantic model.
        
        Returns dict mapping measure name to translation result.
        """
        results = {}
        
        for measure in measures:
            logger.info(f"Translating measure: {measure.name}")
            result = self.translate_measure(measure)
            results[measure.name] = result
            
            if not result.success:
                logger.warning(f"Failed to translate {measure.name}")
            elif result.manual_review_needed:
                logger.warning(f"Manual review needed for {measure.name}")
                
        return results
        
    def extract_measures_from_semantic_model(self, model_json: Dict) -> List[DAXMeasure]:
        """
        Extract DAX measures from a Fabric semantic model definition.
        """
        measures = []
        
        for table in model_json.get('tables', []):
            table_name = table.get('name', '')
            
            for measure in table.get('measures', []):
                dax_measure = DAXMeasure(
                    name=measure.get('name', ''),
                    expression=measure.get('expression', ''),
                    table_name=table_name,
                    format_string=measure.get('formatString', ''),
                    description=measure.get('description', '')
                )
                measures.append(dax_measure)
                
        return measures
        
    def generate_sql_view_definitions(self, 
                                      measures: Dict[str, TranslationResult],
                                      base_table: str) -> str:
        """
        Generate SQL view definitions for translated measures.
        """
        view_cols = []
        ctes = []
        
        for name, result in measures.items():
            if result.success:
                # Add column to view
                col_name = name.replace(' ', '_').upper()
                view_cols.append(f"    {result.translated_sql} AS {col_name}")
                
                # Collect CTEs
                if result.translated_sql.startswith('WITH'):
                    # Extract CTE portion
                    cte_match = re.search(r'WITH\s+(.+?)\s+SELECT', result.translated_sql, re.DOTALL)
                    if cte_match:
                        ctes.append(cte_match.group(1))
                        
        columns_sql = ",\n".join(view_cols)
        
        if ctes:
            cte_sql = "WITH\n" + ",\n".join(ctes) + "\n"
        else:
            cte_sql = ""
            
        view_sql = f"""
-- Generated SQL from DAX measures
-- Dialect: {self.dialect}
-- Generated at: {__import__('datetime').datetime.now().isoformat()}

{cte_sql}CREATE OR REPLACE VIEW MEASURES_VIEW AS
SELECT
    *,
{columns_sql}
FROM {base_table};
"""
        
        return view_sql
        
    def get_translation_report(self) -> Dict[str, Any]:
        """Get summary report of translations."""
        successful = [r for r in self.translation_log if r.success]
        failed = [r for r in self.translation_log if not r.success]
        need_review = [r for r in self.translation_log if r.manual_review_needed]
        
        return {
            'total_translations': len(self.translation_log),
            'successful': len(successful),
            'failed': len(failed),
            'needs_review': len(need_review),
            'average_confidence': sum(r.confidence for r in successful) / len(successful) if successful else 0,
            'details': [{
                'original': r.original_dax[:100],
                'translated': r.translated_sql[:200],
                'success': r.success,
                'confidence': r.confidence,
                'warnings': r.warnings
            } for r in self.translation_log]
        }


def translate_pbix_measures(pbix_path: str, output_path: str, dialect: str = 'snowflake'):
    """
    Convenience function to translate all measures from a PBIX file.
    
    Note: Requires pbixray or similar to extract the model definition.
    """
    translator = DAXToSQLTranslator(dialect=dialect)
    
    # Would need to extract model.bim from PBIX
    # For now, this is a placeholder
    logger.info(f"Would translate measures from {pbix_path} to {output_path}")
    
    return translator.get_translation_report()
