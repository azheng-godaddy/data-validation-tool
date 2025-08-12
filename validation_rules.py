"""Core validation rules for data comparison between legacy and production tables."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from enum import Enum


class ValidationStatus(Enum):
    """Status of validation result."""
    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"
    INFO = "INFO"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    rule_name: str
    status: ValidationStatus
    legacy_value: Any
    prod_value: Any
    difference: Optional[Any] = None
    percentage_diff: Optional[float] = None
    message: str = ""
    error_details: Optional[str] = None


class ValidationRule(ABC):
    """Abstract base class for validation rules."""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
    
    @abstractmethod
    def generate_sql(self, legacy_table: str, prod_table: str) -> Dict[str, str]:
        """Generate SQL queries for this validation rule.
        
        Returns:
            Dict with 'legacy_sql' and 'prod_sql' keys
        """
        pass
    
    @abstractmethod
    def validate(self, legacy_result: Any, prod_result: Any) -> ValidationResult:
        """Validate the results from legacy and prod queries."""
        pass


class RowCountValidation(ValidationRule):
    """Validates row count between legacy and production tables."""
    
    def __init__(self, tolerance_percentage: float = 0.0, date_filter: Optional[str] = None):
        super().__init__(
            name="Row Count Validation",
            description="Compares total row count between legacy and production tables"
        )
        self.tolerance_percentage = tolerance_percentage
        self.date_filter = date_filter
    
    def generate_sql(self, legacy_table: str, prod_table: str) -> Dict[str, str]:
        where_clause = f" WHERE {self.date_filter}" if self.date_filter else ""
        
        return {
            "legacy_sql": f"SELECT COUNT(*) as row_count FROM {legacy_table}{where_clause}",
            "prod_sql": f"SELECT COUNT(*) as row_count FROM {prod_table}{where_clause}"
        }
    
    def validate(self, legacy_result: Any, prod_result: Any) -> ValidationResult:
        try:
            legacy_count = legacy_result[0]['row_count'] if legacy_result else 0
            prod_count = prod_result[0]['row_count'] if prod_result else 0
            
            difference = abs(legacy_count - prod_count)
            percentage_diff = (difference / max(legacy_count, 1)) * 100
            
            # Always return INFO status for informational reporting
            status = ValidationStatus.INFO
            if difference == 0:
                message = f"Row counts are identical: {legacy_count:,}"
            else:
                message = f"Row count difference: {difference:,} rows ({percentage_diff:.2f}%)"
            
            return ValidationResult(
                rule_name=self.name,
                status=status,
                legacy_value=legacy_count,
                prod_value=prod_count,
                difference=difference,
                percentage_diff=percentage_diff,
                message=message
            )
        except Exception as e:
            return ValidationResult(
                rule_name=self.name,
                status=ValidationStatus.ERROR,
                legacy_value=legacy_result,
                prod_value=prod_result,
                message="Error during validation",
                error_details=str(e)
            )


class PrimaryKeyCountValidation(ValidationRule):
    """Validates primary key count and uniqueness."""
    
    def __init__(self, primary_key_columns: List[str], date_filter: Optional[str] = None):
        super().__init__(
            name="Primary Key Count Validation",
            description="Compares primary key count and uniqueness between tables"
        )
        self.primary_key_columns = primary_key_columns
        self.date_filter = date_filter
    
    def generate_sql(self, legacy_table: str, prod_table: str) -> Dict[str, str]:
        # Build WHERE clause with null checks and optional date filter
        null_conditions = [f"{col} IS NOT NULL" for col in self.primary_key_columns]
        where_conditions = null_conditions
        
        if self.date_filter:
            where_conditions.append(self.date_filter)
        
        where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Use bulletproof approach with separate queries joined by UNION ALL
        if len(self.primary_key_columns) == 1:
            # Single primary key
            pk_column = self.primary_key_columns[0]
            legacy_sql = f"""SELECT 'total_rows' as metric, COUNT(*) as value
FROM {legacy_table}{where_clause}
UNION ALL
SELECT 'distinct_pk_count' as metric, COUNT(DISTINCT {pk_column}) as value
FROM {legacy_table}{where_clause}"""
            
            prod_sql = f"""SELECT 'total_rows' as metric, COUNT(*) as value
FROM {prod_table}{where_clause}
UNION ALL
SELECT 'distinct_pk_count' as metric, COUNT(DISTINCT {pk_column}) as value
FROM {prod_table}{where_clause}"""
        else:
            # Composite primary key - use CONCAT approach
            concat_expr = "CONCAT(" + ", '|', ".join([f"CAST({col} AS VARCHAR)" for col in self.primary_key_columns]) + ")"
            legacy_sql = f"""SELECT 'total_rows' as metric, COUNT(*) as value
FROM {legacy_table}{where_clause}
UNION ALL
SELECT 'distinct_pk_count' as metric, COUNT(DISTINCT {concat_expr}) as value
FROM {legacy_table}{where_clause}"""
            
            prod_sql = f"""SELECT 'total_rows' as metric, COUNT(*) as value
FROM {prod_table}{where_clause}
UNION ALL
SELECT 'distinct_pk_count' as metric, COUNT(DISTINCT {concat_expr}) as value
FROM {prod_table}{where_clause}"""
        
        return {
            "legacy_sql": legacy_sql,
            "prod_sql": prod_sql
        }
    
    def validate(self, legacy_result: Any, prod_result: Any) -> ValidationResult:
        try:
            # Parse results from UNION ALL format
            legacy_metrics = {row['metric']: row['value'] for row in legacy_result} if legacy_result else {}
            prod_metrics = {row['metric']: row['value'] for row in prod_result} if prod_result else {}
            
            legacy_total = legacy_metrics.get('total_rows', 0)
            legacy_unique = legacy_metrics.get('distinct_pk_count', 0)
            prod_total = prod_metrics.get('total_rows', 0)
            prod_unique = prod_metrics.get('distinct_pk_count', 0)
            
            # Check if PKs are unique in each table
            legacy_unique_pct = (legacy_unique / max(legacy_total, 1)) * 100
            prod_unique_pct = (prod_unique / max(prod_total, 1)) * 100
            
            issues = []
            if legacy_unique_pct < 100:
                issues.append(f"Legacy table has duplicate PKs ({legacy_unique_pct:.1f}% unique)")
            if prod_unique_pct < 100:
                issues.append(f"Prod table has duplicate PKs ({prod_unique_pct:.1f}% unique)")
            if legacy_unique != prod_unique:
                issues.append(f"Unique PK count mismatch: {legacy_unique} vs {prod_unique}")
            
            # Always return INFO status for informational reporting
            status = ValidationStatus.INFO
            if not issues:
                message = f"Primary key counts: Legacy={legacy_unique:,}, Prod={prod_unique:,} (both 100% unique)"
            else:
                message = "; ".join(issues)
            
            return ValidationResult(
                rule_name=self.name,
                status=status,
                legacy_value={"total": legacy_total, "unique": legacy_unique},
                prod_value={"total": prod_total, "unique": prod_unique},
                difference=abs(legacy_unique - prod_unique),
                message=message
            )
        except Exception as e:
            return ValidationResult(
                rule_name=self.name,
                status=ValidationStatus.ERROR,
                legacy_value=legacy_result,
                prod_value=prod_result,
                message="Error during validation",
                error_details=str(e)
            )


class NullValueValidation(ValidationRule):
    """Validates null value counts across specified columns."""
    
    def __init__(self, columns: List[str], date_filter: Optional[str] = None):
        super().__init__(
            name="Null Value Validation",
            description="Compares null value counts for specified columns"
        )
        self.columns = columns
        self.date_filter = date_filter
    
    def generate_sql(self, legacy_table: str, prod_table: str) -> Dict[str, str]:
        null_checks = [f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as {col}_nulls" 
                      for col in self.columns]
        null_sql = ", ".join(null_checks)
        
        where_clause = f" WHERE {self.date_filter}" if self.date_filter else ""
        
        legacy_sql = f"SELECT COUNT(*) as total_rows, {null_sql} FROM {legacy_table}{where_clause}"
        prod_sql = f"SELECT COUNT(*) as total_rows, {null_sql} FROM {prod_table}{where_clause}"
        
        return {
            "legacy_sql": legacy_sql,
            "prod_sql": prod_sql
        }
    
    def validate(self, legacy_result: Any, prod_result: Any) -> ValidationResult:
        try:
            legacy_data = legacy_result[0] if legacy_result else {}
            prod_data = prod_result[0] if prod_result else {}
            
            issues = []
            null_comparison = {}
            
            for col in self.columns:
                legacy_nulls = legacy_data.get(f"{col}_nulls", 0)
                prod_nulls = prod_data.get(f"{col}_nulls", 0)
                
                legacy_total = legacy_data.get('total_rows', 1)
                prod_total = prod_data.get('total_rows', 1)
                
                legacy_null_pct = (legacy_nulls / legacy_total) * 100
                prod_null_pct = (prod_nulls / prod_total) * 100
                
                null_comparison[col] = {
                    "legacy_nulls": legacy_nulls,
                    "prod_nulls": prod_nulls,
                    "legacy_null_pct": legacy_null_pct,
                    "prod_null_pct": prod_null_pct
                }
                
                if abs(legacy_null_pct - prod_null_pct) > 5:  # 5% tolerance
                    issues.append(f"{col}: {legacy_null_pct:.1f}% vs {prod_null_pct:.1f}% null")
            
            status = ValidationStatus.PASS if not issues else ValidationStatus.FAIL
            message = "Null value validation passed" if not issues else "; ".join(issues)
            
            return ValidationResult(
                rule_name=self.name,
                status=status,
                legacy_value=null_comparison,
                prod_value=null_comparison,
                message=message
            )
        except Exception as e:
            return ValidationResult(
                rule_name=self.name,
                status=ValidationStatus.ERROR,
                legacy_value=legacy_result,
                prod_value=prod_result,
                message="Error during validation",
                error_details=str(e)
            )


class DataTypeValidation(ValidationRule):
    """Validates data types and schema compatibility using AWS Glue Catalog.
    
    This handles both regular tables and Iceberg tables by using the Glue Data Catalog
    instead of information_schema queries which fail for Iceberg tables.
    """
    
    def __init__(self):
        super().__init__(
            name="Data Type Validation",
            description="Compares column data types between legacy and production tables using Glue Catalog"
        )
    
    def _get_table_schema_from_glue(self, table_name: str) -> Dict[str, Any]:
        """Get table schema from AWS Glue Catalog.
        
        Args:
            table_name: Full table name in format 'database.table'
            
        Returns:
            Dictionary with table schema information
        """
        import boto3
        import os
        from config import settings
        
        db_name, table = table_name.split('.')
        
        # Create Glue client with region (SSO-aware like athena_client.py)
        using_sso = os.getenv('AWS_SESSION_TOKEN') is not None
        if using_sso:
            glue = boto3.client('glue', region_name='us-west-2')
        else:
            glue = boto3.client(
                'glue',
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name='us-west-2'
            )
        
        try:
            response = glue.get_table(DatabaseName=db_name, Name=table)
            table_info = response['Table']
            
            # Get table type
            table_type = table_info.get('Parameters', {}).get('table_type', 'HIVE').upper()
            
            # Get columns
            storage_descriptor = table_info.get('StorageDescriptor', {})
            columns = storage_descriptor.get('Columns', [])
            
            # Build schema dictionary
            schema = {}
            for col in columns:
                col_name = col['Name']
                col_type = col['Type']
                
                # Normalize data types for comparison
                normalized_type = self._normalize_data_type(col_type)
                
                schema[col_name] = {
                    'column_name': col_name,
                    'data_type': col_type,
                    'normalized_type': normalized_type,
                    'comment': col.get('Comment', '')
                }
            
            return {
                'table_type': table_type,
                'column_count': len(columns),
                'schema': schema,
                'table_info': table_info
            }
            
        except Exception as e:
            raise RuntimeError(f"Failed to get schema for {table_name}: {str(e)}")
    
    def _normalize_data_type(self, data_type: str) -> str:
        """Normalize data types for comparison.
        
        Handles variations between Iceberg and Hive table type representations.
        """
        data_type = data_type.lower().strip()
        
        # Common normalizations
        type_mappings = {
            'varchar': 'string',
            'char': 'string', 
            'text': 'string',
            'integer': 'int',
            'bigint': 'bigint',
            'double precision': 'double',
            'real': 'float',
            'bool': 'boolean'
        }
        
        # Extract base type (remove precision/scale)
        base_type = data_type.split('(')[0].strip()
        
        return type_mappings.get(base_type, base_type)
    
    def generate_sql(self, legacy_table: str, prod_table: str) -> Dict[str, str]:
        """This validation doesn't use SQL queries - it uses Glue Catalog directly.
        
        Returns empty SQL as this method uses Glue API instead.
        """
        return {
            "legacy_sql": "-- Schema retrieved via Glue Catalog",
            "prod_sql": "-- Schema retrieved via Glue Catalog"
        }
    
    def validate(self, legacy_result: Any, prod_result: Any) -> ValidationResult:
        """Validate schemas using Glue Catalog data.
        
        Note: The legacy_result and prod_result parameters contain table names,
        not SQL query results, since this validation uses Glue API directly.
        """
        try:
            # For this validation, we need to extract table names
            # This is a special case where we bypass the normal SQL execution
            return self._validate_with_glue_catalog()
            
        except Exception as e:
            return ValidationResult(
                rule_name=self.name,
                status=ValidationStatus.ERROR,
                legacy_value="N/A",
                prod_value="N/A",
                message=f"Glue Catalog schema validation failed: {str(e)}",
                error_details=str(e)
            )
    
    def validate_tables_direct(self, legacy_table: str, prod_table: str) -> ValidationResult:
        """Direct validation method that bypasses SQL execution."""
        try:
            # Get schemas from Glue Catalog
            legacy_info = self._get_table_schema_from_glue(legacy_table)
            prod_info = self._get_table_schema_from_glue(prod_table)
            
            legacy_schema = legacy_info['schema']
            prod_schema = prod_info['schema']
            
            issues = []
            warnings = []
            
            # Report table types
            legacy_type = legacy_info['table_type']
            prod_type = prod_info['table_type']
            
            if legacy_type != prod_type:
                warnings.append(f"Table types differ: Legacy={legacy_type}, Prod={prod_type}")
            
            # Check for missing columns
            legacy_cols = set(legacy_schema.keys())
            prod_cols = set(prod_schema.keys())
            
            missing_in_prod = legacy_cols - prod_cols
            missing_in_legacy = prod_cols - legacy_cols
            
            if missing_in_prod:
                issues.append(f"Columns missing in prod ({len(missing_in_prod)}): {', '.join(list(missing_in_prod)[:5])}{'...' if len(missing_in_prod) > 5 else ''}")
            if missing_in_legacy:
                issues.append(f"Columns missing in legacy ({len(missing_in_legacy)}): {', '.join(list(missing_in_legacy)[:5])}{'...' if len(missing_in_legacy) > 5 else ''}")
            
            # Check data type mismatches for common columns
            common_cols = legacy_cols & prod_cols
            type_mismatches = []
            
            for col in sorted(common_cols):
                legacy_type = legacy_schema[col]['normalized_type']
                prod_type = prod_schema[col]['normalized_type']
                
                if legacy_type != prod_type:
                    original_legacy = legacy_schema[col]['data_type']
                    original_prod = prod_schema[col]['data_type']
                    type_mismatches.append(f"{col}: {original_legacy} → {original_prod}")
            
            if type_mismatches:
                issues.append(f"Type mismatches ({len(type_mismatches)}): {'; '.join(type_mismatches[:3])}{'...' if len(type_mismatches) > 3 else ''}")
            
            # Build summary message
            summary_parts = []
            summary_parts.append(f"Legacy: {legacy_type} ({len(legacy_cols)} cols)")
            summary_parts.append(f"Prod: {prod_type} ({len(prod_cols)} cols)")
            summary_parts.append(f"Common: {len(common_cols)} cols")
            
            if warnings:
                summary_parts.extend(warnings)
            
            # Always return INFO status for informational reporting
            status = ValidationStatus.INFO
            if issues:
                message = "; ".join(summary_parts + issues)
            else:
                message = "Schema comparison: " + "; ".join(summary_parts)
            
            return ValidationResult(
                rule_name=self.name,
                status=status,
                legacy_value=f"{legacy_type} ({len(legacy_cols)} cols)",
                prod_value=f"{prod_type} ({len(prod_cols)} cols)",
                difference=abs(len(legacy_cols) - len(prod_cols)),
                message=message
            )
            
        except Exception as e:
            return ValidationResult(
                rule_name=self.name,
                status=ValidationStatus.ERROR,
                legacy_value="N/A",
                prod_value="N/A",
                message="Glue Catalog schema validation failed",
                error_details=str(e)
            ) 