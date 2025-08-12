"""Main data validation orchestrator."""

from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass
import time
from datetime import datetime

from config import settings
from athena_client import AthenaClient
from llm_sql_generator import SQLGenerator
from validation_rules import (
    ValidationRule, 
    RowCountValidation, 
    PrimaryKeyCountValidation, 
    NullValueValidation, 
    DataTypeValidation,
    ValidationResult,
    ValidationStatus
)


@dataclass
class ValidationReport:
    """Complete validation report."""
    legacy_table: str
    prod_table: str
    validation_results: List[ValidationResult]
    execution_time: float
    timestamp: datetime
    summary: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    error_checks: int


class DataValidator:
    """Main data validation orchestrator."""
    
    def __init__(self):
        """Initialize the data validator."""
        print("🔧 Using Direct Athena API Execution")
        print("📋 Running queries directly through Athena")
        
        # Initialize Athena client
        self.athena_client = AthenaClient()
        
        # Initialize SQL generator for AI service (GoCaaS or GoCode)
        self.sql_generator = SQLGenerator()
            
        self.predefined_rules = []
    
    def add_validation_rule(self, rule: ValidationRule):
        """Add a predefined validation rule.
        
        Args:
            rule: ValidationRule instance to add
        """
        self.predefined_rules.append(rule)
    
    def validate_tables(
        self,
        legacy_table: str,
        prod_table: str,
        custom_validation_request: Optional[str] = None,
        include_schema_validation: bool = True,
        primary_key_columns: Optional[List[str]] = None,
        null_check_columns: Optional[List[str]] = None,
        row_count_tolerance: float = 0.0,
        date_column: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> ValidationReport:
        """Perform comprehensive validation between legacy and production tables.
        
        Args:
            legacy_table: Name of the legacy table
            prod_table: Name of the production table
            custom_validation_request: Optional custom validation request in natural language
            include_schema_validation: Whether to include schema validation
            primary_key_columns: List of primary key columns for PK validation
            null_check_columns: List of columns to check for null values
            row_count_tolerance: Tolerance percentage for row count differences
            date_column: Column name for date filtering
            start_date: Start date for filtering (YYYY-MM-DD format)
            end_date: End date for filtering (YYYY-MM-DD format)
            
        Returns:
            ValidationReport with complete results
        """
        start_time = time.time()
        validation_results = []
        
        date_info = ""
        if date_column and (start_date or end_date):
            date_info = f" (filtered by {date_column}"
            if start_date and end_date:
                date_info += f" from {start_date} to {end_date})"
            elif start_date:
                date_info += f" from {start_date})"
            elif end_date:
                date_info += f" until {end_date})"
        
        print(f"🔍 Starting validation: {legacy_table} vs {prod_table}{date_info}")
        
        # Test table access first
        if not self._test_table_access(legacy_table, prod_table):
            return self._create_error_report(
                legacy_table, prod_table, "Table access test failed", start_time
            )
        
        # 1. Basic validation rules
        basic_rules = self._get_basic_validation_rules(
            row_count_tolerance, primary_key_columns, null_check_columns, 
            date_column, start_date, end_date
        )
        
        # 2. Schema validation
        if include_schema_validation:
            basic_rules.append(DataTypeValidation())
        
        # 3. Execute predefined rules
        print("📊 Executing predefined validation rules...")
        for rule in basic_rules + self.predefined_rules:
            try:
                result = self._execute_validation_rule(rule, legacy_table, prod_table, 
                                                     date_column, start_date, end_date)
                validation_results.append(result)
                print(f"   ✓ {rule.name}: {result.status.value}")
            except Exception as e:
                error_result = ValidationResult(
                    rule_name=rule.name,
                    status=ValidationStatus.ERROR,
                    legacy_value=None,
                    prod_value=None,
                    message="Execution error",
                    error_details=str(e)
                )
                validation_results.append(error_result)
                print(f"   ❌ {rule.name}: ERROR - {str(e)}")
        
        # 4. Custom LLM-generated validation
        if custom_validation_request:
            print("🤖 Executing custom LLM-generated validation...")
            try:
                custom_result = self._execute_custom_validation(
                    legacy_table, prod_table, custom_validation_request,
                    date_column, start_date, end_date
                )
                validation_results.append(custom_result)
                print(f"   ✓ Custom Validation: {custom_result.status.value}")
            except Exception as e:
                error_result = ValidationResult(
                    rule_name="Custom LLM Validation",
                    status=ValidationStatus.ERROR,
                    legacy_value=None,
                    prod_value=None,
                    message="Custom validation error",
                    error_details=str(e)
                )
                validation_results.append(error_result)
                print(f"   ❌ Custom Validation: ERROR - {str(e)}")
        
        # 5. Generate summary with LLM
        execution_time = time.time() - start_time
        summary = self._generate_summary(validation_results, legacy_table, prod_table)
        
        # 6. Create final report
        report = self._create_validation_report(
            legacy_table, prod_table, validation_results, execution_time, summary
        )
        
        print(f"✅ Validation completed in {execution_time:.2f} seconds")
        return report
    
    def validate_with_custom_sql(
        self,
        legacy_table: str,
        prod_table: str,
        legacy_sql: str,
        prod_sql: str,
        validation_name: str = "Custom SQL Validation"
    ) -> ValidationResult:
        """Execute validation with custom SQL queries.
        
        Args:
            legacy_table: Legacy table name
            prod_table: Production table name
            legacy_sql: Custom SQL for legacy table
            prod_sql: Custom SQL for production table
            validation_name: Name for this validation
            
        Returns:
            ValidationResult
        """
        try:
            print(f"🔍 Executing custom SQL validation: {validation_name}")
            
            # Execute queries in parallel
            queries = {
                "legacy": legacy_sql,
                "prod": prod_sql
            }
            
            results = self.athena_client.execute_parallel_queries(queries)
            
            legacy_result = results["legacy"]
            prod_result = results["prod"]
            
            # Simple comparison - you can customize this logic
            if legacy_result == prod_result:
                status = ValidationStatus.PASS
                message = "Custom SQL results match"
            else:
                status = ValidationStatus.FAIL
                message = "Custom SQL results differ"
            
            return ValidationResult(
                rule_name=validation_name,
                status=status,
                legacy_value=legacy_result,
                prod_value=prod_result,
                message=message
            )
            
        except Exception as e:
            return ValidationResult(
                rule_name=validation_name,
                status=ValidationStatus.ERROR,
                legacy_value=None,
                prod_value=None,
                message="Custom SQL execution error",
                error_details=str(e)
            )
    
    def _build_date_filter(
        self, 
        date_column: Optional[str], 
        start_date: Optional[str], 
        end_date: Optional[str]
    ) -> Optional[str]:
        """Build SQL WHERE clause for date filtering.
        
        Handles both DATE and VARCHAR date columns by using TRY_CAST for safe conversion.
        
        Args:
            date_column: Column name for date filtering
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            SQL WHERE clause or None if no filtering
        """
        if not date_column or not (start_date or end_date):
            return None
        
        conditions = []
        
        # Use TRY_CAST to safely convert VARCHAR dates to DATE type
        # This works for both actual DATE columns and VARCHAR date columns
        date_expr = f"TRY_CAST({date_column} AS DATE)"
        
        if start_date:
            conditions.append(f"{date_expr} >= DATE '{start_date}'")
        
        if end_date:
            conditions.append(f"{date_expr} <= DATE '{end_date}'")
        
        return " AND ".join(conditions)
    
    def _test_table_access(self, legacy_table: str, prod_table: str) -> bool:
        """Test if both tables are accessible."""
        try:
            legacy_accessible = self.athena_client.test_table_access(legacy_table)
            prod_accessible = self.athena_client.test_table_access(prod_table)
            
            if not legacy_accessible:
                print(f"❌ Cannot access legacy table: {legacy_table}")
            if not prod_accessible:
                print(f"❌ Cannot access production table: {prod_table}")
            
            return legacy_accessible and prod_accessible
            
        except Exception as e:
            print(f"❌ Table access test failed: {str(e)}")
            return False
    
    def _get_basic_validation_rules(
        self,
        row_count_tolerance: float,
        primary_key_columns: Optional[List[str]],
        null_check_columns: Optional[List[str]],
        date_column: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[ValidationRule]:
        """Get basic validation rules based on parameters."""
        # Add date filter context to all rules
        date_filter = self._build_date_filter(date_column, start_date, end_date)
        
        rules = [RowCountValidation(tolerance_percentage=row_count_tolerance, date_filter=date_filter)]
        
        if primary_key_columns:
            rules.append(PrimaryKeyCountValidation(primary_key_columns, date_filter=date_filter))
        
        if null_check_columns:
            rules.append(NullValueValidation(null_check_columns, date_filter=date_filter))
        
        return rules
    
    def _execute_validation_rule(
        self, 
        rule: ValidationRule, 
        legacy_table: str, 
        prod_table: str,
        date_column: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> ValidationResult:
        """Execute a single validation rule."""
        # Special handling for DataTypeValidation which uses Glue Catalog instead of SQL
        if isinstance(rule, DataTypeValidation):
            return rule.validate_tables_direct(legacy_table, prod_table)
        
        # Standard SQL-based validation for other rules
        # Generate SQL queries
        sql_queries = rule.generate_sql(legacy_table, prod_table)
        
        # Execute queries in parallel - pass as list, not dictionary
        queries = [sql_queries["legacy_sql"], sql_queries["prod_sql"]]
        
        results = self.athena_client.execute_parallel_queries(queries)
        
        # Validate results - results[0] is legacy, results[1] is prod
        legacy_result = results[0] if len(results) > 0 else []
        prod_result = results[1] if len(results) > 1 else []
        
        return rule.validate(legacy_result, prod_result)
    
    def _execute_custom_validation(
        self, 
        legacy_table: str, 
        prod_table: str, 
        validation_request: str,
        date_column: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> ValidationResult:
        """Execute custom validation using LLM-generated SQL if available."""
        
        # Check if SQL generator is available
        if not self.sql_generator:
            return ValidationResult(
                rule_name="Custom Validation",
                status=ValidationStatus.ERROR,
                legacy_value="N/A",
                prod_value="N/A",
                message="Custom validation requires GoCode API access. Use predefined rules or configure GOCODE_API_TOKEN.",
                error_details="Set GOCODE_API_TOKEN in your .env file. Get token from: https://caas.godaddy.com/gocode/my-api-keys (requires VPN)"
            )
        
        try:
            # Get table schemas for better SQL generation
            legacy_schema = self.athena_client.get_table_schema(legacy_table)
            prod_schema = self.athena_client.get_table_schema(prod_table)
            
            schema_info = {
                legacy_table: legacy_schema,
                prod_table: prod_schema
            }
            
            # Build date context for LLM
            date_context = ""
            if date_column and (start_date or end_date):
                date_context = f" with date filtering on {date_column}"
                if start_date and end_date:
                    date_context += f" between {start_date} and {end_date}"
                elif start_date:
                    date_context += f" from {start_date} onwards"
                elif end_date:
                    date_context += f" up to {end_date}"
            
            max_retries = 2
            sql_result = None
            
            # Generate SQL with LLM (with timeout and retry for errors)
            import time
            generation_start = time.time()
            
            for attempt in range(max_retries):
                try:
                    print(f"🤖 Generating SQL with LLM (attempt {attempt + 1})...")
                    
                    sql_result = self.sql_generator.generate_validation_sql(
                        legacy_table, prod_table, validation_request + date_context, schema_info,
                        date_column, start_date, end_date
                    )
                    print(f"✅ LLM generation successful in {time.time() - generation_start:.1f}s")
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    error_str = str(e)
                    elapsed = time.time() - generation_start
                    
                    # Handle various error types
                    error_indicators = [
                        'JSON', 'property name', 'Expecting', 'timeout', 'stuck', 'slow',
                        'LLM validation failed', 'syntax issues', 'couldn\'t fix issues'
                    ]
                    
                    if attempt < max_retries - 1 and (any(indicator in error_str for indicator in error_indicators) or elapsed > 45):
                        if elapsed > 45:
                            print(f"⚠️  LLM generation timeout after {elapsed:.1f}s - using fallback...")
                        else:
                            print(f"⚠️  LLM error (attempt {attempt + 1}/{max_retries}): {error_str}")
                        
                        print("🔄 Using safe fallback SQL generation...")
                        
                        # Use fallback SQL generation immediately
                        sql_result = self.sql_generator._create_safe_fallback_sql(
                            legacy_table, prod_table, validation_request
                        )
                        break
                    else:
                        raise  # Re-raise if not a generation error or max retries reached
            
            # Execute the generated SQL with retry for syntax errors
            queries = {
                "legacy": sql_result["legacy_sql"],
                "prod": sql_result["prod_sql"]
            }
            
            # Pre-execution validation - check for remaining issues
            for query_type, query_sql in queries.items():
                if query_sql:
                    issues = []
                    
                    # Check for NULLIF issues
                    if 'NULLIF' in query_sql.upper():
                        issues.append("NULLIF syntax")
                    
                    # Check for quote issues
                    quote_count = query_sql.count("'")
                    if quote_count % 2 != 0:
                        issues.append("unmatched quotes")
                    
                    # Check for multiple quote sequences (common LLM errors)
                    import re
                    if re.search(r"'{2,}", query_sql):
                        issues.append("multiple quotes")
                    
                    # Check for lowercase null (Athena requires uppercase NULL)
                    if re.search(r'\bnull\b', query_sql):
                        issues.append("lowercase null (should be NULL)")
                    
                    # Check for parentheses balancing (common cause of EOF errors)
                    open_parens = query_sql.count('(')
                    close_parens = query_sql.count(')')
                    if open_parens != close_parens:
                        issues.append(f"unbalanced parentheses ({open_parens} open, {close_parens} close)")
                    
                    # Check for trailing commas (cause EOF errors)
                    if re.search(r',\s*(?=FROM|WHERE|GROUP|ORDER|UNION|LIMIT|;|$)', query_sql):
                        issues.append("trailing commas before clauses")
                    
                    # Check for incomplete SQL (common with token limits)
                    sql_trimmed = query_sql.strip()
                    if sql_trimmed:
                        last_word = sql_trimmed.split()[-1] if sql_trimmed.split() else ""
                        if last_word.upper() in ['WHERE', 'AND', 'OR', 'SELECT', 'FROM', 'JOIN', 'ON', 'GROUP', 'ORDER', 'HAVING', '=', '>', '<', 'LIKE', 'IN']:
                            issues.append(f"incomplete SQL (ends with '{last_word}')")
                    
                    # Check for position-specific issues around common error locations
                    for pos in [1265, 1292, 1721]:  # Common error positions
                        if len(query_sql) > pos:
                            start_pos = max(0, pos - 25)
                            end_pos = min(len(query_sql), pos + 25)
                            check_area = query_sql[start_pos:end_pos]
                            
                            if (("'" in check_area and check_area.count("'") % 2 != 0) or 
                                re.search(r"'{2,}", check_area) or 
                                re.search(r'\bnull\b', check_area)):
                                issues.append(f"syntax issues around position {pos}")
                    
                    if issues:
                        print(f"⚠️  Warning: {query_type} query has issues: {', '.join(issues)}")
                        print(f"📝 Query length: {len(query_sql)} chars")
                        if len(query_sql) > 1700:
                            print(f"📝 Around pos 1721: ...{query_sql[1710:1730]}...")
                        else:
                            print(f"📝 Query: {query_sql[:300]}...")
                        
                        # Force fallback if any issues detected
                        print("🔄 Forcing fallback SQL generation due to syntax issues...")
                        sql_result = self.sql_generator._create_safe_fallback_sql(
                            legacy_table, prod_table, validation_request
                        )
                        queries = {
                            "legacy": sql_result["legacy_sql"],
                            "prod": sql_result["prod_sql"]
                        }
                        break
            
            for attempt in range(max_retries):
                try:
                    results = self.athena_client.execute_parallel_queries(queries)
                    break  # Success, exit retry loop
                except Exception as e:
                    error_str = str(e)
                    syntax_error_indicators = [
                        'NULLIF', 'mismatched input', 'Expecting:', 
                        'InvalidRequestException', "'''", "''''", 'quote', 
                        'line 1:', 'position', 'null', 'identifier',
                        '<EOF>', 'ORDER', 'incomplete', 'parentheses'
                    ]
                    
                    if attempt < max_retries - 1 and any(indicator in error_str for indicator in syntax_error_indicators):
                        print(f"⚠️  SQL syntax error (attempt {attempt + 1}/{max_retries}): {error_str}")
                        
                        # Specific handling for quote errors
                        if "'''" in error_str or 'quote' in error_str.lower():
                            print("🔧 Quote-related error detected - using safe fallback...")
                        
                        print("🔄 Regenerating SQL with fallback...")
                        
                        # Try fallback SQL generation
                        sql_result = self.sql_generator._create_safe_fallback_sql(
                            legacy_table, prod_table, validation_request
                        )
                        queries = {
                            "legacy": sql_result["legacy_sql"],
                            "prod": sql_result["prod_sql"]
                        }
                    else:
                        raise  # Re-raise if not a syntax error or max retries reached
            
            # Basic validation logic
            legacy_result = results["legacy"]
            prod_result = results["prod"]
            
            if legacy_result == prod_result:
                status = ValidationStatus.PASS
                message = f"Custom validation passed: {sql_result['explanation']}"
            else:
                status = ValidationStatus.FAIL
                message = f"Custom validation failed: {sql_result['explanation']}"
            
            return ValidationResult(
                rule_name="Custom LLM Validation",
                status=status,
                legacy_value=legacy_result,
                prod_value=prod_result,
                message=message
            )
            
        except Exception as e:
            return ValidationResult(
                rule_name="Custom Validation",
                status=ValidationStatus.ERROR,
                legacy_value="N/A",
                prod_value="N/A",
                message="Custom validation failed to execute",
                error_details=str(e)
            )
    
    def _generate_summary(self, validation_results: List[ValidationResult], legacy_table: str, prod_table: str) -> str:
        """Generate a summary of validation results using LLM if available."""
        try:
            # Use LLM summary if SQL generator is available
            if self.sql_generator:
                # Convert results to dict format for LLM
                results_dict = []
                for result in validation_results:
                    results_dict.append({
                        "rule_name": result.rule_name,
                        "status": result.status.value,
                        "message": result.message
                    })
                
                return self.sql_generator.explain_validation_results(
                    results_dict, legacy_table, prod_table
                )
            else:
                # Fallback to simple summary
                total = len(validation_results)
                passed = sum(1 for r in validation_results if r.status == ValidationStatus.PASS)
                failed = sum(1 for r in validation_results if r.status == ValidationStatus.FAIL)
                info = sum(1 for r in validation_results if r.status == ValidationStatus.INFO)
                errors = sum(1 for r in validation_results if r.status == ValidationStatus.ERROR)
                
                return f"Data comparison completed: {info} informational reports, {passed} validations passed, {failed} failed, {errors} errors."
        except Exception:
            # Fallback summary
            total = len(validation_results)
            passed = sum(1 for r in validation_results if r.status == ValidationStatus.PASS)
            failed = sum(1 for r in validation_results if r.status == ValidationStatus.FAIL)
            info = sum(1 for r in validation_results if r.status == ValidationStatus.INFO)
            errors = sum(1 for r in validation_results if r.status == ValidationStatus.ERROR)
            
            return f"Data comparison completed: {info} informational reports, {passed} validations passed, {failed} failed, {errors} errors."
    
    def _create_validation_report(
        self,
        legacy_table: str,
        prod_table: str,
        validation_results: List[ValidationResult],
        execution_time: float,
        summary: str
    ) -> ValidationReport:
        """Create final validation report."""
        total_checks = len(validation_results)
        passed_checks = sum(1 for r in validation_results if r.status == ValidationStatus.PASS)
        failed_checks = sum(1 for r in validation_results if r.status == ValidationStatus.FAIL)
        error_checks = sum(1 for r in validation_results if r.status == ValidationStatus.ERROR)
        
        return ValidationReport(
            legacy_table=legacy_table,
            prod_table=prod_table,
            validation_results=validation_results,
            execution_time=execution_time,
            timestamp=datetime.now(),
            summary=summary,
            total_checks=total_checks,
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            error_checks=error_checks
        )
    
    def _create_error_report(
        self, 
        legacy_table: str, 
        prod_table: str, 
        error_message: str, 
        start_time: float
    ) -> ValidationReport:
        """Create error report when validation cannot proceed."""
        error_result = ValidationResult(
            rule_name="Table Access",
            status=ValidationStatus.ERROR,
            legacy_value=None,
            prod_value=None,
            message=error_message
        )
        
        return ValidationReport(
            legacy_table=legacy_table,
            prod_table=prod_table,
            validation_results=[error_result],
            execution_time=time.time() - start_time,
            timestamp=datetime.now(),
            summary=f"Validation failed: {error_message}",
            total_checks=1,
            passed_checks=0,
            failed_checks=0,
            error_checks=1
        ) 