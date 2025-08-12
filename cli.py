"""Command-line interface for the data validation tool."""

import click
from typing import Optional, List, Dict, Any, Union
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress
from tabulate import tabulate
import json
import re

from config import validate_config
from data_validator import DataValidator, ValidationReport
from validation_rules import ValidationStatus, ValidationResult, DataTypeValidation
from datetime import datetime


console = Console()


@click.group()
def cli():
    """Data Validation Tool - Compare legacy and production tables using AWS Athena."""
    # Skip validation for setup and diagnostic commands
    import sys
    skip_validation_commands = ['setup-env', 'test-gocode']
    if not any(cmd in sys.argv for cmd in skip_validation_commands):
        if not validate_config():
            console.print("[red]❌ Configuration validation failed. Please check your .env file.[/red]")
            raise click.Abort()


@cli.command('setup-env')
@click.option('--gocode-token', '-t', default='', help='GoCode API token (optional)')
@click.option('--github-token', '-g', default='', help='GitHub token for enhanced schema lookup (optional)')
def setup_env(gocode_token: str, github_token: str):
    """Create .env configuration file automatically."""
    import os
    
    console.print("[bold blue]🔧 Setting up your .env file...[/bold blue]")
    
    # Prompt for GoCode API token if not provided
    if not gocode_token:
        console.print("\n[bold]GoCode API Configuration (Optional):[/bold]")
        console.print("GoCode is GoDaddy's internal AI service for custom SQL generation.")
        console.print("You can skip this and still use all basic validation features.")
        console.print("[dim]Get your API token from: https://caas.godaddy.com/gocode/my-api-keys[/dim]")
        console.print("[dim]Requires GoDaddy VPN connection[/dim]")
        
        gocode_token = click.prompt('GoCode API Token', default='', show_default=False)
    
    # Prompt for GitHub token if not provided
    if not github_token:
        console.print("\n[bold]GitHub Schema Repository Configuration (Optional):[/bold]")
        console.print("Enhance LLM SQL generation with DDL/schema from the gdcorp-dna/lake repository.")
        console.print("This provides more accurate SQL by using actual table schemas and comments.")
        console.print("[dim]Create a GitHub token at: https://github.com/settings/tokens[/dim]")
        console.print("[dim]Requires 'Contents: read' permission for private repos[/dim]")
        
        enable_github = click.confirm('Enable GitHub schema enhancement?', default=True)
        if enable_github:
            github_token = click.prompt('GitHub Token', default='', show_default=False)
    
    # Fixed configuration for your environment
    region = 'us-west-2'
    s3_location = 's3://aws-athena-query-results-255575434142-us-west-2/'
    
    # Athena Notebooks configuration (always used for both regular and Iceberg tables)
    console.print("\n[bold]Athena Notebooks Configuration:[/bold]")
    console.print("Using Athena Notebooks for all data validation (supports both regular and Iceberg tables)")
    
    # Create .env content
    env_content = f"""# AWS Configuration
AWS_REGION={region}

# Athena Configuration (Direct API Access, No Workgroup)
ATHENA_OUTPUT_LOCATION={s3_location}

# Iceberg Support (basic level)
ICEBERG_CATALOG=awsdatacatalog
"""
    
    if gocode_token.strip():
        env_content += f"""
# GoCode API Configuration (GoDaddy's internal AI service)
GOCODE_API_TOKEN={gocode_token.strip()}
GOCAAS_MODEL=claude-3-7-sonnet-20250219
GOCAAS_BASE_URL=https://caas-gocode-prod.caas-prod.prod.onkatana.net
"""
    
    # Add GitHub configuration
    if github_token.strip():
        env_content += f"""
# GitHub Schema Repository Configuration
GITHUB_TOKEN={github_token.strip()}
GITHUB_REPO_OWNER=gdcorp-dna
GITHUB_REPO_NAME=lake
GITHUB_BRANCH=main
ENABLE_GITHUB_SCHEMA=true
"""
    else:
        env_content += f"""
# GitHub Schema Repository Configuration (disabled)
# GITHUB_TOKEN=your_github_token_here
# GITHUB_REPO_OWNER=gdcorp-dna
# GITHUB_REPO_NAME=lake
# GITHUB_BRANCH=main
ENABLE_GITHUB_SCHEMA=false
"""
    
    # Check if .env already exists
    if os.path.exists('.env'):
        if not click.confirm("⚠️  .env file already exists. Overwrite?"):
            console.print("[yellow]Setup cancelled.[/yellow]")
            return
    
    # Write .env file
    try:
        with open('.env', 'w') as f:
            f.write(env_content)
        
        console.print("[green]✅ .env file created successfully![/green]")
        console.print("\n[bold]Your configuration:[/bold]")
        console.print(f"[blue]Region:[/blue] {region}")
        console.print(f"[blue]S3 Location:[/blue] {s3_location}")
        
        if use_notebooks:
            console.print("[green]✅ Athena Notebooks enabled (Iceberg support)[/green]")
        else:
            console.print("[blue]📊 Regular Athena enabled[/blue]")
        
        if gocode_token.strip():
            masked_token = f"{'*' * (len(gocode_token) - 8) + gocode_token[-8:]}" if len(gocode_token) > 8 else "***"
            console.print(f"[blue]GoCode API Token:[/blue] {masked_token}")
            console.print("[green]✅ LLM SQL generation enabled[/green]")
        else:
            console.print("[blue]GoCode API Token:[/blue] Not configured")
            console.print("[yellow]⚠️  LLM SQL generation disabled (basic validation only)[/yellow]")
        
        if github_token.strip():
            masked_github_token = f"{'*' * (len(github_token) - 8) + github_token[-8:]}" if len(github_token) > 8 else "***"
            console.print(f"[blue]GitHub Token:[/blue] {masked_github_token}")
            console.print("[green]✅ Enhanced schema lookup enabled (gdcorp-dna/lake)[/green]")
        else:
            console.print("[blue]GitHub Token:[/blue] Not configured")
            console.print("[yellow]⚠️  Enhanced schema lookup disabled (Athena/Glue only)[/yellow]")
            
        console.print("[blue]Database:[/blue] Dynamic (extracted from table names)")
        
        console.print("\n[bold yellow]Next steps:[/bold yellow]")
        console.print("1. Authenticate with SSO:")
        console.print("   [dim]eval $(aws-okta-processor authenticate -e -o godaddy.okta.com -u your-username)[/dim]")
        console.print("2. Test your setup:")
        console.print("   [dim]python3 setup_aws.py check-credentials[/dim]")
        console.print("3. Start validating:")
        console.print("   [dim]python3 cli.py prompt \"validate tables...\"[/dim]")
        
    except Exception as e:
        console.print(f"[red]❌ Error creating .env file: {e}[/red]")
        raise click.Abort()


@cli.command('validate')
@click.option('--legacy-table', '-l', required=True, help='Legacy table name (e.g., ecomm_mart.fact_bill_line)')
@click.option('--prod-table', '-p', required=False, default='', help='Production table name (optional for single-table mode)')
@click.option('--primary-key', '-k', help='Primary key column name(s) - comma-separated for composite keys (e.g., bill_id or bill_id,line_id)')
@click.option('--date-column', '-d', help='Date column for filtering (e.g., bill_modified_mst_date)')
@click.option('--start-date', '-s', help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e', help='End date (YYYY-MM-DD)')
@click.option('--output-format', '-o', type=click.Choice(['table', 'json', 'csv']), 
              default='table', help='Output format')
def validate(legacy_table: str, prod_table: str, primary_key: str, date_column: str, 
             start_date: str, end_date: str, output_format: str):
    """
    Predefined data validation.
    - Two-table mode: provide -l and -p to compare legacy vs prod
    - Single-table mode: provide only -l for profiling-style checks (row count, optional PK uniqueness, schema summary)
    
    Examples:
    # Two-table
    python3 cli.py validate -l ecomm_mart.fact_bill_line -p enterprise_linked.fact_bill_line -k bill_id
    
    # Single-table
    python3 cli.py validate -l ecomm_mart.fact_bill_line -k bill_id,bill_line_num
    """
    console.print("🎯 [bold blue]Predefined Data Validation[/bold blue]")
    
    # Parse primary key(s) - handle comma-separated composite keys
    primary_key_list = None
    if primary_key and primary_key.strip():
        primary_key_list = [pk.strip() for pk in primary_key.split(',') if pk.strip()]
        primary_key_display = ', '.join(primary_key_list)
        if len(primary_key_list) > 1:
            primary_key_display += ' (composite key)'
    else:
        primary_key_display = 'Not specified (will skip PK validation)'
    
    # Determine mode (single-table vs two-table)
    is_single_table = not prod_table or not str(prod_table).strip()
    
    if is_single_table:
        # Single-table mode
        console.print("\n🔍 [bold]Validation Plan:[/bold]")
        console.print(f"  📊 Table: [blue]{legacy_table}[/blue]")
        console.print(f"  🔑 Primary Key: [blue]{primary_key_display if primary_key_list else 'Not specified (PK uniqueness check skipped)'}[/blue]")
        console.print(f"  📅 Date Column: [blue]{date_column or 'Not specified'}[/blue]")
        if start_date or end_date:
            console.print(f"  📅 Date Range: [blue]{start_date or 'beginning'} to {end_date or 'end'}[/blue]")
        else:
            console.print(f"  📅 Date Range: [blue]All data[/blue]")
        
        console.print("\n🚀 [bold]Validation Types:[/bold]")
        console.print("  ✅ Row Count (single table)")
        console.print("  ✅ Primary Key Uniqueness (if PK provided)")
        console.print("  ✅ Schema Summary (Glue Catalog)")
        
        try:
            console.print("\n🚀 Starting validation...")
            if not validate_config():
                console.print("❌ Configuration validation failed. Please set up your .env file first.")
                return
            
            validator = DataValidator()
            results: List[ValidationResult] = []
            from datetime import datetime as _dt
            start_ts = _dt.now()
            
            # Build optional WHERE conditions
            where_clauses: List[str] = []
            if date_column and (start_date or end_date):
                date_filter = validator._build_date_filter(date_column, start_date, end_date)
                if date_filter:
                    where_clauses.append(date_filter)
            where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
            
            # Row count
            row_sql = f"SELECT COUNT(*) as row_count FROM {legacy_table}{where_sql}"
            row_res = validator.athena_client.execute_query(row_sql)
            row_count = row_res[0]['row_count'] if row_res else 0
            results.append(ValidationResult(
                rule_name="Row Count (Single Table)",
                status=ValidationStatus.INFO,
                legacy_value=row_count,
                prod_value=None,
                message=f"Row count: {row_count:,}"
            ))
            
            # PK uniqueness (optional)
            if primary_key_list:
                if len(primary_key_list) == 1:
                    pk_col = primary_key_list[0]
                    pk_where = [f"{pk_col} IS NOT NULL"] + where_clauses
                    pk_where_sql = (" WHERE " + " AND ".join(pk_where)) if pk_where else ""
                    pk_sql = f"SELECT COUNT(*) as total_rows, COUNT(DISTINCT {pk_col}) as distinct_pk_count FROM {legacy_table}{pk_where_sql}"
                else:
                    concat_expr = "CONCAT(" + ", '|', ".join([f"CAST({col} AS VARCHAR)" for col in primary_key_list]) + ")"
                    not_nulls = [f"{col} IS NOT NULL" for col in primary_key_list]
                    pk_where = not_nulls + where_clauses
                    pk_where_sql = (" WHERE " + " AND ".join(pk_where)) if pk_where else ""
                    pk_sql = f"SELECT COUNT(*) as total_rows, COUNT(DISTINCT {concat_expr}) as distinct_pk_count FROM {legacy_table}{pk_where_sql}"
                pk_res = validator.athena_client.execute_query(pk_sql)
                total_rows = pk_res[0]['total_rows'] if pk_res else 0
                distinct_pk = pk_res[0]['distinct_pk_count'] if pk_res else 0
                unique_pct = (distinct_pk / max(total_rows, 1)) * 100
                pk_status = ValidationStatus.PASS if unique_pct == 100 else ValidationStatus.FAIL
                pk_message = f"PK uniqueness: {unique_pct:.2f}% ({distinct_pk:,}/{total_rows:,} unique)"
                results.append(ValidationResult(
                    rule_name="Primary Key Uniqueness (Single Table)",
                    status=pk_status,
                    legacy_value={"total": total_rows, "unique": distinct_pk, "unique_pct": unique_pct},
                    prod_value=None,
                    message=pk_message
                ))
            
            # Schema summary via Glue (INFO)
            dt_rule = DataTypeValidation()
            schema_result = dt_rule.validate_tables_direct(legacy_table, legacy_table)
            schema_result.rule_name = "Schema Summary (Glue Catalog)"
            results.append(schema_result)
            
            # Build report-like object for consistent display
            exec_time = (_dt.now() - start_ts).total_seconds()
            report = ValidationReport(
                legacy_table=legacy_table,
                prod_table=legacy_table,
                validation_results=results,
                execution_time=exec_time,
                timestamp=_dt.now(),
                summary="; ".join([r.message for r in results if r.message]),
                total_checks=len(results),
                passed_checks=len([r for r in results if r.status == ValidationStatus.PASS]),
                failed_checks=len([r for r in results if r.status == ValidationStatus.FAIL]),
                error_checks=len([r for r in results if r.status == ValidationStatus.ERROR])
            )
            
            _display_results(report, output_format)
            console.print(f"\n✅ [bold green]Single-table validation completed successfully![/bold green]")
            console.print(f"📊 Executed {report.total_checks} checks in {report.execution_time:.2f}s")
        except Exception as e:
            console.print(f"❌ [bold red]Validation failed:[/bold red] {str(e)}")
            raise click.Abort()
        return
    
    # Two-table mode
    # Show validation plan
    console.print("\n🔍 [bold]Validation Plan:[/bold]")
    console.print(f"  📊 Legacy Table: [blue]{legacy_table}[/blue]")
    console.print(f"  📊 Production Table: [blue]{prod_table}[/blue]")
    console.print(f"  🔑 Primary Key: [blue]{primary_key_display}[/blue]")
    console.print(f"  📅 Date Column: [blue]{date_column or 'Not specified'}[/blue]")
    if start_date or end_date:
        date_range = f"{start_date or 'beginning'} to {end_date or 'end'}"
        console.print(f"  📅 Date Range: [blue]{date_range}[/blue]")
    else:
        console.print(f"  📅 Date Range: [blue]All data[/blue]")
    
    console.print("\n🚀 [bold]Validation Types:[/bold]")
    console.print("  ✅ Row Count Comparison")
    if primary_key_list:
        if len(primary_key_list) > 1:
            console.print(f"  ✅ Primary Key Validation (composite: {len(primary_key_list)} columns)")
        else:
            console.print("  ✅ Primary Key Validation")
    else:
        console.print("  ⏸️  Primary Key Validation (skipped - no PK specified)")
    console.print("  ✅ Schema/Data Type Comparison")
    
    # Execute validation
    try:
        console.print("\n🚀 Starting validation...")
        
        if not validate_config():
            console.print("❌ Configuration validation failed. Please set up your .env file first.")
            return
        
        validator = DataValidator()
        
        result = validator.validate_tables(
            legacy_table=legacy_table,
            prod_table=prod_table,
            primary_key_columns=primary_key_list,
            date_column=date_column,
            start_date=start_date,
            end_date=end_date
        )
        
        # Display results
        _display_results(result, output_format)
        
        # Success message with summary
        console.print(f"\n✅ [bold green]Validation completed successfully![/bold green]")
        console.print(f"📊 Executed {result.total_checks} validation checks in {result.execution_time:.2f}s")
        
    except Exception as e:
        console.print(f"❌ [bold red]Validation failed:[/bold red] {str(e)}")
        raise click.Abort()


@cli.command('validate-single')
@click.option('--table', '-t', required=True, help='Table name (e.g., ecomm_mart.fact_bill_line)')
@click.option('--primary-key', '-k', help='Primary key column name(s) - comma-separated for composite keys (optional)')
@click.option('--date-column', '-d', help='Date column for filtering (e.g., bill_modified_mst_date)')
@click.option('--start-date', '-s', help='Start date (YYYY-MM-DD)')
@click.option('--end-date', '-e', help='End date (YYYY-MM-DD)')
@click.option('--output-format', '-o', type=click.Choice(['table', 'json', 'csv']),
              default='table', help='Output format')
def validate_single(table: str, primary_key: str, date_column: str, start_date: str, end_date: str, output_format: str):
    """Predefined single-table validation (profiling-style checks without LLM)."""
    console.print("🧪 [bold blue]Predefined Single-Table Validation[/bold blue]")
    
    # Parse primary key(s)
    primary_key_list: Optional[List[str]] = None
    if primary_key and primary_key.strip():
        primary_key_list = [pk.strip() for pk in primary_key.split(',') if pk.strip()]
        pk_display = ', '.join(primary_key_list) + (" (composite key)" if len(primary_key_list) > 1 else "")
    else:
        pk_display = 'Not specified (PK uniqueness check skipped)'
    
    console.print("\n🔍 [bold]Validation Plan:[/bold]")
    console.print(f"  📊 Table: [blue]{table}[/blue]")
    console.print(f"  🔑 Primary Key: [blue]{pk_display}[/blue]")
    console.print(f"  📅 Date Column: [blue]{date_column or 'Not specified'}[/blue]")
    if start_date or end_date:
        console.print(f"  📅 Date Range: [blue]{start_date or 'beginning'} to {end_date or 'end'}[/blue]")
    else:
        console.print(f"  📅 Date Range: [blue]All data[/blue]")
    
    if not validate_config():
        console.print("❌ Configuration validation failed. Please set up your .env file first.")
        return
    
    validator = DataValidator()
    results: List[ValidationResult] = []
    start_ts = datetime.now()
    
    # Build optional WHERE conditions
    where_clauses: List[str] = []
    if date_column and (start_date or end_date):
        # Reuse DataValidator's date filter builder
        date_filter = validator._build_date_filter(date_column, start_date, end_date)
        if date_filter:
            where_clauses.append(date_filter)
    
    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    
    try:
        # Row count
        row_sql = f"SELECT COUNT(*) as row_count FROM {table}{where_sql}"
        row_res = validator.athena_client.execute_query(row_sql)
        row_count = row_res[0]['row_count'] if row_res else 0
        results.append(ValidationResult(
            rule_name="Row Count (Single Table)",
            status=ValidationStatus.INFO,
            legacy_value=row_count,
            prod_value=None,
            message=f"Row count: {row_count:,}"
        ))
        
        # PK uniqueness (optional)
        if primary_key_list:
            if len(primary_key_list) == 1:
                pk_col = primary_key_list[0]
                pk_where = [f"{pk_col} IS NOT NULL"] + where_clauses
                pk_where_sql = (" WHERE " + " AND ".join(pk_where)) if pk_where else ""
                pk_sql = f"SELECT COUNT(*) as total_rows, COUNT(DISTINCT {pk_col}) as distinct_pk_count FROM {table}{pk_where_sql}"
            else:
                concat_expr = "CONCAT(" + ", '|', ".join([f"CAST({col} AS VARCHAR)" for col in primary_key_list]) + ")"
                not_nulls = [f"{col} IS NOT NULL" for col in primary_key_list]
                pk_where = not_nulls + where_clauses
                pk_where_sql = (" WHERE " + " AND ".join(pk_where)) if pk_where else ""
                pk_sql = f"SELECT COUNT(*) as total_rows, COUNT(DISTINCT {concat_expr}) as distinct_pk_count FROM {table}{pk_where_sql}"
            
            pk_res = validator.athena_client.execute_query(pk_sql)
            total_rows = pk_res[0]['total_rows'] if pk_res else 0
            distinct_pk = pk_res[0]['distinct_pk_count'] if pk_res else 0
            unique_pct = (distinct_pk / max(total_rows, 1)) * 100
            pk_status = ValidationStatus.PASS if unique_pct == 100 else ValidationStatus.FAIL
            pk_message = f"PK uniqueness: {unique_pct:.2f}% ({distinct_pk:,}/{total_rows:,} unique)"
            results.append(ValidationResult(
                rule_name="Primary Key Uniqueness (Single Table)",
                status=pk_status,
                legacy_value={"total": total_rows, "unique": distinct_pk, "unique_pct": unique_pct},
                prod_value=None,
                message=pk_message
            ))
        
        # Schema summary via Glue (INFO)
        dt_rule = DataTypeValidation()
        schema_result = dt_rule.validate_tables_direct(table, table)
        schema_result.rule_name = "Schema Summary (Glue Catalog)"
        results.append(schema_result)
        
        # Build report-like object for consistent display
        exec_time = (datetime.now() - start_ts).total_seconds()
        report = ValidationReport(
            legacy_table=table,
            prod_table=table,
            validation_results=results,
            execution_time=exec_time,
            timestamp=datetime.now(),
            summary="; ".join([r.message for r in results if r.message]),
            total_checks=len(results),
            passed_checks=len([r for r in results if r.status == ValidationStatus.PASS]),
            failed_checks=len([r for r in results if r.status == ValidationStatus.FAIL]),
            error_checks=len([r for r in results if r.status == ValidationStatus.ERROR])
        )
        
        _display_results(report, output_format)
        console.print(f"\n✅ [bold green]Single-table validation completed successfully![/bold green]")
        console.print(f"📊 Executed {report.total_checks} checks in {report.execution_time:.2f}s")
    except Exception as e:
        console.print(f"❌ [bold red]Single-table validation failed:[/bold red] {str(e)}")
        raise click.Abort()


@cli.command()
@click.option('--legacy-table', '-l', required=True, help='Legacy table name')
@click.option('--prod-table', '-p', required=True, help='Production table name')
@click.option('--legacy-sql', required=True, help='SQL query for legacy table')
@click.option('--prod-sql', required=True, help='SQL query for production table')
@click.option('--validation-name', '-n', default='Custom SQL Validation', help='Name for this validation')
def custom_sql(
    legacy_table: str,
    prod_table: str,
    legacy_sql: str,
    prod_sql: str,
    validation_name: str
):
    """Run validation with custom SQL queries."""
    
    console.print(f"[bold blue]🔍 Custom SQL Validation[/bold blue]")
    console.print(f"Validation: [yellow]{validation_name}[/yellow]")
    
    validator = DataValidator()
    
    try:
        with console.status("[bold green]Executing custom SQL..."):
            result = validator.validate_with_custom_sql(
                legacy_table=legacy_table,
                prod_table=prod_table,
                legacy_sql=legacy_sql,
                prod_sql=prod_sql,
                validation_name=validation_name
            )
        
        # Display single result
        display_single_result(result)
        
    except Exception as e:
        console.print(f"[red]❌ Custom SQL validation failed: {str(e)}[/red]")
        raise click.Abort()


@cli.command('notebook-session')
@click.option('-l', '--legacy-table', required=True, help='Legacy table name (can be Iceberg)')
@click.option('-p', '--prod-table', required=True, help='Production table name')
@click.option('-k', '--primary-key', help='Primary key column name')
@click.option('--workgroup', default='de-ckpetlbatch-spark-common', help='Spark workgroup for Iceberg support')
@click.option('--generate-notebook', is_flag=True, help='Generate notebook content for manual execution')
def notebook_session(legacy_table, prod_table, primary_key, workgroup, generate_notebook):
    """Create notebook session for Iceberg table validation."""
    from notebook_session_client import NotebookSessionClient
    from config import Settings
    from rich.panel import Panel
    from rich.syntax import Syntax
    
    console.print("🧊 [bold cyan]Iceberg Table Notebook Session[/bold cyan]")
    
    # Configure for Spark workgroup
    settings = Settings(
        athena_workgroup=workgroup,
        athena_notebook_instance='data-validation_notebook'
    )
    
    client = NotebookSessionClient(settings)
    
    console.print(f"📋 Legacy Table: [yellow]{legacy_table}[/yellow]")
    console.print(f"📋 Production Table: [yellow]{prod_table}[/yellow]")
    console.print(f"📍 Workgroup: [yellow]{workgroup}[/yellow]")
    
    if generate_notebook:
        console.print("\n🚀 Generating validation notebook...")
        
        # Generate notebook content
        notebook_content = client.generate_validation_notebook(
            legacy_table=legacy_table,
            prod_table=prod_table,
            primary_key=primary_key
        )
        
        # Save notebook to file
        notebook_file = f"validation_notebook_{legacy_table.replace('.', '_')}_{prod_table.replace('.', '_')}.md"
        with open(notebook_file, 'w') as f:
            f.write(notebook_content)
        
        console.print(f"✅ Notebook saved to: [green]{notebook_file}[/green]")
        
        # Show setup instructions
        compatibility = client.test_notebook_compatibility()
        instructions = compatibility['setup_instructions']
        
        setup_text = f"""1. Open AWS Athena Console: {instructions['console_access']}
2. Navigate to Notebooks
3. Select workgroup: [bold]{instructions['workgroup_selection']}[/bold]
4. Create or open notebook: [bold]data-validation_notebook[/bold]
5. Configure Spark properties:
   {chr(10).join('   - ' + prop for prop in instructions['required_spark_properties'])}
6. Open the generated notebook file: [bold]{notebook_file}[/bold]
7. Copy and paste the content into your Athena notebook
8. Execute each cell step by step"""
        
        console.print(Panel(setup_text, title="📋 Setup Instructions", border_style="green"))
        
    else:
        # Test Iceberg table access
        console.print("\n🧪 Testing Iceberg table access...")
        
        table_info = client.get_iceberg_table_info(legacy_table)
        if 'error' not in table_info:
            console.print("✅ Iceberg table info generated")
            console.print(f"Available queries: {list(table_info['queries'].keys())}")
            
            # Show sample query
            sample_query = table_info['queries']['sample_data']
            syntax = Syntax(f"%%sql\n{sample_query}", "sql", theme="monokai", line_numbers=True)
            console.print(Panel(syntax, title="📝 Sample Query for Notebook"))
        else:
            console.print(f"❌ Error: {table_info['error']}")
    
    console.print("✅ [bold green]Notebook session setup completed![/bold green]")




def _extract_tables_and_dates_from_prompt(prompt: str) -> dict:
    """Extract table names and date ranges from natural language prompt."""
    import re
    
    result = {
        'tables': [],
        'start_date': None,
        'end_date': None,
        'date_column': None
    }
    
    # Extract table names (database.table format and standalone tables)
    table_patterns = [
        # Full database.table format
        r'\b(\w+\.\w+(?:_\w+)*)\b',
        # Common table patterns
        r'\b(fact_\w+)\b', r'\b(dim_\w+)\b', r'\b(\w+_fact)\b', r'\b(\w+_dim)\b',
        r'\b(\w+_xref)\b', r'\b(\w+_lookalike)\b', r'\b(\w+_mart)\b'
    ]
    
    found_tables = set()
    for pattern in table_patterns:
        matches = re.findall(pattern, prompt, re.IGNORECASE)
        found_tables.update(matches)
    
    # Filter out common words that aren't tables
    excluded_words = {'between', 'from', 'where', 'select', 'table', 'tables', 'data', 'record', 'records'}
    result['tables'] = [t for t in found_tables if t.lower() not in excluded_words and len(t) > 3]
    
    # Extract date ranges
    date_patterns = [
        # YYYY-MM-DD to YYYY-MM-DD
        r'between\s+(\d{4}-\d{2}-\d{2})\s+(?:to|and)\s+(\d{4}-\d{2}-\d{2})',
        r'from\s+(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})',
        r'(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})',
        # Single dates
        r'(?:after|since|from)\s+(\d{4}-\d{2}-\d{2})',
        r'(?:before|until|to)\s+(\d{4}-\d{2}-\d{2})'
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, prompt, re.IGNORECASE)
        if match:
            if len(match.groups()) == 2:  # Date range
                result['start_date'] = match.group(1)
                result['end_date'] = match.group(2)
            else:  # Single date
                if 'after' in pattern or 'since' in pattern or 'from' in pattern:
                    result['start_date'] = match.group(1)
                else:
                    result['end_date'] = match.group(1)
            break
    
    # Try to detect date column mentions
    date_column_patterns = [
        r'\b(\w*date\w*)\b', r'\b(\w*time\w*)\b', r'\b(\w*created\w*)\b', r'\b(\w*modified\w*)\b'
    ]
    
    for pattern in date_column_patterns:
        matches = re.findall(pattern, prompt, re.IGNORECASE)
        for match in matches:
            if len(match) > 4 and 'date' in match.lower():
                result['date_column'] = match
                break
        if result['date_column']:
            break
    
    return result


@cli.command('llm-validate')
@click.argument('validation_request', required=True)
@click.option('--tables', '-t', help='Table names (optional - will auto-extract from prompt if not provided)')
@click.option('--date-column', '-d', help='Date column for filtering (optional - will auto-detect from prompt)')
@click.option('--start-date', '-s', help='Start date (optional - will auto-extract from prompt)')
@click.option('--end-date', '-e', help='End date (optional - will auto-extract from prompt)')
@click.option('--primary-key', '-k', help='Primary key column(s) for context (optional)')
@click.option('--output-format', '-o', type=click.Choice(['table', 'json', 'csv']), 
              default='table', help='Output format')
def llm_validate(validation_request: str, tables: str, date_column: str, start_date: str, 
                end_date: str, primary_key: str, output_format: str):
    """
    🤖 LLM-powered data validation using natural language.
    
    This command uses GoCode/GoCaaS (GoDaddy's AI service) to generate custom SQL based on your 
    natural language description. You can describe any validation you want to perform.
    
    ✨ NEW: Auto-extracts table names and dates from your prompt!
    
    Examples:
    
    🎯 SMART EXTRACTION (No extra parameters needed):
    python3 cli.py llm-validate "check shopper_id mismatch between enterprise_linked.dim_bill_shopper_id_xref and enterprise.dim_bill_shopper_id_xref_lookalike from 2025-07-19 to 2025-07-24"
    
    python3 cli.py llm-validate "compare fact_bill_line and fact_order tables for data quality issues between 2025-01-01 and 2025-01-31"
    
    python3 cli.py llm-validate "find missing records in dim_customer table after 2025-06-01"
    
    🔧 MANUAL PARAMETERS (Traditional way):
    python3 cli.py llm-validate "compare row counts" -t "table1,table2" -s "2025-01-01" -e "2025-01-31"
    """
    
    console.print("🤖 [bold blue]LLM-Powered Data Validation[/bold blue]")
    console.print(f"📝 Request: [yellow]{validation_request}[/yellow]")
    
    # Check if GoCaaS is configured
    if not validate_config():
        console.print("❌ Configuration validation failed. Please set up your .env file first.")
        return
    
    # Check if GoCaaS is available
    try:
        from llm_sql_generator import SQLGenerator
        sql_generator = SQLGenerator()
        
        # Test AI service availability (GoCaaS or GoCode)
        if sql_generator.auth_type == 'none':
            console.print("\n[red]❌ No AI service credentials found![/red]")
            console.print("This command requires either GoCaaS or GoCode API for LLM SQL generation.")
            console.print("\n[bold]Option 1 - Old GoCaaS:[/bold]")
            console.print("  Set GOCAAS_KEY_ID and GOCAAS_SECRET_KEY in .env")
            console.print("\n[bold]Option 2 - New GoCode:[/bold]")
            console.print("  1. Go to: [blue]https://caas.godaddy.com/gocode/my-api-keys[/blue]")
            console.print("  2. Create API key and set GOCODE_API_TOKEN in .env")
            console.print("  3. Requires GoDaddy VPN connection")
            return
            
    except Exception as e:
        console.print(f"[red]❌ Error initializing LLM generator: {str(e)}[/red]")
        return
    
    # Extract information from prompt if parameters not provided
    extracted_info = _extract_tables_and_dates_from_prompt(validation_request)
    
    # Use extracted or provided table names
    if not tables and extracted_info['tables']:
        table_list = extracted_info['tables']
        console.print(f"🔍 Auto-detected tables: [green]{', '.join(table_list)}[/green]")
    elif tables:
        table_list = [t.strip() for t in tables.split(',') if t.strip()]
    else:
        tables = click.prompt(
            '📊 Enter table names (comma-separated, e.g., "table1,table2" or "db1.table1,db2.table2")',
            type=str
        )
        table_list = [t.strip() for t in tables.split(',') if t.strip()]
    
    # Use extracted dates if not provided
    if not start_date and extracted_info['start_date']:
        start_date = extracted_info['start_date']
        console.print(f"🗓️  Auto-detected start date: [green]{start_date}[/green]")
    
    if not end_date and extracted_info['end_date']:
        end_date = extracted_info['end_date']
        console.print(f"🗓️  Auto-detected end date: [green]{end_date}[/green]")
    
    if not date_column and extracted_info['date_column']:
        date_column = extracted_info['date_column']
        console.print(f"📅 Auto-detected date column: [green]{date_column}[/green]")
    
    # Parse table names
    if len(table_list) < 1:
        console.print("[red]❌ At least one table is required.[/red]")
        return
    elif len(table_list) == 1:
        legacy_table = table_list[0]
        prod_table = None
        console.print(f"📊 Single table analysis: [blue]{legacy_table}[/blue]")
    elif len(table_list) == 2:
        legacy_table = table_list[0]
        prod_table = table_list[1]
        console.print(f"📊 Table comparison: [blue]{legacy_table}[/blue] vs [blue]{prod_table}[/blue]")
    else:
        console.print("[yellow]⚠️  More than 2 tables specified. Using first two for comparison.[/yellow]")
        legacy_table = table_list[0]
        prod_table = table_list[1]
    
    # Show request details
    console.print(f"🔑 Primary Key: [blue]{primary_key or 'Not specified'}[/blue]")
    console.print(f"📅 Date Column: [blue]{date_column or 'Not specified'}[/blue]")
    if start_date or end_date:
        date_range = f"{start_date or 'beginning'} to {end_date or 'end'}"
        console.print(f"📅 Date Range: [blue]{date_range}[/blue]")
    
    try:
        console.print("\n🧠 [bold]Generating SQL with LLM...[/bold]")
        
        # Generate SQL using LLM
        with console.status("[bold green]🤖 LLM is thinking..."):
            try:
                sql_result = sql_generator.generate_validation_sql(
                    legacy_table=legacy_table,
                    prod_table=prod_table or legacy_table,  # Use same table if only one specified
                    validation_request=validation_request,
                    date_column=date_column,
                    start_date=start_date,
                    end_date=end_date
                )
            except Exception as e:
                console.print(f"\n[yellow]⚠️  LLM generation failed: {str(e)}[/yellow]")
                console.print("[blue]💡 Falling back to basic SQL generation...[/blue]")
                
                # Fallback: Generate basic SQL based on request keywords
                sql_result = _generate_fallback_sql(
                    validation_request, legacy_table, prod_table, 
                    date_column, start_date, end_date
                )
        
        if not sql_result or 'legacy_sql' not in sql_result:
            console.print("[red]❌ Failed to generate SQL. Please try rephrasing your request.[/red]")
            return
        
        # Show generated SQL
        console.print("\n[bold]🔍 Generated SQL Queries:[/bold]")
        console.print(f"\n[bold blue]Legacy/Primary Table Query:[/bold blue]")
        console.print(f"[dim]{sql_result['legacy_sql']}[/dim]")
        
        if prod_table and 'prod_sql' in sql_result and sql_result['prod_sql'].strip():
            console.print(f"\n[bold green]Production/Secondary Table Query:[/bold green]")
            console.print(f"[dim]{sql_result['prod_sql']}[/dim]")
        elif prod_table:
            console.print(f"\n[bold cyan]💡 Smart Strategy:[/bold cyan] Using single comprehensive query for comparison")
        
        if 'explanation' in sql_result:
            console.print(f"\n[bold yellow]Explanation:[/bold yellow] {sql_result['explanation']}")
        
        if not click.confirm("\nProceed with executing these queries?", default=True):
            console.print("[yellow]Execution cancelled.[/yellow]")
            return
        
        # Execute the generated SQL
        console.print("\n🚀 [bold]Executing queries...[/bold]")
        
        validator = DataValidator()
        
        if prod_table and 'prod_sql' in sql_result and sql_result['prod_sql'].strip():
            # Two separate queries
            result = validator.validate_with_custom_sql(
                legacy_table=legacy_table,
                prod_table=prod_table,
                legacy_sql=sql_result['legacy_sql'],
                prod_sql=sql_result['prod_sql'],
                validation_name=f"LLM Validation: {validation_request}"
            )
            
            # Display comparison result
            display_single_result(result)
            
        else:
            # Single comprehensive query (or single table analysis)
            with console.status("[bold green]Executing comprehensive query..."):
                result_data = validator.athena_client.execute_query(sql_result['legacy_sql'])
            
            table_name = f"{legacy_table} vs {prod_table}" if prod_table else legacy_table
            console.print(f"\n[bold green]✅ Query Results for {table_name}:[/bold green]")
            
            if output_format == 'table':
                _display_query_results_as_table(result_data)
            elif output_format == 'json':
                import json
                console.print(json.dumps(result_data, indent=2, default=str))
            elif output_format == 'csv':
                _display_query_results_as_csv(result_data)
        
        # Generate LLM explanation of results
        console.print("\n🧠 [bold]LLM Analysis of Results:[/bold]")
        try:
            with console.status("[bold green]🤖 LLM is analyzing results..."):
                if prod_table and 'prod_sql' in sql_result and sql_result['prod_sql'].strip():
                    # Two separate queries - use existing analysis
                    analysis = sql_generator.explain_validation_results([result], legacy_table, prod_table)
                else:
                    # Single comprehensive query - analyze the results directly
                    table_description = f"{legacy_table} vs {prod_table}" if prod_table else legacy_table
                    analysis = sql_generator._call_gocaas(
                        messages=[
                            {"role": "system", "content": "You are a data analyst providing insights on query results."},
                            {"role": "user", "content": f"Analyze these query results from {table_description} for the request '{validation_request}':\n\n{str(result_data)}"}
                        ],
                        max_tokens=500
                    )
                
                console.print(f"[bold cyan]💡 Insights:[/bold cyan] {analysis}")
                
        except Exception as e:
            console.print(f"[yellow]⚠️  Could not generate LLM analysis: {str(e)}[/yellow]")
        
        console.print(f"\n✅ [bold green]LLM validation completed successfully![/bold green]")
        
    except Exception as e:
        error_str = str(e)
        if 'JSON' in error_str or 'property name' in error_str or 'Expecting' in error_str:
            console.print(f"❌ [bold red]LLM JSON parsing failed:[/bold red] The AI response was malformed")
            console.print("💡 [yellow]This typically means the AI model returned invalid JSON. The system should have automatically fallen back to safe SQL generation.[/yellow]")
        else:
            console.print(f"❌ [bold red]LLM validation failed:[/bold red] {error_str}")
        raise click.Abort()


@cli.command('test-gocode')
def test_gocode():
    """Test GoCode API connectivity and configuration."""
    console.print("🔍 [bold blue]GoCode API Diagnostics[/bold blue]")
    console.print("📝 [dim]Note: This test only checks GoCode API connectivity, not AWS credentials.[/dim]")
    
    try:
        from config import settings
        from llm_sql_generator import SQLGenerator
        
        # Check configuration
        console.print("\n📋 [bold]Configuration Check:[/bold]")
        
        if settings.gocode_api_token:
            token_preview = settings.gocode_api_token[:20] + "..." if len(settings.gocode_api_token) > 20 else settings.gocode_api_token
            console.print(f"   GoCode Token: [green]{token_preview}[/green]")
        else:
            console.print(f"   GoCode Token: [red]Not configured[/red]")
            console.print("   💡 Run: python3 cli.py setup-env --gocode-token YOUR_TOKEN")
            return
        
        base_url = getattr(settings, 'gocaas_base_url', 'https://caas-gocode-prod.caas-prod.prod.onkatana.net')
        console.print(f"   API Base URL: [yellow]{base_url}[/yellow]")
        
        # Test API connectivity
        console.print("\n🔗 [bold]Connectivity Test:[/bold]")
        
        sql_generator = SQLGenerator()
        
        if sql_generator.auth_type != 'gocode':
            console.print(f"   [red]❌ Not using GoCode (using: {sql_generator.auth_type})[/red]")
            return
        
        # Simple test call
        try:
            with console.status("[bold green]Testing GoCode API..."):
                response = sql_generator._call_gocode_api(
                    messages=[
                        {"role": "system", "content": "You are a test assistant."},
                        {"role": "user", "content": "Say 'API test successful' and nothing else."}
                    ],
                    max_tokens=50
                )
            
            console.print(f"   [green]✅ API Response: {response}[/green]")
            console.print("\n🎉 [bold green]GoCode API is working correctly![/bold green]")
            
        except Exception as e:
            error_str = str(e)
            console.print(f"   [red]❌ API Error: {error_str}[/red]")
            
            # Provide specific guidance based on error
            if "404" in error_str or "endpoint not found" in error_str.lower():
                console.print("\n💡 [yellow]Possible solutions:[/yellow]")
                console.print("   1. Check if you're connected to GoDaddy VPN")
                console.print("   2. Verify the API URL in your configuration")
                console.print("   3. Contact the GoCode team for current API endpoint")
            elif "timeout" in error_str.lower() or "connection" in error_str.lower():
                console.print("\n💡 [yellow]Possible solutions:[/yellow]")
                console.print("   1. Check your VPN connection to GoDaddy network")
                console.print("   2. Test network connectivity")
                console.print("   3. Try again in a few minutes")
            elif "401" in error_str or "unauthorized" in error_str.lower():
                console.print("\n💡 [yellow]Possible solutions:[/yellow]")
                console.print("   1. Get a new API token from https://caas.godaddy.com/gocode/my-api-keys")
                console.print("   2. Re-run: python3 cli.py setup-env --gocode-token NEW_TOKEN")
            else:
                console.print("\n💡 [yellow]General troubleshooting:[/yellow]")
                console.print("   1. Verify VPN connection")
                console.print("   2. Check API token validity") 
                console.print("   3. Try the fallback mode instead")
            
            console.print("\n🛡️  [cyan]Note: The system will automatically use safe fallback SQL when GoCode is unavailable.[/cyan]")
    
    except Exception as e:
        console.print(f"[red]❌ Diagnostic failed: {str(e)}[/red]")


def display_validation_report(report: ValidationReport, output_format: str):
    """Display validation report in specified format."""
    
    if output_format == 'json':
        report_dict = {
            'legacy_table': report.legacy_table,
            'prod_table': report.prod_table,
            'timestamp': report.timestamp.isoformat(),
            'execution_time': report.execution_time,
            'summary': report.summary,
            'total_checks': report.total_checks,
            'passed_checks': report.passed_checks,
            'failed_checks': report.failed_checks,
            'error_checks': report.error_checks,
            'results': []
        }
        
        for result in report.validation_results:
            report_dict['results'].append({
                'rule_name': result.rule_name,
                'status': result.status.value,
                'legacy_value': result.legacy_value,
                'prod_value': result.prod_value,
                'difference': result.difference,
                'percentage_diff': result.percentage_diff,
                'message': result.message,
                'error_details': result.error_details
            })
        
        console.print(json.dumps(report_dict, indent=2))
        return
    
    if output_format == 'csv':
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Rule Name', 'Status', 'Legacy Value', 'Prod Value', 'Difference', 'Message'])
        
        for result in report.validation_results:
            writer.writerow([
                result.rule_name,
                result.status.value,
                str(result.legacy_value),
                str(result.prod_value),
                str(result.difference),
                result.message
            ])
        
        console.print(output.getvalue())
        return
    
    # Table format (default)
    console.print()
    console.print(Panel.fit(
        f"[bold]Validation Report[/bold]\n"
        f"Legacy Table: {report.legacy_table}\n"
        f"Production Table: {report.prod_table}\n"
        f"Execution Time: {report.execution_time:.2f}s\n"
        f"Timestamp: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
    ))
    
    # Results table
    table = Table()
    table.add_column("Rule Name", style="bold")
    table.add_column("Status")
    table.add_column("Legacy Value")
    table.add_column("Prod Value")
    table.add_column("Difference")
    table.add_column("Message")
    
    for result in report.validation_results:
        if result.status == ValidationStatus.PASS:
            status_style = "green"
        elif result.status == ValidationStatus.FAIL:
            status_style = "red"
        elif result.status == ValidationStatus.INFO:
            status_style = "blue"
        else:  # ERROR
            status_style = "yellow"
        
        table.add_row(
            result.rule_name,
            f"[{status_style}]{result.status.value}[/{status_style}]",
            str(result.legacy_value) if result.legacy_value is not None else "N/A",
            str(result.prod_value) if result.prod_value is not None else "N/A",
            str(result.difference) if result.difference is not None else "N/A",
            result.message + (f" | ERROR DETAILS: {result.error_details}" if result.status == ValidationStatus.ERROR and hasattr(result, 'error_details') and result.error_details else "")
        )
    
    console.print(table)
    
    # Summary
    console.print()
    console.print(Panel.fit(
        f"[bold]Summary[/bold]\n"
        f"Total Checks: {report.total_checks}\n"
        f"✅ Passed: {report.passed_checks}\n"
        f"❌ Failed: {report.failed_checks}\n"
        f"⚠️  Errors: {report.error_checks}\n\n"
        f"{report.summary}"
    ))


def display_single_result(result):
    """Display a single validation result."""
    if result.status == ValidationStatus.PASS:
        status_style = "green"
    elif result.status == ValidationStatus.FAIL:
        status_style = "red"
    elif result.status == ValidationStatus.INFO:
        status_style = "blue"
    else:  # ERROR
        status_style = "yellow"
    
    console.print()
    console.print(Panel.fit(
        f"[bold]{result.rule_name}[/bold]\n"
        f"Status: [{status_style}]{result.status.value}[/{status_style}]\n"
        f"Message: {result.message}\n"
        f"Legacy Value: {result.legacy_value}\n"
        f"Prod Value: {result.prod_value}"
    ))



def _display_results(report, output_format: str):
    """Display validation results in the specified format."""
    if output_format == 'json':
        import json
        report_dict = {
            'legacy_table': report.legacy_table,
            'prod_table': report.prod_table,
            'execution_time': report.execution_time,
            'timestamp': report.timestamp.isoformat(),
            'validation_results': [
                {
                    'rule_name': result.rule_name,
                    'status': result.status.value,
                    'legacy_value': result.legacy_value,
                    'prod_value': result.prod_value,
                    'difference': result.difference,
                    'message': result.message,
                    'error_details': getattr(result, 'error_details', None)
                }
                for result in report.validation_results
            ],
            'summary': {
                'total_checks': len(report.validation_results),
                'passed': len([r for r in report.validation_results if r.status.value == 'PASS']),
                'failed': len([r for r in report.validation_results if r.status.value == 'FAIL']),
                'errors': len([r for r in report.validation_results if r.status.value == 'ERROR'])
            }
        }
        console.print(json.dumps(report_dict, indent=2))
    elif output_format == 'csv':
        console.print("rule_name,status,legacy_value,prod_value,difference,message")
        for result in report.validation_results:
            console.print(f"{result.rule_name},{result.status.value},{result.legacy_value},{result.prod_value},{result.difference},\"{result.message}\"")
    else:
        # Display table format (default)
        console.print()
        console.print("📊 Validation Report")
        console.print(f"Legacy Table: {report.legacy_table}")
        console.print(f"Production Table: {report.prod_table}")
        console.print(f"Execution Time: {report.execution_time:.2f}s")
        console.print()
        
        # Create results table
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Rule Name", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Legacy Value", justify="right")
        table.add_column("Prod Value", justify="right") 
        table.add_column("Difference", justify="right")
        table.add_column("Message", style="yellow")
        
        for result in report.validation_results:
            status_color = "green" if result.status.value == "PASS" else "red" if result.status.value == "FAIL" else "yellow"
            table.add_row(
                result.rule_name,
                f"[{status_color}]{result.status.value}[/{status_color}]",
                str(result.legacy_value) if result.legacy_value is not None else "N/A",
                str(result.prod_value) if result.prod_value is not None else "N/A",
                str(result.difference) if result.difference is not None else "N/A",
                result.message + (f" | ERROR DETAILS: {result.error_details}" if result.status == ValidationStatus.ERROR and hasattr(result, 'error_details') and result.error_details else "")
            )
        
        console.print(table)
        console.print()
        console.print(f"✅ Passed: {report.passed_checks}")
        console.print(f"❌ Failed: {report.failed_checks}")
        console.print(f"⚠️ Errors: {report.error_checks}")




def _display_query_results_as_table(results):
    """Display query results in a formatted table."""
    if not results:
        console.print("[yellow]No results returned.[/yellow]")
        return
    
    # Create table
    table = Table(show_header=True, header_style="bold magenta")
    
    # Get column names from first row
    if isinstance(results, list) and len(results) > 0:
        if isinstance(results[0], dict):
            # Results are list of dictionaries
            columns = list(results[0].keys())
            for col in columns:
                table.add_column(col)
            
            for row in results:
                table.add_row(*[str(row.get(col, '')) for col in columns])
        else:
            # Results are list of lists/tuples
            columns = [f"Column_{i+1}" for i in range(len(results[0]))]
            for col in columns:
                table.add_column(col)
            
            for row in results:
                table.add_row(*[str(val) for val in row])
    
    console.print(table)


def _display_query_results_as_csv(results):
    """Display query results in CSV format."""
    if not results:
        console.print("No results returned.")
        return
    
    import csv
    import io
    
    output = io.StringIO()
    
    if isinstance(results, list) and len(results) > 0:
        if isinstance(results[0], dict):
            # Results are list of dictionaries
            columns = list(results[0].keys())
            writer = csv.DictWriter(output, fieldnames=columns)
            writer.writeheader()
            writer.writerows(results)
        else:
            # Results are list of lists/tuples
            writer = csv.writer(output)
            writer.writerows(results)
    
    console.print(output.getvalue())


def _generate_fallback_sql(validation_request: str, legacy_table: str, prod_table: str, 
                          date_column: str = None, start_date: str = None, end_date: str = None):
    """Generate basic SQL queries when LLM fails, based on request keywords."""
    
    request_lower = validation_request.lower()
    
    # Build date filter if specified
    date_filter = ""
    if date_column and (start_date or end_date):
        if start_date and end_date:
            date_filter = f" WHERE {date_column} BETWEEN DATE '{start_date}' AND DATE '{end_date}'"
        elif start_date:
            date_filter = f" WHERE {date_column} >= DATE '{start_date}'"
        elif end_date:
            date_filter = f" WHERE {date_column} <= DATE '{end_date}'"
    
    # Generate SQL based on keywords in request
    if any(word in request_lower for word in ['sample', 'preview', 'look', 'show']):
        # Sample data - check this first since 'show' is common
        return {
            'legacy_sql': f"SELECT * FROM {legacy_table}{date_filter} LIMIT 10",
            'prod_sql': f"SELECT * FROM {prod_table}{date_filter} LIMIT 10" if prod_table else f"SELECT * FROM {legacy_table}{date_filter} LIMIT 10",
            'explanation': 'Sample data preview from tables'
        }
    elif any(word in request_lower for word in ['count', 'rows', 'total']):
        # Row count comparison
        return {
            'legacy_sql': f"SELECT COUNT(*) as row_count FROM {legacy_table}{date_filter}",
            'prod_sql': f"SELECT COUNT(*) as row_count FROM {prod_table}{date_filter}" if prod_table else f"SELECT COUNT(*) as row_count FROM {legacy_table}{date_filter}",
            'explanation': 'Row count comparison between tables'
        }
    
    elif any(word in request_lower for word in ['duplicate', 'duplicates']):
        # Duplicate check - simpler approach since DISTINCT * doesn't work well in Athena
        return {
            'legacy_sql': f"SELECT COUNT(*) as total_rows FROM {legacy_table}{date_filter}",
            'prod_sql': f"SELECT COUNT(*) as total_rows FROM {prod_table}{date_filter}" if prod_table else f"SELECT COUNT(*) as total_rows FROM {legacy_table}{date_filter}",
            'explanation': 'Row count for duplicate analysis (fallback - LLM would generate better duplicate detection SQL)'
        }
    
    elif any(word in request_lower for word in ['null', 'nulls', 'missing']):
        # Null value check
        return {
            'legacy_sql': f"SELECT COUNT(*) as total_rows FROM {legacy_table}{date_filter}",
            'prod_sql': f"SELECT COUNT(*) as total_rows FROM {prod_table}{date_filter}" if prod_table else f"SELECT COUNT(*) as total_rows FROM {legacy_table}{date_filter}",
            'explanation': 'Basic row count for null analysis'
        }
    

    
    else:
        # Default: row count
        return {
            'legacy_sql': f"SELECT COUNT(*) as row_count FROM {legacy_table}{date_filter}",
            'prod_sql': f"SELECT COUNT(*) as row_count FROM {prod_table}{date_filter}" if prod_table else f"SELECT COUNT(*) as row_count FROM {legacy_table}{date_filter}",
            'explanation': 'Basic row count comparison'
        }


@cli.command('cache-stats')
def cache_stats():
    """Show SQL cache statistics and performance metrics."""
    try:
        from llm_sql_generator import SQLGenerator
        from sql_cache_manager import SQLCacheManager
        from config import settings
        
        console.print("\n[bold blue]🗄️  SQL Cache Statistics[/bold blue]")
        
        # Initialize cache manager to get stats
        if getattr(settings, 'enable_sql_cache', True):
            cache_manager = SQLCacheManager(
                cache_dir=getattr(settings, 'sql_cache_dir', '.sql_cache'),
                ttl_hours=getattr(settings, 'sql_cache_ttl_hours', 24),
                max_entries=getattr(settings, 'sql_cache_max_entries', 1000)
            )
            
            stats = cache_manager.get_cache_stats()
            
            # Create statistics table
            stats_table = Table(title="Cache Performance")
            stats_table.add_column("Metric", style="cyan")
            stats_table.add_column("Value", style="green")
            
            stats_table.add_row("Cache Entries", str(stats["entries_count"]))
            stats_table.add_row("Max Entries", str(stats["max_entries"]))
            stats_table.add_row("TTL Hours", str(stats["ttl_hours"]))
            stats_table.add_row("Cache Hits", str(stats["cache_hits"]))
            stats_table.add_row("Cache Misses", str(stats["cache_misses"]))
            stats_table.add_row("Hit Rate", f"{stats['hit_rate_percent']}%")
            stats_table.add_row("Total Saves", str(stats["saves"]))
            stats_table.add_row("Evictions", str(stats["evictions"]))
            stats_table.add_row("Cache Size", f"{stats['cache_size_mb']} MB")
            stats_table.add_row("Last Cleanup", stats["last_cleanup"])
            
            console.print(stats_table)
            
            # Show recent cache entries
            recent_entries = cache_manager.list_cache_entries(limit=5)
            if recent_entries:
                console.print("\n[bold cyan]Recent Cache Entries:[/bold cyan]")
                entries_table = Table()
                entries_table.add_column("Validation Request", style="yellow", max_width=60)
                entries_table.add_column("Tables", style="blue")
                entries_table.add_column("Age (hrs)", style="green")
                entries_table.add_column("Access Count", style="magenta")
                
                for entry in recent_entries:
                    entries_table.add_row(
                        entry["validation_request"],
                        entry["tables"],
                        str(entry["age_hours"]),
                        str(entry["access_count"])
                    )
                
                console.print(entries_table)
        else:
            console.print("[yellow]⚠️  SQL caching is disabled in configuration[/yellow]")
            
    except Exception as e:
        console.print(f"[red]❌ Failed to get cache stats: {str(e)}[/red]")


@cli.command('cache-clear')
@click.option('--confirm', '-c', is_flag=True, help='Skip confirmation prompt')
def cache_clear(confirm: bool):
    """Clear all cached SQL queries."""
    try:
        from sql_cache_manager import SQLCacheManager
        from config import settings
        
        if not confirm:
            if not click.confirm("Are you sure you want to clear all cached SQL queries?"):
                console.print("[yellow]Cache clear cancelled.[/yellow]")
                return
        
        if getattr(settings, 'enable_sql_cache', True):
            cache_manager = SQLCacheManager(
                cache_dir=getattr(settings, 'sql_cache_dir', '.sql_cache'),
                ttl_hours=getattr(settings, 'sql_cache_ttl_hours', 24),
                max_entries=getattr(settings, 'sql_cache_max_entries', 1000)
            )
            
            cleared_count = cache_manager.clear_cache()
            console.print(f"[green]✅ Cleared {cleared_count} cache entries[/green]")
        else:
            console.print("[yellow]⚠️  SQL caching is disabled in configuration[/yellow]")
            
    except Exception as e:
        console.print(f"[red]❌ Failed to clear cache: {str(e)}[/red]")


@cli.command('cache-config')
def cache_config():
    """Show current cache configuration."""
    try:
        from config import settings
        
        console.print("\n[bold blue]🗄️  SQL Cache Configuration[/bold blue]")
        
        config_table = Table(title="Cache Settings")
        config_table.add_column("Setting", style="cyan")
        config_table.add_column("Value", style="green")
        config_table.add_column("Description", style="dim")
        
        config_table.add_row(
            "enable_sql_cache",
            str(getattr(settings, 'enable_sql_cache', True)),
            "Enable/disable SQL caching"
        )
        config_table.add_row(
            "sql_cache_dir", 
            getattr(settings, 'sql_cache_dir', '.sql_cache'),
            "Directory for cache files"
        )
        config_table.add_row(
            "sql_cache_ttl_hours",
            str(getattr(settings, 'sql_cache_ttl_hours', 24)),
            "Cache entry time-to-live in hours"
        )
        config_table.add_row(
            "sql_cache_max_entries",
            str(getattr(settings, 'sql_cache_max_entries', 1000)),
            "Maximum number of cache entries"
        )
        
        console.print(config_table)
        
        console.print("\n[dim]💡 These settings can be configured in your .env file[/dim]")
        
    except Exception as e:
        console.print(f"[red]❌ Failed to show cache config: {str(e)}[/red]")


if __name__ == '__main__':
    cli() 