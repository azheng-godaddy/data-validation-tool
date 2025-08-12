"""Setup script to help configure AWS connection for data validation tool."""

import boto3
import click
import os
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from config import settings, validate_config

console = Console()


@click.group()
def setup():
    """Setup and test AWS connection for data validation tool."""
    pass


@setup.command()
def check_credentials():
    """Check what AWS credentials are currently configured."""
    console.print("[bold blue]🔍 Checking AWS Credential Setup[/bold blue]")
    
    credential_sources = []
    
    # Check for SSO/temporary credentials first
    if os.getenv('AWS_SESSION_TOKEN'):
        credential_sources.append(("✅ SSO/Temporary credentials", "Environment (AWS_SESSION_TOKEN)", "Active SSO session"))
    
    # Check .env file
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            content = f.read()
            if 'AWS_ACCESS_KEY_ID' in content:
                credential_sources.append(("✅ .env file", "Project directory", "High priority"))
    
    # Check environment variables
    if os.getenv('AWS_ACCESS_KEY_ID'):
        credential_sources.append(("✅ Environment variables", "Shell environment", "Medium priority"))
    
    # Check AWS credentials file
    aws_creds_file = os.path.expanduser('~/.aws/credentials')
    if os.path.exists(aws_creds_file):
        credential_sources.append(("✅ AWS credentials file", "~/.aws/credentials", "Low priority"))
    
    # Check AWS config file
    aws_config_file = os.path.expanduser('~/.aws/config')
    if os.path.exists(aws_config_file):
        credential_sources.append(("✅ AWS config file", "~/.aws/config", "For region/output"))
    
    if credential_sources:
        table = Table(title="AWS Credential Sources Found")
        table.add_column("Source", style="cyan")
        table.add_column("Location", style="yellow")
        table.add_column("Priority/Purpose", style="green")
        
        for source, location, priority in credential_sources:
            table.add_row(source, location, priority)
        
        console.print(table)
        
        console.print("\n[bold yellow]🔍 How AWS credentials are loaded (in order):[/bold yellow]")
        console.print("1. Environment variables (including SSO tokens)")
        console.print("2. .env file (if using this data validation tool)")
        console.print("3. ~/.aws/credentials file")
        console.print("4. IAM roles (if running on EC2)")
        
        # Check if this looks like an SSO session
        if os.getenv('AWS_SESSION_TOKEN'):
            console.print("\n[bold green]🎉 SSO Session Detected![/bold green]")
            console.print("You're using temporary credentials - this is more secure than permanent keys!")
            
    else:
        console.print("[red]❌ No AWS credentials found![/red]")
        console.print("\n[yellow]You need to set up credentials using one of these methods:[/yellow]")
        console.print("1. [cyan]SSO authentication[/cyan] (most secure)")
        console.print("2. [cyan].env file[/cyan] (good for development)")
        console.print("3. [cyan]~/.aws/credentials[/cyan] (standard AWS approach)")
        console.print("4. [cyan]Environment variables[/cyan]")
    
    # Test what's actually being used
    console.print("\n[bold blue]🧪 Testing actual credential access...[/bold blue]")
    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        
        if credentials:
            # Don't show the actual keys for security
            console.print(f"[green]✅ Using Access Key: ***{credentials.access_key[-4:]}[/green]")
            console.print(f"[green]✅ Region: {session.region_name or 'default'}[/green]")
            
            # Check if these are temporary credentials
            if hasattr(credentials, 'token') and credentials.token:
                console.print(f"[cyan]🔐 Using temporary credentials (SSO/STS)[/cyan]")
                console.print(f"[cyan]📅 Session token present - expires automatically[/cyan]")
            else:
                console.print(f"[yellow]🔑 Using permanent credentials[/yellow]")
        else:
            console.print("[red]❌ No valid credentials found[/red]")
    
    except Exception as e:
        console.print(f"[red]❌ Credential test failed: {str(e)}[/red]")


@setup.command()
def sso_setup():
    """Guide for setting up with GoDaddy SSO."""
    console.print("[bold blue]🔐 GoDaddy SSO Setup Guide[/bold blue]")
    
    console.print(Panel.fit(
        """[bold cyan]GoDaddy SSO Workflow[/bold cyan]

1. Set up your environment:
   [yellow]python3 -m venv dex[/yellow]
   [yellow]source dex/bin/activate[/yellow]
   [yellow]pipenv install gd-lake --skip-lock[/yellow]
   [yellow]pip install aws-okta-processor[/yellow]

2. Authenticate with GoDaddy Okta:
   [yellow]eval $(aws-okta-processor authenticate -e -o godaddy.okta.com -u <your-userid>)[/yellow]

3. Get SSO token:
   [yellow]python3 sso_token.py[/yellow]

4. Test our data validation tool:
   [yellow]python setup_aws.py check-credentials[/yellow]""",
        title="SSO Steps"
    ))
    
    console.print("\n[bold green]✅ Benefits of SSO approach:[/bold green]")
    console.print("• 🔐 More secure (temporary credentials)")
    console.print("• ⏰ Auto-expires (can't be leaked long-term)")
    console.print("• 🏢 Follows company security policies")
    console.print("• 🔄 Centralized access management")
    
    console.print("\n[bold yellow]⚠️ Things to remember with SSO:[/bold yellow]")
    console.print("• 🕐 Tokens expire (usually 1-8 hours)")
    console.print("• 🔄 Need to re-authenticate periodically")
    console.print("• 🚫 Don't use .env file for AWS keys (use SSO instead)")
    
    console.print("\n[bold blue]🎯 For data validation tool:[/bold blue]")
    console.print("1. Complete your SSO authentication first")
    console.print("2. Only set non-AWS settings in .env:")
    
    env_example = """# Only set these in .env for SSO users
ATHENA_DATABASE=your_database_name
ATHENA_WORKGROUP=primary  
ATHENA_OUTPUT_LOCATION=s3://your-bucket/
OPENAI_API_KEY=sk-your_key
OPENAI_MODEL=gpt-4

# DON'T set these (use SSO instead):
# AWS_ACCESS_KEY_ID=
# AWS_SECRET_ACCESS_KEY=
# AWS_REGION= (from SSO)"""
    
    console.print(f"\n[cyan]{env_example}[/cyan]")



@setup.command()
def list_tables():
    """List available tables in your data catalog."""
    console.print("[bold blue]📋 Listing Available Tables[/bold blue]")
    
    using_sso = os.getenv('AWS_SESSION_TOKEN') is not None
    
    if not using_sso and not validate_config():
        console.print("[red]❌ Configuration validation failed.[/red]")
        return
    
    try:
        if using_sso:
            glue_client = boto3.client('glue', region_name='us-west-2')
        else:
            glue_client = boto3.client(
                'glue',
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name='us-west-2'
            )
        
        # Get database name
        if hasattr(settings, 'athena_database') and settings.athena_database:
            database_name = settings.athena_database
        else:
            console.print("[yellow]⚠️ No database specified in config. Listing all databases:[/yellow]")
            databases = glue_client.get_databases()
            for db in databases['DatabaseList']:
                console.print(f"   🗄️ {db['Name']}")
            return
        
        # Get tables from the configured database
        try:
            tables = glue_client.get_tables(DatabaseName=database_name)
            
            console.print(f"\n[cyan]Tables in database '{database_name}':[/cyan]")
            for table in tables['TableList']:
                table_name = f"{database_name}.{table['Name']}"
                console.print(f"   📊 {table_name}")
                
                # Show some column info
                if 'StorageDescriptor' in table and 'Columns' in table['StorageDescriptor']:
                    columns = table['StorageDescriptor']['Columns'][:3]  # First 3 columns
                    col_info = ", ".join([f"{col['Name']} ({col['Type']})" for col in columns])
                    console.print(f"      Columns: {col_info}{'...' if len(table['StorageDescriptor']['Columns']) > 3 else ''}")
        
        except Exception as e:
            console.print(f"[red]❌ Could not list tables in database '{database_name}': {str(e)}[/red]")
            
            # List all databases instead
            databases = glue_client.get_databases()
            console.print(f"\n[yellow]Available databases:[/yellow]")
            for db in databases['DatabaseList']:
                console.print(f"   🗄️ {db['Name']}")
    
    except Exception as e:
        console.print(f"[red]❌ Failed to connect to Glue: {str(e)}[/red]")


@setup.command()
def create_example_env():
    """Create an example .env file."""
    console.print("[bold blue]📝 Creating Example .env File[/bold blue]")
    
    env_content = """# AWS Configuration - Replace with your actual values
AWS_ACCESS_KEY_ID=AKIA...your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_REGION=us-east-1
ATHENA_DATABASE=your_database_name
ATHENA_WORKGROUP=primary
ATHENA_OUTPUT_LOCATION=s3://your-athena-results-bucket/

# Jupyter Notebook Configuration (optional)
JUPYTER_OUTPUT_FORMAT=styled
JUPYTER_SHOW_PLOTS=true

# OpenAI Configuration
OPENAI_API_KEY=sk-...your_openai_key
OPENAI_MODEL=gpt-4
"""
    
    with open('.env.example', 'w') as f:
        f.write(env_content)
    
    console.print("[green]✅ Created .env.example file[/green]")
    console.print("\n[yellow]Next steps:[/yellow]")
    console.print("1. Copy .env.example to .env: [cyan]cp .env.example .env[/cyan]")
    console.print("2. Edit .env with your actual AWS credentials")
    console.print("3. Run: [cyan]python setup_aws.py check-credentials[/cyan]")


@setup.command()
def create_sso_env():
    """Create .env file for SSO users (no AWS credentials needed)."""
    console.print("[bold blue]📝 Creating .env File for SSO Users[/bold blue]")
    
    env_content = """# Configuration for SSO users
# Don't include AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY - use SSO instead!

ATHENA_DATABASE=your_database_name
ATHENA_WORKGROUP=primary
ATHENA_OUTPUT_LOCATION=s3://your-bucket-name/

# Jupyter Notebook Configuration (optional)
JUPYTER_OUTPUT_FORMAT=styled
JUPYTER_SHOW_PLOTS=true

# OpenAI Configuration
OPENAI_API_KEY=sk-...your_openai_key
OPENAI_MODEL=gpt-4
"""
    
    with open('.env', 'w') as f:
        f.write(env_content)
    
    console.print("[green]✅ Created .env file for SSO users[/green]")
    console.print("\n[yellow]Next steps:[/yellow]")
    console.print("1. Edit .env with your actual database/bucket names")
    console.print("2. Complete SSO authentication in your terminal")
    console.print("3. Run: [cyan]python setup_aws.py check-credentials[/cyan]")


@setup.command()
def create_aws_profile():
    """Help create standard AWS credentials file."""
    console.print("[bold blue]📝 Creating AWS Credentials File[/bold blue]")
    
    # Check if ~/.aws directory exists
    aws_dir = os.path.expanduser('~/.aws')
    if not os.path.exists(aws_dir):
        os.makedirs(aws_dir)
        console.print(f"[green]✅ Created directory: {aws_dir}[/green]")
    
    # Get credentials from user
    access_key = console.input("Enter your AWS Access Key ID: ")
    secret_key = console.input("Enter your AWS Secret Access Key: ")
    region = console.input("Enter your AWS region (default: us-east-1): ") or "us-east-1"
    
    # Write credentials file
    creds_file = os.path.join(aws_dir, 'credentials')
    with open(creds_file, 'w') as f:
        f.write(f"""[default]
aws_access_key_id = {access_key}
aws_secret_access_key = {secret_key}
""")
    
    # Write config file
    config_file = os.path.join(aws_dir, 'config')
    with open(config_file, 'w') as f:
        f.write(f"""[default]
region = {region}
output = json
""")
    
    console.print(f"[green]✅ Created AWS credentials file: {creds_file}[/green]")
    console.print(f"[green]✅ Created AWS config file: {config_file}[/green]")
    
    console.print("\n[yellow]💡 Note:[/yellow] This tool will still use .env file if it exists.")
    console.print("To use AWS credentials file instead, remove AWS settings from .env")


@setup.command()
def quick_test():
    """Quick test with sample query."""
    console.print("[bold blue]⚡ Quick Test Query[/bold blue]")
    
    using_sso = os.getenv('AWS_SESSION_TOKEN') is not None
    
    if not using_sso and not validate_config():
        console.print("[red]❌ Configuration validation failed.[/red]")
        return
    
    # Get table name from user
    table_name = console.input("Enter a table name to test (e.g., database.table_name): ")
    
    if not table_name:
        console.print("[yellow]No table name provided.[/yellow]")
        return
    
    try:
        from athena_client import AthenaClient
        
        client = AthenaClient()
        
        console.print(f"🔍 Testing table access: {table_name}")
        
        # Test simple query
        sql = f"SELECT COUNT(*) as row_count FROM {table_name} LIMIT 1"
        
        with console.status("[bold green]Running test query..."):
            result = client.execute_query(sql)
        
        if result:
            row_count = result[0]['row_count']
            console.print(f"[green]✅ Query successful![/green]")
            console.print(f"   Table: {table_name}")
            console.print(f"   Row count: {row_count:,}")
            
            if using_sso:
                console.print(f"[cyan]🔐 Using SSO credentials[/cyan]")
        else:
            console.print(f"[yellow]⚠️ Query returned no results[/yellow]")
    
    except Exception as e:
        console.print(f"[red]❌ Query failed: {str(e)}[/red]")
        console.print("\n[yellow]Common issues:[/yellow]")
        if using_sso:
            console.print("   • SSO session may have expired")
            console.print("   • Re-authenticate with your SSO flow")
        else:
            console.print("   • Table name might be incorrect")
            console.print("   • Check database.table_name format")
            console.print("   • Verify table exists in AWS Glue catalog")


if __name__ == '__main__':
    setup() 