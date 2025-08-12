"""Configuration management for the data validation tool."""

import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings."""
    
    # AWS Configuration
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: str = "us-west-2"  # Default to us-west-2 for GoDaddy
    
    # Athena Configuration (Direct API Access, No Workgroup)
    athena_output_location: str = "s3://aws-athena-query-results-255575434142-us-west-2/"
    
    # Iceberg support (basic level via standard Athena)
    iceberg_catalog: str = Field(default="awsdatacatalog", description="Iceberg catalog name")
    iceberg_warehouse: Optional[str] = Field(default=None, description="Iceberg warehouse location")
    
    # AI Service Configuration (supports both old GoCaaS and new GoCode)
    # Old GoCaaS credentials
    gocaas_key_id: Optional[str] = None
    gocaas_secret_key: Optional[str] = None
    # New GoCode credentials  
    gocode_api_token: Optional[str] = None
    # Shared settings
    gocaas_model: str = "claude-3-5-sonnet-20241022"
    gocaas_base_url: str = "https://caas.api.godaddy.com/v1"
    gocaas_temperature: float = 0.1  # Control LLM randomness (0.0 = deterministic, 1.0 = very random)
    
    # GitHub Schema Repository Configuration
    github_token: Optional[str] = None
    github_repo_owner: str = "gdcorp-dna"
    github_repo_name: str = "lake"
    github_branch: str = "main"
    enable_github_schema: bool = True
    
    # Jupyter Notebook Configuration
    jupyter_output_format: str = "styled"  # 'styled', 'plain', 'html'
    jupyter_show_plots: bool = True
    
    # SQL Cache Configuration
    enable_sql_cache: bool = Field(default=True, description="Enable caching of LLM-generated SQL queries")
    sql_cache_dir: str = Field(default=".sql_cache", description="Directory to store SQL cache files")
    sql_cache_ttl_hours: int = Field(default=24, description="Time-to-live for cached SQL queries in hours")
    sql_cache_max_entries: int = Field(default=1000, description="Maximum number of cache entries to store")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"  # Ignore extra fields from .env file

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Override with environment variables if available
        if os.getenv('AWS_REGION'):
            self.aws_region = os.getenv('AWS_REGION')
        elif os.getenv('AWS_DEFAULT_REGION'):
            self.aws_region = os.getenv('AWS_DEFAULT_REGION')


# Global settings instance
settings = Settings()


def validate_config() -> bool:
    """Validate that required configuration is present."""
    import boto3
    import os
    
    # Check if AWS credentials are available (SSO or otherwise)
    try:
        # Try to get caller identity - this works with SSO
        sts = boto3.client('sts', region_name=settings.aws_region)
        identity = sts.get_caller_identity()
        print(f"🔐 SSO session detected - Account: {identity['Account']}")
        return True
    except Exception:
        # No valid AWS credentials found
        print("❌ No valid AWS credentials found")
        print("💡 Please authenticate with SSO:")
        print("   eval $(aws-okta-processor authenticate -e -o godaddy.okta.com -u azheng)")
        return False 