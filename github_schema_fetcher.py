import requests
import os
import re
from typing import Dict, Optional, List, Any
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)

class GitHubSchemaFetcher:
    """
    Fetches table DDL and schema information from the GitHub lake repository.
    
    Integrates with the existing LLM SQL generation to provide accurate schema context
    from the authoritative DDL source.
    """
    
    def __init__(self, repo_owner: str = "gdcorp-dna", repo_name: str = "lake", 
                 github_token: Optional[str] = None, branch: str = "main"):
        """
        Initialize GitHub Schema Fetcher.
        
        Args:
            repo_owner: GitHub repository owner (default: gdcorp-dna)
            repo_name: Repository name (default: lake)
            github_token: Optional GitHub token for private repos or rate limiting
            branch: Branch to fetch from (default: main)
        """
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.branch = branch
        self.github_token = github_token
        self.base_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"
        
        # Cache for DDL content to avoid repeated API calls
        self._ddl_cache = {}
        
        # Headers for GitHub API
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "DataValidationTool/1.0"
        }
        
        if github_token:
            self.headers["Authorization"] = f"token {github_token}"
    
    def search_table_ddl(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Search for DDL file containing the specified table definition.
        
        Args:
            table_name: Table name in format 'database.table' or just 'table'
            
        Returns:
            Dictionary with DDL content and metadata, or None if not found
        """
        try:
            # Parse table name
            if '.' in table_name:
                database, table = table_name.split('.', 1)
            else:
                database = None
                table = table_name
            
            # Search for DDL files
            ddl_content = self._search_ddl_files(database, table)
            
            if ddl_content:
                # Parse DDL to extract schema information
                schema_info = self._parse_ddl_content(ddl_content, table_name)
                return {
                    "table_name": table_name,
                    "ddl_content": ddl_content,
                    "schema_info": schema_info,
                    "source": "github_lake_repo"
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error searching DDL for {table_name}: {str(e)}")
            return None
    
    def _search_ddl_files(self, database: Optional[str], table: str) -> Optional[str]:
        """
        Search for DDL files in the catalog/config/prod directory.
        
        Specific patterns for GoDaddy lake repository:
        - catalog/config/prod/{database}/{table}.sql
        - catalog/config/prod/{database}/{table}.ddl
        - catalog/config/prod/{table}.sql
        - catalog/config/prod/{table}.ddl
        """
        search_patterns = []
        base_path = "catalog/config/prod"
        
        if database:
            # Search in database-specific subdirectories
            search_patterns.extend([
                f"{base_path}/{database}/{table}.sql",
                f"{base_path}/{database}/{table}.ddl",
                f"{base_path}/{database}/{table}_ddl.sql",
                f"{base_path}/{database}/{table}_schema.sql",
                # Try with underscores in case table names use them
                f"{base_path}/{database.replace('.', '_')}/{table.replace('.', '_')}.sql",
                f"{base_path}/{database.replace('.', '_')}/{table.replace('.', '_')}.ddl",
            ])
        
        # Search in root catalog/config/prod directory
        search_patterns.extend([
            f"{base_path}/{table}.sql",
            f"{base_path}/{table}.ddl",
            f"{base_path}/{table}_ddl.sql",
            f"{base_path}/{table}_schema.sql",
            # Try with underscores
            f"{base_path}/{table.replace('.', '_')}.sql",
            f"{base_path}/{table.replace('.', '_')}.ddl",
        ])
        
        # Try direct file access first
        for pattern in search_patterns:
            content = self._fetch_file_content(pattern)
            if content:
                return content
        
        # If direct access fails, search through catalog/config/prod directory
        return self._search_repository_content(table, base_path)
    
    def _fetch_file_content(self, file_path: str) -> Optional[str]:
        """Fetch content of a specific file from GitHub."""
        if file_path in self._ddl_cache:
            return self._ddl_cache[file_path]
        
        try:
            url = f"{self.base_url}/contents/{file_path}?ref={self.branch}"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                file_data = response.json()
                if file_data.get('type') == 'file':
                    # Decode base64 content
                    import base64
                    content = base64.b64decode(file_data['content']).decode('utf-8')
                    self._ddl_cache[file_path] = content
                    return content
            
            return None
            
        except Exception as e:
            logger.debug(f"Could not fetch {file_path}: {str(e)}")
            return None
    
    def _search_repository_content(self, table: str, base_path: str = "catalog/config/prod") -> Optional[str]:
        """Search through specific directory for files containing the table definition."""
        try:
            # Search for files using GitHub search API within specific path
            search_query = f"CREATE TABLE {table} repo:{self.repo_owner}/{self.repo_name} path:{base_path}"
            search_url = f"https://api.github.com/search/code?q={search_query}"
            
            response = requests.get(search_url, headers=self.headers, timeout=15)
            
            if response.status_code == 200:
                search_results = response.json()
                
                for item in search_results.get('items', []):
                    # Check if file is in our target directory and is a SQL/DDL file
                    if (item['path'].startswith(base_path) and 
                        (item['name'].endswith('.sql') or item['name'].endswith('.ddl'))):
                        # Fetch the file content
                        content = self._fetch_file_content(item['path'])
                        if content and self._contains_table_definition(content, table):
                            return content
            
            # If no results with CREATE TABLE, try a broader search within the directory
            search_query_alt = f"{table} repo:{self.repo_owner}/{self.repo_name} path:{base_path} extension:sql"
            search_url_alt = f"https://api.github.com/search/code?q={search_query_alt}"
            
            response_alt = requests.get(search_url_alt, headers=self.headers, timeout=15)
            
            if response_alt.status_code == 200:
                search_results_alt = response_alt.json()
                
                for item in search_results_alt.get('items', []):
                    if item['path'].startswith(base_path):
                        content = self._fetch_file_content(item['path'])
                        if content and self._contains_table_definition(content, table):
                            return content
            
            return None
            
        except Exception as e:
            logger.error(f"Repository search failed: {str(e)}")
            return None
    
    def _contains_table_definition(self, content: str, table: str) -> bool:
        """Check if DDL content contains the table definition."""
        # Handle both full table names (database.table) and just table names
        table_name_only = table.split('.')[-1] if '.' in table else table
        
        # Look for CREATE TABLE statements with various formats
        patterns = [
            # Standard CREATE TABLE patterns
            rf"CREATE\s+TABLE\s+(?:\w+\.)?{re.escape(table)}\s*\(",
            rf"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?{re.escape(table)}\s*\(",
            # Just table name without database prefix
            rf"CREATE\s+TABLE\s+(?:\w+\.)?{re.escape(table_name_only)}\s*\(",
            rf"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?{re.escape(table_name_only)}\s*\(",
            # With backticks (common in some DDL formats)
            rf"CREATE\s+TABLE\s+`?(?:\w+\.)?{re.escape(table_name_only)}`?\s*\(",
            rf"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(?:\w+\.)?{re.escape(table_name_only)}`?\s*\(",
            # Table name in comments or metadata (sometimes DDL files have table references)
            rf"(?:--\s*)?(?:Table|TABLE):\s*{re.escape(table)}",
            rf"(?:--\s*)?(?:Table|TABLE):\s*{re.escape(table_name_only)}",
        ]
        
        for pattern in patterns:
            if re.search(pattern, content, re.IGNORECASE | re.MULTILINE):
                return True
        
        return False
    
    def _parse_ddl_content(self, ddl_content: str, table_name: str) -> Dict[str, Any]:
        """
        Parse DDL content to extract structured schema information.
        
        Returns schema in format compatible with existing LLM formatting.
        """
        try:
            columns = []
            table_name_only = table_name.split('.')[-1] if '.' in table_name else table_name
            
            # Multiple patterns to extract CREATE TABLE statement
            table_patterns = [
                # Standard patterns
                rf"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?{re.escape(table_name_only)}\s*\((.*?)\)",
                rf"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(?:\w+\.)?{re.escape(table_name_only)}`?\s*\((.*?)\)",
                # With full table name
                rf"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?{re.escape(table_name)}\s*\((.*?)\)",
            ]
            
            columns_section = None
            for pattern in table_patterns:
                match = re.search(pattern, ddl_content, re.IGNORECASE | re.DOTALL)
                if match:
                    columns_section = match.group(1)
                    break
            
            if columns_section:
                # Parse individual columns with improved pattern matching
                # Handle various column definition formats
                column_patterns = [
                    # Standard: column_name data_type [constraints] [COMMENT 'comment']
                    r'`?(\w+)`?\s+(\w+(?:\([^)]+\))?(?:\s+\w+)*)\s*(?:NOT\s+NULL|NULL)?\s*(?:DEFAULT\s+[^,\)]+?)?\s*(?:COMMENT\s+[\'"]([^\'"]*)[\'"])?\s*[,\)]',
                    # Simpler pattern: column_name data_type
                    r'`?(\w+)`?\s+(\w+(?:\([^)]+\))?)\s*[,\)]',
                    # With backticks and complex types
                    r'`([^`]+)`\s+([^,\)]+?)(?:\s+COMMENT\s+[\'"]([^\'"]*)[\'"])?\s*[,\)]'
                ]
                
                for pattern in column_patterns:
                    for col_match in re.finditer(pattern, columns_section, re.IGNORECASE):
                        col_name = col_match.group(1).strip()
                        col_type = col_match.group(2).strip()
                        col_comment = col_match.group(3).strip() if len(col_match.groups()) >= 3 and col_match.group(3) else ""
                        
                        # Clean up the type (remove extra keywords)
                        col_type = re.sub(r'\s+(NOT\s+)?NULL.*$', '', col_type).strip()
                        col_type = re.sub(r'\s+DEFAULT.*$', '', col_type).strip()
                        
                        # Avoid duplicates
                        if not any(c['name'].lower() == col_name.lower() for c in columns):
                            columns.append({
                                "name": col_name,
                                "type": col_type,
                                "comment": col_comment
                            })
                    
                    # If we found columns, break
                    if columns:
                        break
            
            # If no columns found yet, try to extract from other patterns in the file
            if not columns:
                # Look for column definitions in comments or other sections
                comment_patterns = [
                    rf"--\s*Columns?\s*:?\s*\n((?:--.*\n)*)",
                    rf"\/\*\s*Columns?\s*:?\s*(.*?)\*\/",
                ]
                
                for pattern in comment_patterns:
                    match = re.search(pattern, ddl_content, re.IGNORECASE | re.DOTALL)
                    if match:
                        comment_section = match.group(1)
                        # Extract column info from comments
                        for line in comment_section.split('\n'):
                            line = line.strip().lstrip('--').lstrip('*').strip()
                            if re.match(r'\w+\s+\w+', line):
                                parts = line.split()
                                if len(parts) >= 2:
                                    columns.append({
                                        "name": parts[0],
                                        "type": parts[1],
                                        "comment": " ".join(parts[2:]) if len(parts) > 2 else ""
                                    })
            
            return {
                "columns": columns,
                "ddl_source": "github_lake_repo",
                "raw_ddl": ddl_content[:1000] + "..." if len(ddl_content) > 1000 else ddl_content
            }
            
        except Exception as e:
            logger.error(f"Error parsing DDL for {table_name}: {str(e)}")
            return {"columns": [], "error": str(e)}
    
    def get_enhanced_schema_context(self, table_name: str, fallback_schema: Optional[List[Dict]] = None) -> str:
        """
        Get enhanced schema context combining GitHub DDL with fallback schema.
        
        Args:
            table_name: Table name to get schema for
            fallback_schema: Fallback schema from Athena/Glue if GitHub lookup fails
            
        Returns:
            Formatted schema context for LLM
        """
        github_ddl = self.search_table_ddl(table_name)
        
        if github_ddl and github_ddl.get("schema_info", {}).get("columns"):
            # Use GitHub DDL schema
            columns = github_ddl["schema_info"]["columns"]
            
            schema_text = f"{table_name} (from GitHub DDL):\n"
            for col in columns:
                comment_text = f" -- {col['comment']}" if col.get('comment') else ""
                schema_text += f"  - {col['name']} ({col['type']}){comment_text}\n"
            
            # Add raw DDL snippet for better context
            ddl_snippet = github_ddl.get("ddl_content", "")[:500]
            if len(ddl_snippet) == 500:
                ddl_snippet += "..."
            
            schema_text += f"\nDDL Context:\n{ddl_snippet}\n"
            
            return schema_text
        
        elif fallback_schema:
            # Use fallback Athena/Glue schema
            schema_text = f"{table_name} (from Athena/Glue):\n"
            for col in fallback_schema:
                col_name = col.get('column_name', col.get('Name', ''))
                col_type = col.get('data_type', col.get('Type', ''))
                schema_text += f"  - {col_name} ({col_type})\n"
            
            return schema_text
        
        else:
            return f"{table_name}: No schema information available"


def get_github_token() -> Optional[str]:
    """Get GitHub token from environment variables."""
    return os.getenv('GITHUB_TOKEN') or os.getenv('GH_TOKEN') 