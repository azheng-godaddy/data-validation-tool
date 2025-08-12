"""LLM integration for generating custom SQL validation queries using GoCaaS."""

import requests
import json
from typing import Dict, List, Optional
from config import settings
from sql_cache_manager import SQLCacheManager


class SQLGenerator:
    """Generates SQL queries using GoCaaS (GoDaddy's AI service) for data validation."""
    
    def __init__(self):
        """Initialize the SQL generator with both GoCaaS and GoCode API support."""
        self.model = getattr(settings, 'gocaas_model', 'claude-3-5-sonnet-20241022')
        
        # Support both old GoCaaS and new GoCode systems
        self.gocaas_key_id = getattr(settings, 'gocaas_key_id', '')
        self.gocaas_secret_key = getattr(settings, 'gocaas_secret_key', '')
        self.gocode_api_token = getattr(settings, 'gocode_api_token', '')
        
        # Configuration options
        self.enable_llm_validation = getattr(settings, 'enable_llm_validation', True)  # New feature flag
        self.temperature = getattr(settings, 'gocaas_temperature', 0.1)  # LLM randomness control
        
        # Initialize SQL cache manager
        self.cache_enabled = getattr(settings, 'enable_sql_cache', True)
        if self.cache_enabled:
            self.cache_manager = SQLCacheManager(
                cache_dir=getattr(settings, 'sql_cache_dir', '.sql_cache'),
                ttl_hours=getattr(settings, 'sql_cache_ttl_hours', 24),
                max_entries=getattr(settings, 'sql_cache_max_entries', 1000)
            )
            print(f"🗄️  SQL caching enabled: {self.cache_manager.cache_dir}")
        else:
            self.cache_manager = None
            print("⚠️  SQL caching disabled")
        
        # Determine which authentication method to use
        if self.gocode_api_token:
            self.auth_type = 'gocode'
            # Use the correct GoCode API URL from setup
            self.base_url = getattr(settings, 'gocaas_base_url', 'https://caas-gocode-prod.caas-prod.prod.onkatana.net')
            self.headers = {
                'Authorization': f'Bearer {self.gocode_api_token}',
                'Content-Type': 'application/json'
            }
            print(f"🤖 Using GoCode API for LLM SQL generation: {self.base_url}")
        elif self.gocaas_key_id and self.gocaas_secret_key:
            self.auth_type = 'gocaas'
            self.base_url = 'https://caas-prod.us-west-2.godaddy.com/api/v1'
            self.headers = {
                'x-caas-key-id': self.gocaas_key_id,
                'x-caas-secret-key': self.gocaas_secret_key,
                'Content-Type': 'application/json'
            }
            print("🤖 Using legacy GoCaaS API for LLM SQL generation")
        else:
            self.auth_type = 'none'
            print("❌ No API credentials found for LLM SQL generation")
    
    def _call_gocaas(self, messages: List[Dict], max_tokens: int = 1000) -> str:
        """Make a call to either GoCaaS or GoCode API."""
        
        if self.auth_type == 'gocode':
            # New GoCode API - uses Anthropic format
            return self._call_gocode_api(messages, max_tokens)
        elif self.auth_type == 'gocaas':
            # Old GoCaaS API - uses custom format
            return self._call_gocaas_api(messages, max_tokens)
        else:
            raise Exception("No API credentials configured. Please set either GOCODE_API_TOKEN or GOCAAS_KEY_ID/GOCAAS_SECRET_KEY")
    
    def _call_gocode_api(self, messages: List[Dict], max_tokens: int = 1000) -> str:
        """Call the new GoCode API using OpenAI-compatible format."""
        
        # Build OpenAI-compatible request
        system_content = ""
        user_content = ""
        
        for message in messages:
            if message["role"] == "system":
                system_content = message["content"]
            elif message["role"] == "user":
                user_content = message["content"]
        
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content}
            ],
            "max_tokens": max_tokens,
            "temperature": self.temperature
        }
        
        try:
            # Try the GoCode chat completions endpoint
            endpoint = f"{self.base_url}/chat/completions"
            print(f"🔗 Calling GoCode API: {endpoint}")
            
            response = requests.post(endpoint, headers=self.headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ GoCode API Success!")
                
                # Handle Anthropic response format
                if "content" in result and len(result["content"]) > 0:
                    content_item = result["content"][0]
                    if "text" in content_item:
                        return content_item["text"].strip()
                    elif "content" in content_item:
                        return content_item["content"].strip()
                
                # Fallback for other formats
                if "choices" in result and len(result["choices"]) > 0:
                    return result["choices"][0]["message"]["content"].strip()
                elif "response" in result:
                    return result["response"].strip()
                elif "text" in result:
                    return result["text"].strip()
                else:
                    return str(result).strip()
            else:
                # Check if we got HTML (404 page) instead of JSON
                content_type = response.headers.get('content-type', '')
                if 'text/html' in content_type:
                    print(f"❌ GoCode API returned HTML (404 page) - endpoint may be incorrect")
                    print(f"📍 URL attempted: {endpoint}")
                    raise Exception("GoCode API endpoint not found. Check URL and VPN connection.")
                else:
                    error_msg = f"GoCode API error: {response.status_code} - {response.text[:200]}"
                    print(error_msg)
                    raise Exception(error_msg)
                
        except requests.exceptions.ConnectTimeout:
            raise Exception("GoCode API timeout. Check VPN connection and network.")
        except requests.exceptions.ConnectionError:
            raise Exception("Failed to connect to GoCode API. Check VPN connection and API URL.")
        except Exception as e:
            error_msg = str(e)
            if "404" in error_msg or "endpoint not found" in error_msg.lower():
                raise Exception("GoCode API endpoint not found. Check API URL configuration.")
            elif "timeout" in error_msg.lower():
                raise Exception("GoCode API timeout. Check VPN connection.")
            elif "connection" in error_msg.lower():
                raise Exception("Failed to connect to GoCode API. Check VPN connection and API token.")
            else:
                raise Exception(f"GoCode API error: {error_msg}")
    
    def _call_gocaas_api(self, messages: List[Dict], max_tokens: int = 1000) -> str:
        """Call the old GoCaaS API using custom format."""
        
        # Convert OpenAI-style messages to a single prompt for GoCaaS
        prompt_text = ""
        for message in messages:
            if message["role"] == "system":
                prompt_text += f"System: {message['content']}\n\n"
            elif message["role"] == "user":
                prompt_text += f"User: {message['content']}\n\n"
        
        # GoCaaS request format
        payload = {
            "prompt": prompt_text.strip(),
            "provider": "anthropic",
            "providerOptions": {
                "model": self.model,
                "max_tokens": max_tokens,
                "temperature": self.temperature
            }
        }
        
        # Try different possible endpoints
        endpoints = [
            f"{self.base_url}/prompts",
            f"{self.base_url}/chat/completions",
            f"{self.base_url}/completions", 
            f"{self.base_url}/generate"
        ]
        
        for endpoint in endpoints:
            try:
                print(f"Trying GoCaaS endpoint: {endpoint}")
                response = requests.post(endpoint, headers=self.headers, json=payload, timeout=30)
                
                if response.status_code == 200:
                    result = response.json()
                    print(f"✅ GoCaaS Success! Response: {result}")
                    
                    # Handle GoCaaS response format
                    if "response" in result:
                        return result["response"].strip()
                    elif "content" in result:
                        return result["content"].strip()
                    elif "text" in result:
                        return result["text"].strip()
                    elif "choices" in result and len(result["choices"]) > 0:
                        return result["choices"][0]["message"]["content"].strip()
                    else:
                        return str(result).strip()
                        
                elif response.status_code == 404:
                    print(f"❌ 404 - {endpoint} not found")
                    continue  # Try next endpoint
                else:
                    print(f"❌ GoCaaS API error: {response.status_code} - {response.text}")
                    continue
                    
            except Exception as e:
                print(f"❌ Error calling {endpoint}: {e}")
                continue
        
        raise Exception("Failed to connect to GoCaaS API. Please check your credentials and network access.")
    
    def generate_validation_sql(
        self, 
        legacy_table: str, 
        prod_table: str, 
        validation_request: str,
        table_schema: Optional[Dict] = None,
        date_column: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, str]:
        """Generate SQL queries for custom validation requests.
        
        Args:
            legacy_table: Name of the legacy table
            prod_table: Name of the production table  
            validation_request: Natural language description of validation needed
            table_schema: Optional schema information for better SQL generation
            date_column: Optional date column for filtering
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
            
        Returns:
            Dictionary with 'legacy_sql' and 'prod_sql' keys
        """
        
        # Check cache first if enabled
        if self.cache_enabled and self.cache_manager:
            cached_result = self.cache_manager.get_cached_sql(
                legacy_table=legacy_table,
                prod_table=prod_table,
                validation_request=validation_request,
                date_column=date_column,
                start_date=start_date,
                end_date=end_date,
                table_schema=table_schema
            )
            
            if cached_result:
                print(f"🎯 Using cached SQL for validation request")
                return cached_result
        
        print(f"🤖 Generating new SQL with LLM...")
        
        schema_context = ""
        if table_schema:
            schema_context = f"\nTable Schema Information:\n{self._format_schema(table_schema)}"
        
        # Try to get enhanced schema from GitHub repository
        github_schema_context = self._get_github_schema_context(legacy_table, prod_table, table_schema)
        if github_schema_context:
            schema_context = github_schema_context
        
        # Also extract any additional table names mentioned in the validation request
        additional_schema_context = self._extract_table_schemas_from_prompt(validation_request)
        if additional_schema_context:
            if schema_context:
                schema_context += f"\n\n{additional_schema_context}"
            else:
                schema_context = f"\nAdditional Table Schemas:\n{additional_schema_context}"
        
        date_filter_context = ""
        if date_column and (start_date or end_date):
            date_filter_context = f"\nDate Filtering Required:\n- Column: {date_column}"
            if start_date and end_date:
                date_filter_context += f"\n- Date Range: {start_date} to {end_date}"
            elif start_date:
                date_filter_context += f"\n- From Date: {start_date}"
            elif end_date:
                date_filter_context += f"\n- Until Date: {end_date}"
        
        # Determine if we need a unified query for both tables
        has_two_tables = prod_table and prod_table.strip() and prod_table != legacy_table
        
        # Check if the request indicates comparison/validation across both tables
        comparison_keywords = [
            'compare', 'comparison', 'vs', 'versus', 'against',
            'validate', 'check', 'verify', 'match', 'difference', 'diff',
            'between', 'across', 'both tables', 'consistency', 'discrepancy',
            'primary key', 'uniqueness', 'duplicate', 'integrity',
            'same', 'different', 'equal', 'unequal'
        ]
        
        request_lower = validation_request.lower()
        is_comparison_request = any(keyword in request_lower for keyword in comparison_keywords)
        
        need_unified_query = has_two_tables and is_comparison_request
        
        if need_unified_query:
            prompt = f"""
Generate ONE SIMPLE, COMPLETE Athena SQL query to COMPARE/VALIDATE: {validation_request}

COMPARISON SCENARIO - TWO TABLES:
- Legacy Table: {legacy_table}
- Prod Table: {prod_table}
{schema_context}
{date_filter_context}

REQUIREMENTS:
1. Generate ONE query that compares/validates BOTH tables
2. Use UNION ALL to combine results from both tables
3. Add 'source' column to identify which table each row comes from
4. Keep it SIMPLE - basic SELECT, COUNT, SUM functions only
5. MUST end with semicolon

EXAMPLES for comparison:
✅ Primary Key Check:
SELECT 'legacy' as source, COUNT(*) as total_rows, COUNT(DISTINCT bill_id, bill_line_num) as unique_keys
FROM {legacy_table}
UNION ALL  
SELECT 'prod' as source, COUNT(*) as total_rows, COUNT(DISTINCT bill_id, bill_line_num) as unique_keys
FROM {prod_table};

✅ Row Count Comparison:
SELECT 'legacy' as source, COUNT(*) as row_count FROM {legacy_table}
UNION ALL
SELECT 'prod' as source, COUNT(*) as row_count FROM {prod_table};

JSON Response:
{{
    "legacy_sql": "unified_comparison_query_here;",
    "prod_sql": "",
    "explanation": "compares both tables for [specific validation]"
}}
"""
        else:
            prompt = f"""
Generate SIMPLE, COMPLETE Athena SQL for: {validation_request}

TABLE: {legacy_table}
{schema_context}
{date_filter_context}

REQUIREMENTS:
1. Keep it SIMPLE - basic SELECT statements only
2. Generate ONE complete query in "legacy_sql" 
3. MUST end with semicolon or be naturally complete
4. Use COUNT(*), SUM(), basic aggregations

EXAMPLES:
✅ GOOD: SELECT COUNT(*) FROM {legacy_table};
✅ GOOD: SELECT DISTINCT category FROM {legacy_table};

JSON Response:
{{
    "legacy_sql": "your_complete_simple_query_here;",
    "prod_sql": "",
    "explanation": "what this checks"
}}
"""

        try:
            # Adjust system message based on whether we need to analyze multiple tables
            if need_unified_query:
                system_msg = "You are a simple SQL generator. When given multiple tables, generate ONE query that analyzes BOTH tables using UNION ALL. Keep it simple - basic SELECT statements only. Always end with semicolon. Return valid JSON with single query in legacy_sql."
            else:
                system_msg = "You are a simple SQL generator. Generate BASIC, COMPLETE Athena SQL queries only. Keep it simple - just SELECT, FROM, WHERE, basic functions. Always end with semicolon. Return valid JSON."
            
            response = self._call_gocaas(
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=800  # Slightly higher for dual queries
            )
            
            # Check for schema error first - only fail if NO schema found for ANY table
            if "Schema not found for one or more columns" in response:
                raise Exception("Schema validation failed: No schema information available for any of the requested tables. Please check table names and ensure AWS credentials are valid for Athena fallback.")
            
            # Extract JSON from response
            if response.startswith("```json"):
                response = response[7:-3]
            elif response.startswith("```"):
                response = response[3:-3]
            
            # Use robust JSON cleaning and parsing
            result = self._robust_json_clean_and_parse(response)
            
            # If JSON parsing completely failed, trigger fallback
            if result is None:
                raise json.JSONDecodeError("All JSON parsing strategies failed", response, 0)
            
            # First pass: Basic syntax fixing
            print(f"🔍 Basic SQL syntax validation...")
            legacy_sql = self._validate_and_fix_sql_syntax(result["legacy_sql"])
            prod_sql = self._validate_and_fix_sql_syntax(result.get("prod_sql", ""))
            
            # Second pass: LLM self-validation for the main query
            if legacy_sql and self.enable_llm_validation:
                print(f"🤖 LLM self-validation of generated SQL...")
                validation_result = self._validate_sql_with_llm(legacy_sql)
                
                if not validation_result["is_valid"]:
                    print(f"⚠️  LLM found syntax issues: {', '.join(validation_result['issues'])}")
                    if validation_result["corrected_sql"] and validation_result["corrected_sql"] != legacy_sql:
                        print(f"🔧 Using LLM-corrected SQL...")
                        legacy_sql = validation_result["corrected_sql"]
                    else:
                        print(f"🛡️  LLM couldn't fix issues - will trigger fallback during execution")
                else:
                    print(f"✅ LLM confirmed SQL syntax is valid")
            
            # Log final SQL for debugging
            print(f"📋 Final Legacy SQL: {legacy_sql[:200]}{'...' if len(legacy_sql) > 200 else ''}")
            if prod_sql:
                print(f"📋 Final Prod SQL: {prod_sql[:200]}{'...' if len(prod_sql) > 200 else ''}")
            
            sql_result = {
                "legacy_sql": legacy_sql,
                "prod_sql": prod_sql,
                "explanation": result.get("explanation", "Custom validation query")
            }
            
            # Cache the result if caching is enabled
            if self.cache_enabled and self.cache_manager:
                self.cache_manager.cache_sql_result(
                    legacy_table=legacy_table,
                    prod_table=prod_table,
                    validation_request=validation_request,
                    sql_result=sql_result,
                    date_column=date_column,
                    start_date=start_date,
                    end_date=end_date,
                    table_schema=table_schema
                )
            
            return sql_result
            
        except Exception as e:
            # Use safe fallback SQL when LLM fails or produces invalid syntax
            print(f"⚠️  LLM SQL generation failed: {str(e)}")
            print("🔄 Using safe fallback SQL generation...")
            
            # Try safe fallback first
            try:
                return self._create_safe_fallback_sql(legacy_table, prod_table, validation_request)
            except Exception:
                pass
            
            # Enhanced fallback with more useful analysis when LLM fails
            if len(legacy_table.split('.')) >= 2:
                # Single table analysis for data quality checks
                return {
                    "legacy_sql": f"""
WITH data_quality AS (
  SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT *) AS unique_rows,
    COUNT(*) - COUNT(DISTINCT *) AS duplicate_rows
  FROM {legacy_table}
)
SELECT 
  'TOTAL_ROWS' AS metric,
  CAST(total_rows AS VARCHAR) AS value
FROM data_quality
UNION ALL
SELECT 
  'UNIQUE_ROWS' AS metric,
  CAST(unique_rows AS VARCHAR) AS value  
FROM data_quality
UNION ALL
SELECT 
  'DUPLICATE_ROWS' AS metric,
  CAST(duplicate_rows AS VARCHAR) AS value
FROM data_quality
""",
                    "prod_sql": "",
                    "explanation": f"Fallback data quality analysis (LLM JSON parsing error: {str(e)})"
                }
            else:
                # Simple fallback for malformed table names
                return {
                    "legacy_sql": f"SELECT COUNT(*) AS row_count FROM {legacy_table}",
                    "prod_sql": "",
                    "explanation": f"Fallback count query (LLM error: {str(e)})"
                }
    
    def _fix_unterminated_strings(self, response: str) -> str:
        """Fix common unterminated string issues in LLM JSON responses."""
        import re
        
        # First, try to fix the most common issue: unterminated JSON string values
        # Look for patterns where a JSON value is not properly terminated
        
        # Fix unterminated JSON strings by finding unmatched quotes
        lines = response.split('\n')
        fixed_lines = []
        
        for line in lines:
            # Check if line has unterminated string (odd number of unescaped quotes)
            # Count quotes but ignore escaped ones
            quote_count = 0
            escaped = False
            for char in line:
                if char == '\\' and not escaped:
                    escaped = True
                    continue
                elif char == '"' and not escaped:
                    quote_count += 1
                escaped = False
            
            # If odd number of quotes, add closing quote at end
            if quote_count % 2 != 0:
                # Find the last quote and add closing quote before any trailing characters
                if line.rstrip().endswith(','):
                    line = line.rstrip()[:-1] + '",'
                else:
                    line = line.rstrip() + '"'
            
            fixed_lines.append(line)
        
        fixed_response = '\n'.join(fixed_lines)
        
        # Additional safety: ensure JSON structure is complete
        if not fixed_response.strip().endswith('}'):
            fixed_response = fixed_response.rstrip() + '\n}'
        
        return fixed_response
    
    def _robust_json_clean_and_parse(self, response: str) -> Dict[str, str]:
        """More robust JSON cleaning and parsing with multiple fallback strategies."""
        import re
        import json
        
        # Strategy 1: Basic cleaning and parsing
        try:
            cleaned = self._fix_unterminated_strings(response)
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass
        
        # Strategy 2: More aggressive JSON cleaning
        try:
            # Remove all content before first { and after last }
            start_idx = response.find('{')
            end_idx = response.rfind('}')
            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                json_content = response[start_idx:end_idx + 1]
                
                # Fix common JSON issues
                fixes = [
                    # Fix unescaped newlines in strings
                    (r'(?<!\\)\\n(?!["\\/bfnrtu])', r'\\n'),
                    # Fix trailing commas
                    (r',(\s*[}\]])', r'\1'),
                    # Fix missing quotes around property names - more aggressive
                    (r'(\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":'),
                    # Fix single quotes to double quotes - more comprehensive
                    (r"'([^'\\]*(?:\\.[^'\\]*)*)'", r'"\1"'),
                    # Fix unescaped quotes inside strings
                    (r'(?<!\\)"([^"]*[^\\])"([^,}\]\s])', r'"\1\\""\2'),
                ]
                
                for pattern, replacement in fixes:
                    json_content = re.sub(pattern, replacement, json_content)
                
                return json.loads(json_content)
        except (json.JSONDecodeError, ValueError):
            pass
        
        # Strategy 3: Extract SQL queries using regex if JSON parsing completely fails
        try:
            legacy_sql_match = re.search(r'"legacy_sql"\s*:\s*"([^"]+(?:\\.[^"]*)*)"', response, re.DOTALL)
            prod_sql_match = re.search(r'"prod_sql"\s*:\s*"([^"]+(?:\\.[^"]*)*)"', response, re.DOTALL)
            explanation_match = re.search(r'"explanation"\s*:\s*"([^"]+(?:\\.[^"]*)*)"', response, re.DOTALL)
            
            if legacy_sql_match:
                legacy_sql = legacy_sql_match.group(1).replace('\\"', '"').replace('\\n', '\n')
                prod_sql = prod_sql_match.group(1).replace('\\"', '"').replace('\\n', '\n') if prod_sql_match else ""
                explanation = explanation_match.group(1).replace('\\"', '"') if explanation_match else "Extracted from malformed JSON"
                
                return {
                    "legacy_sql": legacy_sql,
                    "prod_sql": prod_sql,
                    "explanation": explanation
                }
        except Exception:
            pass
        
        # Strategy 4: Last resort - return None to trigger fallback
        return None
    
    def _validate_and_fix_sql_syntax(self, sql: str) -> str:
        """Validate and fix common SQL syntax errors, especially with NULLIF and similar functions."""
        import re
        
        if not sql or sql.strip() == "":
            return sql
            
        # Store original for debugging
        original_sql = sql
        
        # Remove extra whitespace and normalize
        sql = re.sub(r'\s+', ' ', sql.strip())
        
        # Enhanced quote and string literal fixes (PRIORITY: Fix before other patterns)
        quote_fixes = [
            # Fix quad quotes and higher (common LLM error)
            (r"'{4,}", r"'"),
            # Fix triple quotes (common LLM error)
            (r"'''", r"'"),
            # Fix double single quotes
            (r"''(?!')", r"'"),
            # Fix any sequence of 2+ quotes that aren't proper SQL escapes
            (r"'{2,}", r"'"),
            # Fix unterminated strings at line/statement end
            (r"'([^']*?)(?=\s*(?:FROM|WHERE|GROUP|ORDER|UNION|LIMIT|;|$))", r"'\1'"),
            # Fix escaped quotes that got mangled
            (r"\\'", r"''"),
            # Fix quotes around non-string values that should be unquoted
            (r"'(\d+)'(?=\s*[,)])", r"\1"),
            # Fix string concatenation issues
            (r"'\s*\+\s*'", r""),
            # Fix quotes in column aliases
            (r"\s+AS\s+'([^']+)'", r" AS \1"),
            # Clean up any remaining quote clusters
            (r"'+", r"'"),
        ]
        
        fixed_sql = sql
        for pattern, replacement in quote_fixes:
            new_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE)
            if new_sql != fixed_sql:
                print(f"🔧 Quote Fix Applied: {pattern} -> {replacement}")
                fixed_sql = new_sql
        
        # Enhanced NULLIF detection and fixing
        nullif_fixes = [
            # Fix standalone NULLIF without arguments (add space)
            (r'\bNULLIF\s*(?!\s*\()', r'COALESCE '),
            # Fix NULLIF with only one argument
            (r'\bNULLIF\s*\(\s*([^,)]+)\s*\)', r'COALESCE(\1, NULL)'),
            # Fix NULLIF used in inappropriate contexts (like WHERE clause without proper syntax)
            (r'\bWHERE\s+NULLIF\b', r'WHERE COALESCE'),
            # Fix NULLIF in SELECT without proper parentheses (add space)
            (r'\bSELECT\s+NULLIF\s+([^,\s(]+)', r'SELECT COALESCE(\1, NULL)'),
            # Fix NULLIF used as column name or alias
            (r'\bAS\s+NULLIF\b', r'AS nullif_result'),
            # Fix NULLIF in expressions without proper syntax
            (r'\bNULLIF\s*([^(])', r'COALESCE\1'),
            # Fix incomplete NULLIF expressions
            (r'\bNULLIF\s*\(\s*([^,)]+)\s*,\s*\)', r'COALESCE(\1, NULL)'),
            # Fix NULLIF with missing second argument
            (r'\bNULLIF\s*\(\s*([^,)]+)\s*,\s*([^,)]*)\s*$', r'COALESCE(\1, \2)'),
        ]
        
        # Apply NULLIF fixes and track changes
        for pattern, replacement in nullif_fixes:
            new_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE)
            if new_sql != fixed_sql:
                print(f"🔧 NULLIF Fix Applied: {pattern} -> {replacement}")
                fixed_sql = new_sql
        
        # Check if NULLIF still exists after fixes
        if 'NULLIF' in fixed_sql.upper():
            print(f"⚠️  Warning: NULLIF still present in SQL after fixes")
            print(f"📝 SQL snippet: ...{fixed_sql[max(0, fixed_sql.upper().find('NULLIF')-20):fixed_sql.upper().find('NULLIF')+30]}...")
            
            # More aggressive replacement as last resort
            fixed_sql = re.sub(r'\bNULLIF\b', 'COALESCE', fixed_sql, flags=re.IGNORECASE)
            print(f"🛡️  Applied aggressive NULLIF -> COALESCE replacement")
        
        # Fix other common Athena syntax issues
        common_fixes = [
            # Fix missing FROM before UNION
            (r'\bUNION\s+ALL\s+SELECT', r'UNION ALL\nSELECT'),
            # Fix lowercase null to uppercase NULL (Athena requirement)
            (r'\bnull\b', r'NULL'),
            # Fix null used as column name without AS
            (r'\bSELECT\s+NULL\s+FROM\b', r'SELECT NULL FROM'),
            # Fix incomplete statements (missing closing parentheses)
            (r'\(\s*SELECT[^)]*$', lambda m: m.group(0) + ')'),
            # Fix trailing commas that cause EOF errors
            (r',\s*(?=FROM|WHERE|GROUP|ORDER|UNION|LIMIT|;|$)', r''),
            # Fix incomplete CASE statements
            (r'\bCASE\s+[^E]*(?<!END)$', lambda m: m.group(0) + ' END'),
            # Fix double semicolons
            (r';;+', r';'),
            # Ensure proper spacing around operators
            (r'([<>=!]+)', r' \1 '),
            # Fix multiple spaces
            (r'\s+', ' '),
            # Fix common SQL syntax issues
            (r'\bGROUP\s+BY\s+,', r'GROUP BY 1,'),
            (r'\bORDER\s+BY\s+,', r'ORDER BY 1,'),
        ]
        
        for pattern, replacement in common_fixes:
            fixed_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE)
        
        # Final validation: Check for syntax issues
        
        # Check for incomplete SQL (common with token limits)
        sql_trimmed = fixed_sql.strip()
        if sql_trimmed:
            last_word = sql_trimmed.split()[-1] if sql_trimmed.split() else ""
            
            # Only fix the most obvious incomplete cases
            if last_word.upper() in ['WHERE', 'AND', 'OR']:
                print(f"⚠️  SQL incomplete - ends with '{last_word}', adding default condition")
                fixed_sql += " 1=1"
                print(f"🔧 Added: 1=1")
            
            # Ensure query ends with semicolon for completeness
            if not sql_trimmed.endswith(';') and not last_word.upper() in [')', 'END']:
                fixed_sql += ";"
                print(f"🔧 Added semicolon for completeness")
        
        # Check for parentheses balancing
        open_parens = fixed_sql.count('(')
        close_parens = fixed_sql.count(')')
        if open_parens > close_parens:
            missing_parens = open_parens - close_parens
            fixed_sql += ')' * missing_parens
            print(f"🔧 Added {missing_parens} missing closing parentheses")
        elif close_parens > open_parens:
            print(f"⚠️  Warning: {close_parens - open_parens} extra closing parentheses detected")
        
        # Check for quote issues
        single_quote_count = fixed_sql.count("'")
        
        # Check for consecutive quotes that might cause issues
        import re
        if re.search(r"'{2,}", fixed_sql):
            print(f"⚠️  Warning: Multiple consecutive quotes detected")
            # Replace any remaining consecutive quotes with single quote
            fixed_sql = re.sub(r"'{2,}", "'", fixed_sql)
            print(f"🔧 Cleaned up consecutive quotes")
        
        # Check for unmatched quotes after cleanup
        single_quote_count = fixed_sql.count("'")
        if single_quote_count % 2 != 0:
            print(f"⚠️  Warning: Unmatched single quotes detected (count: {single_quote_count})")
            # Try to balance quotes by adding one at the end
            fixed_sql += "'"
            print(f"🔧 Added closing quote to balance string literals")
        
        # Debug output if significant changes were made
        if fixed_sql != original_sql:
            print(f"🔍 SQL Syntax Fixes Applied:")
            print(f"   Original length: {len(original_sql)} chars")
            print(f"   Fixed length: {len(fixed_sql)} chars")
            if len(original_sql) > 3300:  # Show area around position 3351
                print(f"   Around pos 3351: ...{original_sql[3340:3370]}...")
            
        return fixed_sql.strip()
    
    def _validate_sql_with_llm(self, sql: str) -> Dict[str, any]:
        """Have the LLM validate its own generated SQL for Athena syntax issues."""
        
        if not sql or sql.strip() == "":
            return {"is_valid": False, "issues": ["Empty SQL"], "corrected_sql": ""}
        
        validation_prompt = f"""
You are an AWS Athena SQL syntax checker. Analyze this SQL for syntax errors:

SQL TO CHECK:
{sql}

Check for these common Athena syntax issues:
1. NULLIF usage (must have exactly 2 arguments)
2. NULL keyword (must be uppercase NULL, not lowercase null)
3. Quote balancing (every ' must have a closing ')
4. Parentheses balancing (every ( must have a closing ))
5. Incomplete statements (EOF errors - missing ), END, or proper termination)
6. Unfinished WHERE/JOIN/ON clauses (ending with keywords like WHERE, AND, OR)
7. Trailing commas before FROM/WHERE/GROUP/ORDER clauses
8. Truncated queries due to token limits
9. LIMIT placement (only at query end, never in WITH clauses)
10. UNION structure (FROM before UNION ALL)
11. String literal termination
12. Multiple consecutive quotes (''', '''', etc.)
13. Keywords must be properly cased for Athena

Return JSON:
{{
    "is_valid": true/false,
    "issues": ["list of specific issues found"],
    "corrected_sql": "fixed SQL if issues found, or original if valid"
}}

If no issues found, return is_valid: true with empty issues array.
If issues found, provide corrected_sql with fixes applied.
"""

        try:
            response = self._call_gocaas(
                messages=[
                    {"role": "system", "content": "You are an AWS Athena SQL syntax validator. Return only valid JSON."},
                    {"role": "user", "content": validation_prompt}
                ],
                max_tokens=600
            )
            
            # Clean and parse JSON response
            if response.startswith("```json"):
                response = response[7:-3]
            elif response.startswith("```"):
                response = response[3:-3]
            
            result = self._robust_json_clean_and_parse(response)
            
            if result and "is_valid" in result:
                return {
                    "is_valid": result.get("is_valid", False),
                    "issues": result.get("issues", []),
                    "corrected_sql": result.get("corrected_sql", sql)
                }
            else:
                # Fallback if JSON parsing fails
                return {"is_valid": False, "issues": ["LLM validation failed"], "corrected_sql": sql}
                
        except Exception as e:
            print(f"⚠️  LLM syntax validation failed: {str(e)}")
            return {"is_valid": False, "issues": [f"Validation error: {str(e)}"], "corrected_sql": sql}
    
    def _create_safe_fallback_sql(self, legacy_table: str, prod_table: str, validation_request: str) -> Dict[str, str]:
        """Create a SIMPLE, SAFE fallback SQL query when LLM fails."""
        
        # Always use the simplest possible query
        return {
            "legacy_sql": f"SELECT COUNT(*) AS row_count FROM {legacy_table};",
            "prod_sql": "",
            "explanation": f"Simple fallback query to count rows in {legacy_table}"
        }
    
    def generate_custom_rule_sql(
        self,
        legacy_table: str,
        prod_table: str,
        rule_description: str,
        columns: Optional[List[str]] = None
    ) -> Dict[str, str]:
        """Generate SQL for a custom validation rule.
        
        Args:
            legacy_table: Legacy table name
            prod_table: Production table name
            rule_description: Description of the validation rule
            columns: Optional list of specific columns to focus on
            
        Returns:
            Dictionary with SQL queries and metadata
        """
        
        columns_context = ""
        if columns:
            columns_context = f"\nFocus on these columns: {', '.join(columns)}"
        
        prompt = f"""
Generate SQL queries for a custom data validation rule.

Tables:
- Legacy: {legacy_table}
- Production: {prod_table}

Rule: {rule_description}
{columns_context}

Create queries that will help validate this rule. The queries should return data that can be easily compared to determine if the rule passes or fails.

Use AWS Athena/Presto SQL syntax. Return JSON format:
{{
    "legacy_sql": "SELECT ...",
    "prod_sql": "SELECT ...",
    "validation_logic": "How to interpret the results",
    "pass_criteria": "What indicates the validation passed"
}}
"""

        try:
            response = self._call_gocaas(
                messages=[
                    {"role": "system", "content": "You are a data validation expert."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1200
            )
            
            # Clean and parse JSON
            if response.startswith("```json"):
                response = response[7:-3]
            elif response.startswith("```"):
                response = response[3:-3]
            
            return json.loads(response)
            
        except Exception as e:
            return {
                "legacy_sql": f"SELECT COUNT(*) as count FROM {legacy_table}",
                "prod_sql": f"SELECT COUNT(*) as count FROM {prod_table}",
                "validation_logic": f"Compare counts (LLM error: {str(e)})",
                "pass_criteria": "Counts should be equal"
            }
    
    def explain_validation_results(
        self,
        validation_results: List,
        legacy_table: str,
        prod_table: str
    ) -> str:
        """Generate a human-readable explanation of validation results.
        
        Args:
            validation_results: List of ValidationResult objects
            legacy_table: Legacy table name
            prod_table: Production table name
            
        Returns:
            Human-readable summary of validation results
        """
        
        results_summary = []
        for result in validation_results:
            # Access ValidationResult attributes, not dictionary keys
            results_summary.append(f"- {result.rule_name}: {result.status.value} - {result.message}")
        
        prompt = f"""
Analyze these data validation results between legacy table '{legacy_table}' and production table '{prod_table}':

{chr(10).join(results_summary)}

Provide a concise summary that:
1. Highlights the overall validation status
2. Identifies critical issues that need attention
3. Suggests next steps if there are failures
4. Mentions any data quality concerns

Keep the response professional and actionable.
"""

        try:
            response = self._call_gocaas(
                messages=[
                    {"role": "system", "content": "You are a data analyst providing validation insights."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=800
            )
            
            return response.strip()
            
        except Exception as e:
            # Fallback summary
            total_validations = len(validation_results)
            passed = sum(1 for r in validation_results if r.status.value == 'PASS')
            failed = total_validations - passed
            
            return f"Validation Summary: {passed}/{total_validations} checks passed. {failed} failures need attention."
    
    def _format_schema(self, schema: Dict) -> str:
        """Format schema information for LLM context."""
        if not schema:
            return "No schema information available"
        
        formatted = []
        for table, columns in schema.items():
            formatted.append(f"{table}:")
            for col in columns:
                formatted.append(f"  - {col['name']} ({col['type']})")
        
        return "\n".join(formatted)
    
    def _get_github_schema_context(self, legacy_table: str, prod_table: str, fallback_schema: Optional[Dict] = None) -> Optional[str]:
        """Get enhanced schema context from GitHub repository using table names as knowledge base."""
        try:
            # Only try GitHub if enabled in settings
            if not settings.enable_github_schema:
                return None
            
            from github_schema_fetcher import GitHubSchemaFetcher
            
            # Initialize GitHub fetcher
            fetcher = GitHubSchemaFetcher(
                repo_owner=settings.github_repo_owner,
                repo_name=settings.github_repo_name,
                github_token=settings.github_token,
                branch=settings.github_branch
            )
            
            enhanced_context = []
            
            # Extract just table names (remove database prefixes if present)
            legacy_table_name = legacy_table.split('.')[-1] if '.' in legacy_table else legacy_table
            prod_table_name = prod_table.split('.')[-1] if '.' in prod_table else prod_table
            
            # Get enhanced schema for legacy table (try both with and without database prefix)
            legacy_ddl = fetcher.search_table_ddl(legacy_table) or fetcher.search_table_ddl(legacy_table_name)
            if legacy_ddl:
                legacy_schema_text = self._format_github_ddl_for_llm(legacy_table, legacy_ddl)
                enhanced_context.append(f"Legacy Table ({legacy_table}):\n{legacy_schema_text}")
            
            # Get enhanced schema for production table
            prod_ddl = fetcher.search_table_ddl(prod_table) or fetcher.search_table_ddl(prod_table_name)
            if prod_ddl:
                prod_schema_text = self._format_github_ddl_for_llm(prod_table, prod_ddl)
                enhanced_context.append(f"Production Table ({prod_table}):\n{prod_schema_text}")
            
            # If we found any DDL, return enhanced context
            if enhanced_context:
                return "\n\n".join(enhanced_context)
            
            return None
            
        except ImportError:
            # github_schema_fetcher not available
            return None
        except Exception as e:
            # Log error but don't fail validation
            print(f"Warning: GitHub schema fetch failed: {str(e)}")
            return None
    
    def _format_github_ddl_for_llm(self, table_name: str, ddl_data: dict) -> str:
        """Format GitHub DDL data for LLM context."""
        schema_info = ddl_data.get('schema_info', {})
        columns = schema_info.get('columns', [])
        
        if not columns:
            return f"{table_name}: No column information available"
        
        formatted_lines = [f"{table_name} (from GitHub DDL):"]
        
        for col in columns:
            col_line = f"  - {col['name']} ({col['type']})"
            if col.get('comment'):
                col_line += f" -- {col['comment']}"
            formatted_lines.append(col_line)
        
        # Add DDL context snippet
        raw_ddl = ddl_data.get('ddl_content', '')
        if raw_ddl:
            ddl_snippet = raw_ddl[:300] + "..." if len(raw_ddl) > 300 else raw_ddl
            formatted_lines.append(f"\nDDL Context:\n{ddl_snippet}")
        
        return "\n".join(formatted_lines)
    
    def _extract_table_schemas_from_prompt(self, validation_request: str) -> str:
        """Extract table names from prompt and fetch their DDL from GitHub."""
        try:
            if not settings.enable_github_schema:
                return ""
            
            from github_schema_fetcher import GitHubSchemaFetcher
            import re
            
            # Initialize GitHub fetcher
            fetcher = GitHubSchemaFetcher(
                repo_owner=settings.github_repo_owner,
                repo_name=settings.github_repo_name,
                github_token=settings.github_token,
                branch=settings.github_branch
            )
            
            # Common table name patterns to look for in the prompt
            table_patterns = [
                # Match database.table format
                r'\b(\w+\.\w+)\b',
                # Match standalone table names (common table prefixes)
                r'\b(fact_\w+)\b',
                r'\b(dim_\w+)\b', 
                r'\b(\w+_fact)\b',
                r'\b(\w+_dim)\b',
                r'\b(\w+_mart)\b',
                r'\b(\w+_staging)\b',
                r'\b(\w+_raw)\b',
                # Match quoted table names
                r'[\'"`](\w+)[\'"`]',
                r'[\'"`](\w+\.\w+)[\'"`]'
            ]
            
            found_tables = set()
            
            # Extract potential table names from the prompt
            for pattern in table_patterns:
                matches = re.findall(pattern, validation_request, re.IGNORECASE)
                found_tables.update(matches)
            
            # Filter out common words that aren't table names
            common_words = {'table', 'tables', 'from', 'where', 'select', 'count', 'data', 'row', 'rows', 'column', 'columns'}
            found_tables = {t for t in found_tables if t.lower() not in common_words and len(t) > 3}
            
            if not found_tables:
                return ""
            
            schema_contexts = []
            
            # Fetch DDL for each found table
            for table_name in found_tables:
                ddl_result = fetcher.search_table_ddl(table_name)
                if ddl_result:
                    formatted_schema = self._format_github_ddl_for_llm(table_name, ddl_result)
                    schema_contexts.append(formatted_schema)
            
            if schema_contexts:
                return "\n\n".join(schema_contexts)
            
            return ""
            
        except Exception as e:
            print(f"Warning: Table extraction from prompt failed: {str(e)}")
            return "" 