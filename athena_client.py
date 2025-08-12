"""
AWS Athena Client for executing SQL queries programmatically.
"""

import boto3
import time
from typing import List, Dict, Any, Optional
from config import settings


class AthenaClient:
    """Client for executing queries against AWS Athena."""
    
    def __init__(self):
        """Initialize the Athena client."""
        print("🔧 Using Direct Athena API Execution")
        
        # Initialize AWS clients
        self.athena_client = boto3.client('athena', region_name=settings.aws_region)
        self.s3_client = boto3.client('s3', region_name=settings.aws_region)
        self.glue_client = boto3.client('glue', region_name=settings.aws_region)
        
        # Configuration
        self.output_location = settings.athena_output_location
        
        if self.output_location and self.output_location.strip():
            # If user provided a bucket without a folder prefix, add a safe default prefix
            try:
                bucket, prefix = self._parse_s3_url(self.output_location)
                if prefix == "":
                    default_prefix = "athena-results/"
                    adjusted = f"s3://{bucket}/{default_prefix}"
                    print(f"[info] Output S3 URL had no folder prefix. Using '{adjusted}' instead.")
                    self.output_location = adjusted
            except Exception:
                # If not a valid S3 URL, keep as-is; Athena will error out with a clearer message later
                pass
            
            print(f"📁 Output Location: {self.output_location}")
            # Preflight check: validate ability to write to output location
            try:
                self._validate_output_location()
            except Exception as e:
                print("❌ S3 output location validation failed:", str(e))
                print("💡 Ensure your AWS identity has s3:PutObject, s3:GetObject, and s3:ListBucket on the bucket/prefix.")
                print("💡 Also ensure the bucket is in region:", settings.aws_region)
                print("↩️  Falling back to Athena workgroup's default output location.")
                # Clear configured output so Athena uses workgroup default
                self.output_location = ""
        else:
            print("📁 Output Location: Using Athena default")
        print("📋 Running queries with database context")
    
    def _parse_s3_url(self, s3_url: str) -> (str, str):
        """Parse s3://bucket/prefix URL into bucket and prefix."""
        if not s3_url.startswith("s3://"):
            raise ValueError(f"Invalid S3 URL: {s3_url}")
        path = s3_url[len("s3://"):]
        parts = path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        # Ensure prefix ends with slash for folder semantics
        if prefix and not prefix.endswith("/"):
            prefix = prefix + "/"
        return bucket, prefix
    
    def _validate_output_location(self) -> None:
        """Validate we can write to the configured S3 output location."""
        bucket, prefix = self._parse_s3_url(self.output_location)
        
        # Optional: verify bucket region matches config
        try:
            hdr = self.s3_client.head_bucket(Bucket=bucket)
        except Exception as e:
            raise RuntimeError(f"Cannot access bucket '{bucket}': {e}")
        
        # Try to write and delete a tiny temp object under the prefix
        key = f"{prefix}athena-preflight/{int(time.time())}.txt" if prefix else f"athena-preflight/{int(time.time())}.txt"
        try:
            self.s3_client.put_object(Bucket=bucket, Key=key, Body=b"athena preflight test")
            # Cleanup
            self.s3_client.delete_object(Bucket=bucket, Key=key)
        except Exception as e:
            raise RuntimeError(
                "Access denied when writing to configured S3 output location. "
                f"Bucket='{bucket}', Prefix='{prefix}'. Original error: {e}"
            )
    
    def execute_query(self, sql: str, timeout: int = 300) -> List[Dict[str, Any]]:
        """Execute a single SQL query and return results."""
        try:
            print(f"🔄 Executing query via Athena API...")
            
            # Build query parameters
            query_params = {
                'QueryString': sql,
                'QueryExecutionContext': {
                    'Database': 'default'  # Provide default database context
                }
            }
            
            # Only add ResultConfiguration if we have an output location
            if self.output_location and self.output_location.strip():
                query_params['ResultConfiguration'] = {
                    'OutputLocation': self.output_location
                }
            
            response = self.athena_client.start_query_execution(**query_params)
            
            query_execution_id = response['QueryExecutionId']
            print(f"📋 Query ID: {query_execution_id}")
            
            # Wait for completion
            start_time = time.time()
            while True:
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Query timed out after {timeout} seconds")
                
                status_response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = status_response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED']:
                    print("✅ Query completed successfully")
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    raise Exception(f"Query failed: {error}")
                else:
                    print(f"⏳ Query status: {status}")
                    time.sleep(1)  # Faster polling for better responsiveness
            
            # Get results
            results_response = self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            # Parse results
            rows = []
            if 'Rows' in results_response['ResultSet'] and len(results_response['ResultSet']['Rows']) > 1:
                for row_data in results_response['ResultSet']['Rows'][1:]:  # Skip header
                    row = {}
                    for i, col_data in enumerate(row_data['Data']):
                        col_name = results_response['ResultSet']['ResultSetMetadata']['ColumnInfo'][i]['Name']
                        # Convert numeric strings to appropriate types
                        value = col_data.get('VarCharValue', '')
                        if value and value.isdigit():
                            value = int(value)
                        elif value and self._is_float(value):
                            value = float(value)
                        row[col_name] = value
                    rows.append(row)
            
            print(f"📊 Retrieved {len(rows)} rows")
            return rows
            
        except Exception as e:
            print(f"❌ Query execution failed: {e}")
            return []
    
    def execute_parallel_queries(self, queries: List[str]) -> List[List[Dict[str, Any]]]:
        """Execute multiple queries in parallel."""
        import concurrent.futures
        import threading
        
        results = [None] * len(queries)  # Pre-allocate results list
        
        print(f"🔄 Executing {len(queries)} queries in parallel via Athena API...")
        
        def execute_single_query(index, query):
            """Execute a single query and store result at the given index."""
            try:
                print(f"📝 Starting query {index+1}/{len(queries)}...")
                result = self._execute_query_internal(query)
                results[index] = result
                print(f"✅ Query {index+1} completed successfully")
            except Exception as e:
                print(f"❌ Query {index+1} execution failed: {str(e)}")
                results[index] = []
        
        # Execute queries in parallel using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(queries), 5)) as executor:
            # Submit all queries
            futures = [
                executor.submit(execute_single_query, i, query) 
                for i, query in enumerate(queries)
            ]
            
            # Wait for all to complete
            concurrent.futures.wait(futures)
        
        print(f"🏁 All {len(queries)} queries completed")
        return results
    
    def _execute_query_internal(self, sql: str, timeout: int = 300) -> List[Dict[str, Any]]:
        """Internal method for executing a single query without extra logging."""
        # Build query parameters
        query_params = {
            'QueryString': sql,
            'QueryExecutionContext': {
                'Database': 'default'  # Provide default database context
            }
        }
        
        # Only add ResultConfiguration if we have an output location
        if self.output_location and self.output_location.strip():
            query_params['ResultConfiguration'] = {
                'OutputLocation': self.output_location
            }
        
        response = self.athena_client.start_query_execution(**query_params)
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for completion
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Query timed out after {timeout} seconds")
            
            status_response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = status_response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED']:
                break
            elif status in ['FAILED', 'CANCELLED']:
                error = status_response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                
                # Add specific handling for common SQL syntax errors
                if 'NULLIF' in error and 'mismatched input' in error:
                    raise Exception(f"❌ SQL Syntax Error: Invalid NULLIF usage. {error}\n" +
                                  "💡 Tip: NULLIF requires exactly 2 arguments: NULLIF(expr1, expr2)")
                elif 'mismatched input' in error:
                    raise Exception(f"❌ SQL Syntax Error: {error}\n" +
                                  "💡 Tip: Check for missing parentheses, commas, or incorrect function usage")
                else:
                    raise Exception(f"Query failed: {error}")
            else:
                time.sleep(1)  # Shorter sleep for faster response
        
        # Get results
        results_response = self.athena_client.get_query_results(QueryExecutionId=query_execution_id)
        
        # Parse results
        rows = []
        if 'Rows' in results_response['ResultSet'] and len(results_response['ResultSet']['Rows']) > 1:
            for row_data in results_response['ResultSet']['Rows'][1:]:  # Skip header
                row = {}
                for i, col_data in enumerate(row_data['Data']):
                    col_name = results_response['ResultSet']['ResultSetMetadata']['ColumnInfo'][i]['Name']
                    # Convert numeric strings to appropriate types
                    value = col_data.get('VarCharValue', '')
                    if value and value.isdigit():
                        value = int(value)
                    elif value and self._is_float(value):
                        value = float(value)
                    row[col_name] = value
                rows.append(row)
        
        return rows
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get table schema information from Glue Catalog."""
        try:
            # Split database and table
            if '.' in table_name:
                database, table = table_name.split('.', 1)
            else:
                database = 'default'
                table = table_name
            
            # Get table information from Glue
            response = self.glue_client.get_table(
                DatabaseName=database,
                Name=table
            )
            
            # Extract column information
            columns = []
            storage_descriptor = response['Table']['StorageDescriptor']
            
            for column in storage_descriptor['Columns']:
                columns.append({
                    'name': column['Name'],
                    'type': column['Type'],
                    'comment': column.get('Comment', '')
                })
            
            # Add partition columns if they exist
            if 'PartitionKeys' in response['Table']:
                for partition in response['Table']['PartitionKeys']:
                    columns.append({
                        'name': partition['Name'],
                        'type': partition['Type'],
                        'comment': partition.get('Comment', '') + ' (partition key)'
                    })
            
            return columns
            
        except Exception as e:
            print(f"❌ Failed to get schema for {table_name}: {e}")
            return []
    
    def test_table_access(self, table_name: str) -> Dict[str, Any]:
        """Test access to a specific table."""
        try:
            print(f"🔍 Testing access to table: {table_name}")
            
            # Test with a simple query
            test_query = f"SELECT COUNT(*) as row_count FROM {table_name} LIMIT 1"
            result = self.execute_query(test_query)
            
            return {
                'status': 'SUCCESS',
                'table': table_name,
                'accessible': True,
                'message': f'Table accessible - contains data'
            }
            
        except Exception as e:
            return {
                'status': 'ERROR',
                'table': table_name,
                'accessible': False,
                'error': str(e)
            }
    
    def test_connection(self) -> Dict[str, Any]:
        """Test connection to Athena."""
        try:
            # Simple test query
            test_query = "SELECT 1 as test_value"
            result = self.execute_query(test_query)
            
            return {
                'status': 'SUCCESS',
                'output_location': self.output_location,
                'message': 'Athena connection successful'
            }
            
        except Exception as e:
            return {
                'status': 'ERROR',
                'error': str(e)
            }
    
    def _is_float(self, value: str) -> bool:
        """Check if a string represents a float."""
        try:
            float(value)
            return '.' in value
        except ValueError:
            return False 