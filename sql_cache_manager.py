"""
SQL Cache Manager for LLM-generated queries.

Provides persistent caching of LLM-generated SQL queries to avoid redundant API calls
and improve performance. Features include:
- Persistent file-based storage
- Cache expiration (TTL)
- Cache key hashing based on validation parameters
- Cache statistics and management
- Thread-safe operations
"""

import os
import json
import hashlib
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from threading import Lock
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class CachedSQLEntry:
    """Represents a cached SQL query entry."""
    cache_key: str
    legacy_sql: str
    prod_sql: str
    explanation: str
    legacy_table: str
    prod_table: str
    validation_request: str
    date_column: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    created_at: float  # Unix timestamp
    last_accessed: float  # Unix timestamp
    access_count: int = 1
    
    def is_expired(self, ttl_hours: int) -> bool:
        """Check if cache entry has expired."""
        return time.time() - self.created_at > (ttl_hours * 3600)
    
    def touch(self):
        """Update last accessed time and increment access count."""
        self.last_accessed = time.time()
        self.access_count += 1


class SQLCacheManager:
    """
    Manages persistent caching of LLM-generated SQL queries.
    
    Uses file-based storage with JSON format for persistence across sessions.
    Implements LRU-style eviction and TTL-based expiration.
    """
    
    def __init__(self, cache_dir: str = ".sql_cache", ttl_hours: int = 24, max_entries: int = 1000):
        """
        Initialize SQL cache manager.
        
        Args:
            cache_dir: Directory to store cache files
            ttl_hours: Time-to-live for cache entries in hours
            max_entries: Maximum number of cache entries to store
        """
        self.cache_dir = Path(cache_dir)
        self.ttl_hours = ttl_hours
        self.max_entries = max_entries
        
        # Thread safety for concurrent access
        self._lock = Lock()
        
        # In-memory cache for fast access
        self._memory_cache: Dict[str, CachedSQLEntry] = {}
        
        # Cache metadata
        self.cache_file = self.cache_dir / "sql_cache.json"
        self.stats_file = self.cache_dir / "cache_stats.json"
        
        # Cache statistics (must be initialized before any cleanup uses it)
        self.stats = {
            "hits": 0,
            "misses": 0,
            "saves": 0,
            "evictions": 0,
            "last_cleanup": time.time()
        }
        # Load persisted stats if present
        self._load_stats()

        # Initialize cache directory and load existing cache
        self._initialize_cache()
    
    def _initialize_cache(self):
        """Initialize cache directory and load existing cache entries."""
        try:
            # Create cache directory if it doesn't exist
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Load existing cache from disk
            if self.cache_file.exists():
                self._load_cache_from_disk()
            
            # Perform initial cleanup
            self._cleanup_expired_entries()
            
            logger.info(f"SQL Cache initialized: {len(self._memory_cache)} entries loaded")
            
        except Exception as e:
            logger.error(f"Failed to initialize SQL cache: {e}")
            self._memory_cache = {}
    
    def _load_cache_from_disk(self):
        """Load cache entries from disk storage."""
        try:
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
            
            for entry_data in cache_data.get('entries', []):
                entry = CachedSQLEntry(**entry_data)
                self._memory_cache[entry.cache_key] = entry
                
        except Exception as e:
            logger.error(f"Failed to load cache from disk: {e}")
            self._memory_cache = {}
    
    def _save_cache_to_disk(self):
        """Save cache entries to disk storage."""
        try:
            cache_data = {
                'version': '1.0',
                'last_updated': time.time(),
                'entries': [asdict(entry) for entry in self._memory_cache.values()]
            }
            
            # Atomic write using temporary file
            temp_file = self.cache_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            # Replace original file
            temp_file.replace(self.cache_file)
            
        except Exception as e:
            logger.error(f"Failed to save cache to disk: {e}")
    
    def _load_stats(self):
        """Load cache statistics from disk."""
        try:
            if self.stats_file.exists():
                with open(self.stats_file, 'r') as f:
                    saved_stats = json.load(f)
                    self.stats.update(saved_stats)
        except Exception as e:
            logger.debug(f"Could not load cache stats: {e}")
    
    def _save_stats(self):
        """Save cache statistics to disk."""
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(self.stats, f, indent=2)
        except Exception as e:
            logger.debug(f"Could not save cache stats: {e}")
    
    def _generate_cache_key(
        self,
        legacy_table: str,
        prod_table: str,
        validation_request: str,
        date_column: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        table_schema: Optional[Dict] = None
    ) -> str:
        """
        Generate a unique cache key for the SQL query parameters.
        
        Uses SHA-256 hash of normalized parameters to ensure consistent keys.
        """
        # Normalize parameters for consistent hashing
        params = {
            'legacy_table': legacy_table.lower().strip(),
            'prod_table': prod_table.lower().strip(),
            'validation_request': validation_request.lower().strip(),
            'date_column': date_column.lower().strip() if date_column else None,
            'start_date': start_date.strip() if start_date else None,
            'end_date': end_date.strip() if end_date else None,
        }
        
        # Include schema info in key if available
        if table_schema:
            # Create a simplified schema signature
            schema_signature = []
            for table, columns in table_schema.items():
                if isinstance(columns, list):
                    col_names = sorted([col.get('column_name', col.get('Name', '')) for col in columns])
                    schema_signature.append(f"{table}:{','.join(col_names)}")
            params['schema_signature'] = '|'.join(sorted(schema_signature))
        
        # Create hash from parameters
        param_string = json.dumps(params, sort_keys=True)
        return hashlib.sha256(param_string.encode()).hexdigest()
    
    def get_cached_sql(
        self,
        legacy_table: str,
        prod_table: str,
        validation_request: str,
        date_column: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        table_schema: Optional[Dict] = None
    ) -> Optional[Dict[str, str]]:
        """
        Retrieve cached SQL query if available and not expired.
        
        Returns:
            Dictionary with legacy_sql, prod_sql, and explanation if found, None otherwise
        """
        with self._lock:
            cache_key = self._generate_cache_key(
                legacy_table, prod_table, validation_request,
                date_column, start_date, end_date, table_schema
            )
            
            entry = self._memory_cache.get(cache_key)
            
            if entry is None:
                self.stats["misses"] += 1
                self._save_stats()
                return None
            
            # Check if entry has expired
            if entry.is_expired(self.ttl_hours):
                logger.debug(f"Cache entry expired: {cache_key[:12]}...")
                del self._memory_cache[cache_key]
                self.stats["misses"] += 1
                self.stats["evictions"] += 1
                self._save_stats()
                return None
            
            # Update access statistics
            entry.touch()
            self.stats["hits"] += 1
            self._save_stats()
            
            logger.info(f"🎯 Cache HIT: Retrieved SQL for '{validation_request[:50]}...' (key: {cache_key[:12]}...)")
            
            return {
                "legacy_sql": entry.legacy_sql,
                "prod_sql": entry.prod_sql,
                "explanation": entry.explanation
            }
    
    def cache_sql_result(
        self,
        legacy_table: str,
        prod_table: str,
        validation_request: str,
        sql_result: Dict[str, str],
        date_column: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        table_schema: Optional[Dict] = None
    ) -> str:
        """
        Cache the generated SQL result.
        
        Args:
            legacy_table: Legacy table name
            prod_table: Production table name  
            validation_request: Validation request text
            sql_result: Dictionary containing legacy_sql, prod_sql, explanation
            date_column: Optional date column
            start_date: Optional start date
            end_date: Optional end date
            table_schema: Optional schema information
            
        Returns:
            Cache key for the stored entry
        """
        with self._lock:
            cache_key = self._generate_cache_key(
                legacy_table, prod_table, validation_request,
                date_column, start_date, end_date, table_schema
            )
            
            # Create cache entry
            entry = CachedSQLEntry(
                cache_key=cache_key,
                legacy_sql=sql_result.get("legacy_sql", ""),
                prod_sql=sql_result.get("prod_sql", ""),
                explanation=sql_result.get("explanation", ""),
                legacy_table=legacy_table,
                prod_table=prod_table,
                validation_request=validation_request,
                date_column=date_column,
                start_date=start_date,
                end_date=end_date,
                created_at=time.time(),
                last_accessed=time.time()
            )
            
            # Store in memory cache
            self._memory_cache[cache_key] = entry
            
            # Enforce max entries limit
            if len(self._memory_cache) > self.max_entries:
                self._evict_oldest_entries()
            
            # Save to disk
            self._save_cache_to_disk()
            
            self.stats["saves"] += 1
            self._save_stats()
            
            logger.info(f"💾 Cache SAVE: Stored SQL for '{validation_request[:50]}...' (key: {cache_key[:12]}...)")
            
            return cache_key
    
    def _evict_oldest_entries(self):
        """Evict oldest entries to maintain max_entries limit."""
        try:
            # Sort by last_accessed time and remove oldest 10%
            entries_by_access = sorted(
                self._memory_cache.items(),
                key=lambda x: x[1].last_accessed
            )
            
            evict_count = max(1, len(entries_by_access) // 10)
            
            for i in range(evict_count):
                cache_key = entries_by_access[i][0]
                del self._memory_cache[cache_key]
                self.stats["evictions"] += 1
            
            logger.debug(f"Evicted {evict_count} old cache entries")
            
        except Exception as e:
            logger.error(f"Failed to evict cache entries: {e}")
    
    def _cleanup_expired_entries(self):
        """Remove expired cache entries."""
        try:
            expired_keys = []
            current_time = time.time()
            
            for cache_key, entry in self._memory_cache.items():
                if entry.is_expired(self.ttl_hours):
                    expired_keys.append(cache_key)
            
            for cache_key in expired_keys:
                del self._memory_cache[cache_key]
                if hasattr(self, "stats"):
                    self.stats["evictions"] += 1
            
            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
                self._save_cache_to_disk()
            
            if hasattr(self, "stats"):
                self.stats["last_cleanup"] = current_time
            
        except Exception as e:
            logger.error(f"Failed to cleanup expired entries: {e}")
    
    def clear_cache(self) -> int:
        """
        Clear all cache entries.
        
        Returns:
            Number of entries cleared
        """
        with self._lock:
            count = len(self._memory_cache)
            self._memory_cache.clear()
            
            # Remove cache files
            try:
                if self.cache_file.exists():
                    self.cache_file.unlink()
                if self.stats_file.exists():
                    self.stats_file.unlink()
            except Exception as e:
                logger.error(f"Failed to remove cache files: {e}")
            
            # Reset stats
            self.stats = {
                "hits": 0,
                "misses": 0,
                "saves": 0,
                "evictions": 0,
                "last_cleanup": time.time()
            }
            
            logger.info(f"Cache cleared: {count} entries removed")
            return count
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics and information."""
        with self._lock:
            total_requests = self.stats["hits"] + self.stats["misses"]
            hit_rate = (self.stats["hits"] / total_requests * 100) if total_requests > 0 else 0
            
            return {
                "entries_count": len(self._memory_cache),
                "max_entries": self.max_entries,
                "ttl_hours": self.ttl_hours,
                "cache_hits": self.stats["hits"],
                "cache_misses": self.stats["misses"],
                "hit_rate_percent": round(hit_rate, 2),
                "saves": self.stats["saves"],
                "evictions": self.stats["evictions"],
                "last_cleanup": datetime.fromtimestamp(self.stats["last_cleanup"]).isoformat(),
                "cache_size_mb": self._get_cache_size_mb()
            }
    
    def _get_cache_size_mb(self) -> float:
        """Calculate approximate cache size in MB."""
        try:
            if self.cache_file.exists():
                return round(self.cache_file.stat().st_size / (1024 * 1024), 2)
            return 0.0
        except Exception:
            return 0.0
    
    def list_cache_entries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        List recent cache entries for debugging.
        
        Args:
            limit: Maximum number of entries to return
            
        Returns:
            List of cache entry summaries
        """
        with self._lock:
            entries = sorted(
                self._memory_cache.values(),
                key=lambda x: x.last_accessed,
                reverse=True
            )[:limit]
            
            return [
                {
                    "cache_key": entry.cache_key[:16] + "...",
                    "validation_request": entry.validation_request[:100] + "..." if len(entry.validation_request) > 100 else entry.validation_request,
                    "tables": f"{entry.legacy_table} vs {entry.prod_table}",
                    "created_at": datetime.fromtimestamp(entry.created_at).isoformat(),
                    "last_accessed": datetime.fromtimestamp(entry.last_accessed).isoformat(),
                    "access_count": entry.access_count,
                    "age_hours": round((time.time() - entry.created_at) / 3600, 1)
                }
                for entry in entries
            ] 