import json
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

class CacheManager:
    def __init__(self, redis_url: str = "redis://localhost:6379", ttl_hours: int = 24):
        self.ttl = timedelta(hours=ttl_hours)
        self._memory_cache = {}  # Fallback in-memory cache
        self._cache_timestamps = {}  # Track expiration
        
        try:
            import redis
            self.redis_client = redis.from_url(redis_url, decode_responses=True)
            # Test connection
            self.redis_client.ping()
            self.use_redis = True
            print("Redis connected successfully")
        except Exception as e:
            print(f"Redis not available, using memory cache: {e}")
            self.redis_client = None
            self.use_redis = False
    
    def _is_expired(self, key: str) -> bool:
        """Check if cache entry is expired."""
        if key not in self._cache_timestamps:
            return True
        return datetime.now() > self._cache_timestamps[key]
    
    def _cleanup_expired(self):
        """Remove expired entries from memory cache."""
        expired_keys = [k for k in self._cache_timestamps if self._is_expired(k)]
        for k in expired_keys:
            self._memory_cache.pop(k, None)
            self._cache_timestamps.pop(k, None)
    
    def _make_key(self, data: Dict[str, Any], prefix: str) -> str:
        """Create a cache key based on data hash."""
        # Create a deterministic hash from the data
        data_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
        hash_obj = hashlib.sha256(data_str.encode())
        return f"{prefix}:{hash_obj.hexdigest()[:16]}"
    
    def get_rfm_result(self, file_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached RFM analysis result."""
        if self.use_redis:
            try:
                key = f"rfm:{file_hash}"
                cached = self.redis_client.get(key)
                if cached:
                    return json.loads(cached)
            except Exception as e:
                print(f"Redis get error: {e}")
        
        # Fallback to memory cache
        key = f"rfm:{file_hash}"
        self._cleanup_expired()
        if not self._is_expired(key):
            return self._memory_cache.get(key)
        return None
    
    def set_rfm_result(self, file_hash: str, result: Dict[str, Any]) -> None:
        """Cache RFM analysis result."""
        # Convert datetime objects to strings for JSON serialization
        def convert_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, dict):
                return {k: convert_datetime(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_datetime(item) for item in obj]
            return obj
        
        serializable_result = convert_datetime(result)
         
        if self.use_redis:
            try:
                key = f"rfm:{file_hash}"
                self.redis_client.setex(key, self.ttl, json.dumps(serializable_result))
            except Exception as e:
                print(f"Redis set error: {e}")
        
        # Always store in memory cache as fallback
        key = f"rfm:{file_hash}"
        self._memory_cache[key] = serializable_result
        self._cache_timestamps[key] = datetime.now() + self.ttl
    
    def get_ai_insights(self, summary: Dict[str, Any], top_customers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Get cached AI insights."""
        if self.use_redis:
            try:
                cache_data = {
                    "summary": summary,
                    "top_customers": top_customers[:5]  # Use first 5 for cache key
                }
                key = self._make_key(cache_data, "ai_insights")
                cached = self.redis_client.get(key)
                if cached:
                    return json.loads(cached)
            except Exception as e:
                print(f"Redis AI get error: {e}")
        
        # Fallback to memory cache
        cache_data = {
            "summary": summary,
            "top_customers": top_customers[:5]
        }
        key = self._make_key(cache_data, "ai_insights")
        self._cleanup_expired()
        if not self._is_expired(key):
            return self._memory_cache.get(key)
        return None
    
    def set_ai_insights(self, summary: Dict[str, Any], top_customers: List[Dict[str, Any]], insights: Dict[str, Any]) -> None:
        """Cache AI insights."""
        if self.use_redis:
            try:
                cache_data = {
                    "summary": summary,
                    "top_customers": top_customers[:5]
                }
                key = self._make_key(cache_data, "ai_insights")
                self.redis_client.setex(key, self.ttl, json.dumps(insights))
            except Exception as e:
                print(f"Redis AI set error: {e}")
        
        # Always store in memory cache as fallback
        cache_data = {
            "summary": summary,
            "top_customers": top_customers[:5]
        }
        key = self._make_key(cache_data, "ai_insights")
        self._memory_cache[key] = insights
        self._cache_timestamps[key] = datetime.now() + self.ttl
    
    def get_agent_actions(self, customers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Get cached agent actions."""
        if self.use_redis:
            try:
                # Use customer emails and tiers for cache key
                cache_data = [
                    {"email": c.get("email"), "tier": c.get("tier"), "churn_risk": c.get("churn_risk")}
                    for c in customers if c.get("tier") in ["At-Risk", "Watchlist"] or c.get("churn_risk", 0) >= 70
                ]
                key = self._make_key({"customers": cache_data}, "agent_actions")
                cached = self.redis_client.get(key)
                if cached:
                    return json.loads(cached)
            except Exception as e:
                print(f"Redis agent get error: {e}")
        
        # Fallback to memory cache
        cache_data = [
            {"email": c.get("email"), "tier": c.get("tier"), "churn_risk": c.get("churn_risk")}
            for c in customers if c.get("tier") in ["At-Risk", "Watchlist"] or c.get("churn_risk", 0) >= 70
        ]
        key = self._make_key({"customers": cache_data}, "agent_actions")
        self._cleanup_expired()
        if not self._is_expired(key):
            return self._memory_cache.get(key)
        return None
    
    def set_agent_actions(self, customers: List[Dict[str, Any]], actions: Dict[str, Any]) -> None:
        """Cache agent actions."""
        if self.use_redis:
            try:
                cache_data = [
                    {"email": c.get("email"), "tier": c.get("tier"), "churn_risk": c.get("churn_risk")}
                    for c in customers if c.get("tier") in ["At-Risk", "Watchlist"] or c.get("churn_risk", 0) >= 70
                ]
                key = self._make_key({"customers": cache_data}, "agent_actions")
                self.redis_client.setex(key, self.ttl, json.dumps(actions))
            except Exception as e:
                print(f"Redis agent set error: {e}")
        
        # Always store in memory cache as fallback
        cache_data = [
            {"email": c.get("email"), "tier": c.get("tier"), "churn_risk": c.get("churn_risk")}
            for c in customers if c.get("tier") in ["At-Risk", "Watchlist"] or c.get("churn_risk", 0) >= 70
        ]
        key = self._make_key({"customers": cache_data}, "agent_actions")
        self._memory_cache[key] = actions
        self._cache_timestamps[key] = datetime.now() + self.ttl
    
    def get_file_hash(self, file_content: bytes) -> str:
        """Generate hash for file content."""
        return hashlib.sha256(file_content).hexdigest()[:16]
    
    def clear_cache(self) -> None:
        """Clear all cache entries (for testing)."""
        if self.use_redis:
            try:
                self.redis_client.flushdb()
            except Exception as e:
                print(f"Redis clear error: {e}")
        
        # Clear memory cache
        self._memory_cache.clear()
        self._cache_timestamps.clear()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        self._cleanup_expired()
        # Calculate memory usage safely without JSON serialization
        try:
            memory_size = len(str(self._memory_cache))
        except:
            memory_size = 0
        return {
            "connected_clients": 0,
            "used_memory": f"{memory_size}B",
            "keyspace_hits": 0,
            "keyspace_misses": 0,
            "total_keys": len(self._memory_cache),
            "cache_type": "Memory"
        }

# Global cache instance
cache = CacheManager()
