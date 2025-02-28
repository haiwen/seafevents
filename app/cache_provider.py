import os
from seafevents.app.event_redis import redis_cache
from seafevents.app.memcache import mem_cache

MEMCACHE_PROVIDER = 'memcached'
REDIS_CACHE_PROVIDER = 'redis'


class CacheProvider(object):

    def __init__(self):
        self.cache_client = None
        self._init_cache_provider_from_env()

    def _init_cache_provider_from_env(self):
        cache_provider = os.environ.get('CACHE_PROVIDER')
        if cache_provider not in [MEMCACHE_PROVIDER, REDIS_CACHE_PROVIDER]:
            return
        
        if cache_provider == MEMCACHE_PROVIDER:
            self.cache_client = mem_cache
        else:
            self.cache_client = redis_cache

    def set(self, key, value, timeout=0):
        
        if not self.cache_client:
            return 
        return self.cache_client.set(key, value, timeout)

    def get(self, key):
        
        if not self.cache_client:
            return
        return self.cache_client.get(key)
    
    def delete(self, key):
        
        if not self.cache_client:
            return
        return self.cache_client.delete(key)
    
cache = CacheProvider()
