import logging
import memcache
from seafevents.app.config import MEMCACHED_HOST, MEMCACHED_PORT



class Memcache(object):

    CACHE_NAME = 'memcached'

    def __init__(self):
        self._host = MEMCACHED_HOST
        self._port = MEMCACHED_PORT
        
        self.cache = None
        self._init_config()

    def _init_config(self):
        if self._host and self._port:
            self.cache = memcache.Client(['%s:%s' % (self._host, self._port)])
        else:
            logging.warning("Memcached has not been set up")

    def set(self, key, value, timeout=None):
        if not self.cache:
            return
        return self.cache.set(key, value, timeout)

    def get(self, key):
        if not self.cache:
            return
        return self.cache.get(key)
    
    def delete(self, key):
        if not self.cache:
            return
        return self.cache.delete(key)
    
mem_cache = Memcache()
