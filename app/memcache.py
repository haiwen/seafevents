# cache obj : memcache
import os
import logging
import memcache
from seafevents.utils import get_opt_from_env
from seafevents.app.config import MEMCACHED_SERVER, MEMCACHED_PORT



class Memcache(object):

    CACHE_NAME = 'memcached'

    def __init__(self):
        self._host = MEMCACHED_SERVER
        self._port = MEMCACHED_PORT
        
        self.cache = None
        self._init_config_from_env()

    def _init_config_from_env(self):
        if self._host and self._port:
            self.cache = memcache.Client(['%s:%s' % (self._host, self._port)], debug=1)
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
