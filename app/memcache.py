# cache obj : memcache
import os
import logging
import memcache
from seafevents.utils import get_opt_from_env


class Memcache(object):

    CACHE_NAME = 'memcached'

    def __init__(self):
        self._host = '127.0.0.1'
        self._port = 11211
        
        self.cache = None
        self._init_config_from_env()

    def _init_config_from_env(self):
        enable_memcached = get_opt_from_env('ENABLE_MEMCACHED') == 'true'
        if not enable_memcached:
            return
        m_host = get_opt_from_env('MEMCACHED_SERVER')
        if m_host:
            self._host = m_host
        m_port = get_opt_from_env('MEMCACHED_PORT')
        if m_port:
            self._port = m_port

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
