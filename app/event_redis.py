# -*- coding: utf-8 -*-
import os
import logging
from seafevents.utils import get_opt_from_env
from seafevents.app.config import REDIS_SERVER, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger(__name__)


class RedisClient(object):

    def __init__(self, socket_connect_timeout=30, socket_timeout=None):
        self._host = REDIS_SERVER
        self._port = REDIS_PORT
        self._password = REDIS_PASSWORD
        self.connection = None

        self._init_config_from_env(socket_connect_timeout, socket_timeout)
    
    def _init_config_from_env(self, socket_connect_timeout, socket_timeout):
        if self._host and self._port:
            import redis
            self.connection = redis.Redis(
                host=self._host, port=self._port, password=self._password, decode_responses=True,
                socket_timeout=socket_timeout, socket_connect_timeout=socket_connect_timeout,
            )
        else:
            logging.warning('Redis has not been set up')

    def get(self, key):
        if not self.connection:
            return
        return self.connection.get(key)

    def set(self, key, value, timeout=None):
        if not self.connection:
            return
        if not timeout:
            return self.connection.set(key, value)
        else:
            return self.connection.setex(key, timeout, value)

    def delete(self, key):
        if not self.connection:
            return
        return self.connection.delete(key)

class RedisCache(object):

    CACHE_NAME = 'redis'

    def __init__(self):
        self._redis_client = RedisClient()
        
    def get(self, key):
        return self._redis_client.get(key)

    def set(self, key, value, timeout=None):
        return self._redis_client.set(key, value, timeout=timeout)

    def delete(self, key):
        return self._redis_client.delete(key)
    
redis_cache = RedisCache()
