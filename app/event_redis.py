# -*- coding: utf-8 -*-
import copy
import json
import logging
from seafevents.app.config import REDIS_SERVER, REDIS_PORT, REDIS_PASSWORD
import uuid

import redis
import time

logger = logging.getLogger(__name__)

REDIS_METRIC_KEY = "metric"

class RedisClient(object):

    def __init__(self, socket_connect_timeout=30, socket_timeout=None):
        self._host = REDIS_SERVER
        self._port = REDIS_PORT
        self._password = REDIS_PASSWORD
        self.connection = None

        self._init_config(socket_connect_timeout, socket_timeout)
    
    def _init_config(self, socket_connect_timeout, socket_timeout):
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
    
    def get_subscriber(self, channel_name):
        if not self.connection:
            return
        while True:
            try:
                subscriber = self.connection.pubsub(ignore_subscribe_messages=True)
                subscriber.subscribe(channel_name)
            except redis.AuthenticationError as e:
                logger.critical('connect to redis auth error: %s', e)
                raise e
            except Exception as e:
                logger.error('redis pubsub failed. {} retry after 10s'.format(e))
                time.sleep(10)
            else:
                return subscriber

    def setnx(self, key, value):
        if not self.connection:
            return
        return self.connection.setnx(key, value)

    def expire(self, name, timeout):
        if not self.connection:
            return
        return self.connection.expire(name, timeout)

    def publish(self, channel, message):
        if not self.connection:
            return
        return self.connection.publish(channel, message)


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

    def create_or_update(self, key, value):
        # self._redis_client.delete(key)
        try:
            current_value = self._redis_client.get(key)
            if current_value:
                current_value_dict_copy = copy.deepcopy(json.loads(current_value))
                current_value_dict_copy.update(value)
                self._redis_client.set(key, json.dumps(current_value_dict_copy))
            else:
                self._redis_client.set(key, json.dumps(value))
        except Exception as e:
            logger.error(e)


    def publish(self, channel, message):
        self._redis_client.publish(channel, message)

    
redis_cache = RedisCache()
