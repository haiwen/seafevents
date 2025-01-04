# -*- coding: utf-8 -*-
import copy
import json
import logging
import uuid

import redis
import time

logger = logging.getLogger(__name__)

REDIS_METRIC_KEY = "metric"
LOCK_NAME = "metric_lock"


class RedisClient(object):

    def __init__(self, config, socket_connect_timeout=30, socket_timeout=None):
        self._host = '127.0.0.1'
        self._port = 6379
        self._password = None
        self.connection = None

        self._parse_config(config, socket_connect_timeout, socket_timeout)

    def _parse_config(self, config, socket_connect_timeout, socket_timeout):

        if not config.has_section('REDIS'):
            return

        if config.has_option('REDIS', 'server'):
            self._host = config.get('REDIS', 'server')

        if config.has_option('REDIS', 'port'):
            self._port = config.getint('REDIS', 'port')

        if config.has_option('REDIS', 'password'):
            self._password = config.get('REDIS', 'password')

        """
        By default, each Redis instance created will in turn create its own connection pool.
        Every caller using redis client will has it's own pool with config caller passed.
        """

        self.connection = redis.Redis(
            host=self._host, port=self._port, password=self._password, decode_responses=True,
            socket_timeout=socket_timeout, socket_connect_timeout=socket_connect_timeout,
        )

    def get_subscriber(self, channel_name):
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
        return self.connection.setnx(key, value)

    def expire(self, name, timeout):
        return self.connection.expire(name, timeout)

    def get(self, key):
        return self.connection.get(key)

    def set(self, key, value, timeout=None):
        if not timeout:
            return self.connection.set(key, value)
        else:
            return self.connection.settex(key, timeout, value)
    def delete(self, key):
        return self.connection.delete(key)

    def lrange(self, key, start, end):
        return self.connection.lrange(key, start, end)

    def lpush(self, key, value):
        return self.connection.lpush(key, value)

    def lrem(self, key, count):
        return self.connection.lrem(key, count)

    def publisher(self, channel, message):
        return self.connection.publish(channel, message)


class RedisCache(object):
    def __init__(self):
        self._redis_client = None


    def init_redis(self, config):
        self._redis_client = RedisClient(config)


    def get(self, key):
        return self._redis_client.get(key)


    def set(self, key, value, timeout=None):
        return self._redis_client.set(key, value, timeout=timeout)


    def delete(self, key):
        return self._redis_client.delete(key)

    def lrange(self, key, start, end):
        return self._redis_client.lrange(key, start, end)

    def lpush(self, key, value):
        return self._redis_client.lpush(key, value)

    def lrem(self, key, count):
        return self._redis_client.lrem(key, count)

    def acquire_lock(self):
        lock_value = str(uuid.uuid4())  # 创建一个唯一的锁标识
        if self._redis_client.setnx(LOCK_NAME, lock_value):  # 获取锁
            self._redis_client.expire(LOCK_NAME, timeout=10)  # 设置锁的过期时间，避免死锁
            return lock_value
        return None

    def release_lock(self):
        self._redis_client.delete(LOCK_NAME)

    def create_or_update(self, key, value):
        lock_value = self.acquire_lock()
        if lock_value:
            try:
                current_value = self._redis_client.get(key)
                if current_value:
                    current_value_dict_copy = copy.deepcopy(json.loads(current_value))
                    current_value_dict_copy.update(value)
                    self._redis_client.set(key, json.dumps(current_value_dict_copy))
                else:
                    self._redis_client.set(key, json.dumps(value))
            finally:
                self.release_lock()

    def publisher(self, channel, message):
        self._redis_client.publisher(channel, message)


redis_cache = RedisCache()
