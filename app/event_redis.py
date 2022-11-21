# -*- coding: utf-8 -*-
import logging
import redis

logger = logging.getLogger(__name__)


class RedisClient(object):

    def __init__(self, config, socket_connect_timeout=30, socket_timeout=None):
        self._host = '127.0.0.1'
        self._port = 6379
        self._password = None
        self.connection = None

        self._parse_config(config, socket_connect_timeout, socket_timeout)

    def _parse_config(self, config, socket_connect_timeout, socket_timeout):
        mq_type = ''
        if config.has_option('EVENTS PUBLISH', 'mq_type'):
            mq_type = config.get('EVENTS PUBLISH', 'mq_type').upper()
        if mq_type != 'REDIS':
            logging.warning("Unknown database backend: %s" % mq_type)
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
