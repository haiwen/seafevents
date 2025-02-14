import time
import logging
import json
from threading import Thread

from seaserv import seafile_api

import seafevents.events.handlers as events_handlers
import seafevents.events_publisher.handlers as publisher_handlers
import seafevents.statistics.handlers as stats_handlers
from seafevents.db import init_db_session_class
from seafevents.app.event_redis import RedisClient
import seafevents.repo_metadata.handlers as metadata_handler

logger = logging.getLogger(__name__)

__all__ = [
    'EventsHandler',
    'init_message_handlers'
]


# 管理多个频道的消息处理，每个频道可以有多个处理器。
class MessageHandler(object):
    def __init__(self):
        # A (channel, List<handler>) map. For a given channel, there may be
        # multiple handlers
        # 创建一个空字典 (self._handlers) 来存储频道-处理器映射。
        self._handlers = {}

    # 将一个处理器函数 (func) 添加到特定的消息类型 (msg_type) 中的频道-处理器映射中。
    def add_handler(self, msg_type, func):
        if msg_type in self._handlers:
            funcs = self._handlers[msg_type]
        else:
            funcs = []
            self._handlers[msg_type] = funcs

        if func not in funcs:
            funcs.append(func)

    # 处理一个入站消息 (msg)，通过解析其内容，确定消息类型，并执行相应的处理器函数。
    def handle_message(self, config, session, redis_connection, channel, msg):
        try:
            content = json.loads(msg.get('content'))
        except:
            logger.warning("invalid message format: %s", msg)
            return

        if not content.get('msg_type'):
            return

        msg_type = channel + ':' + content.get('msg_type')
        if msg_type not in self._handlers:
            return

        funcs = self._handlers.get(msg_type)
        for func in funcs:
            try:
                if func.__name__ == 'RepoUpdatePublishHandler' or func.__name__ == 'RepoMetadataUpdateHandler':
                    func(config, redis_connection, msg)
                else:
                    func(config, session, msg)
            except Exception as e:
                logger.exception("error when handle msg: %s", e)

    # 返回一个包含注册处理器的频道的集合。
    def get_channels(self):
        channels = set()
        for msg_type in self._handlers:
            pos = msg_type.find(':')
            channels.add(msg_type[:pos])

        return channels


message_handler = MessageHandler()


# 该函数根据给定的配置初始化消息处理程序。它检查配置中是否启用了审计功能，如果启用，则使用 `message_handler` 对象注册事件、统计、发布和元数据处理程序，并将审计状态传递给事件处理程序。
def init_message_handlers(config):
    if config.has_option('Audit', 'enabled'):
        try:
            enable_audit = config.getboolean('Audit', 'enabled')
        except ValueError:
            enable_audit = False
    elif config.has_option('AUDIT', 'enabled'):
        try:
            enable_audit = config.getboolean('AUDIT', 'enabled')
        except ValueError:
            enable_audit = False
    else:
        enable_audit = False

    events_handlers.register_handlers(message_handler, enable_audit)
    stats_handlers.register_handlers(message_handler)
    publisher_handlers.register_handlers(message_handler)
    metadata_handler.register_handlers(message_handler)


class EventsHandler(object):

    # 初始化 EventsHandler 对象，设置数据库会话类和 Redis 连接。
    def __init__(self, config):
        self._config = config
        self._db_session_class = init_db_session_class(config)
        self._redis_connection = RedisClient().connection

    # 持续监听给定频道上的事件，使用 message_handler 处理每个事件，并处理可能发生的异常。
    def handle_event(self, channel):
        config = self._config
        session = self._db_session_class()
        redis_connection = self._redis_connection
        while 1:
            try:
                msg = seafile_api.pop_event(channel)
            except Exception as e:
                logger.error('Failed to get event: %s' % e)
                time.sleep(3)
                continue
            if msg:
                try:
                    message_handler.handle_message(config, session, redis_connection, channel, msg)
                except Exception as e:
                    logger.error(e)
                finally:
                    session.close()
                    if redis_connection:
                        redis_connection.close()
            else:
                time.sleep(0.5)

    #  启动事件处理过程，订阅所有可用的频道，并为每个频道创建一个新线程来并发处理事件。
    def start(self):
        channels = message_handler.get_channels()
        logger.info('Subscribe to channels: %s', channels)
        for channel in channels:
            event_handler = Thread(target=self.handle_event, args=(channel, ))
            event_handler.start()
