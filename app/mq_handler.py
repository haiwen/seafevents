import time
import logging
from threading import Thread

from seaserv import seafile_api

import seafevents.events.handlers as events_handlers
import seafevents.events_publisher.handlers as publisher_handlers
import seafevents.statistics.handlers as stats_handlers
from seafevents.db import init_db_session_class

logger = logging.getLogger(__name__)

__all__ = [
    'EventsHandler',
    'init_message_handlers'
]


class MessageHandler(object):
    def __init__(self):
        # A (channel, List<handler>) map. For a given channel, there may be
        # multiple handlers
        self._handlers = {}

    def add_handler(self, msg_type, func):
        if msg_type in self._handlers:
            funcs = self._handlers[msg_type]
        else:
            funcs = []
            self._handlers[msg_type] = funcs

        if func not in funcs:
            funcs.append(func)

    def handle_message(self, session, channel, msg):
        pos = msg['content'].find('\t')
        if pos == -1:
            logger.warning("invalid message format: %s", msg)
            return

        msg_type = channel + ':' + msg['content'][:pos]
        if msg_type not in self._handlers:
            return

        funcs = self._handlers.get(msg_type)
        for func in funcs:
            try:
                func(session, msg)
            except Exception as e:
                logger.exception("error when handle msg: %s", e)

    def get_channels(self):
        channels = set()
        for msg_type in self._handlers:
            pos = msg_type.find(':')
            channels.add(msg_type[:pos])

        return channels


message_handler = MessageHandler()


def init_message_handlers(enable_audit):
    events_handlers.register_handlers(message_handler, enable_audit)
    stats_handlers.register_handlers(message_handler)
    publisher_handlers.register_handlers(message_handler)


class EventsHandler(object):

    def __init__(self, events_conf):
        self._db_session_class = init_db_session_class(events_conf)

    def handle_event(self, channel):
        session = self._db_session_class()
        while 1:
            try:
                msg = seafile_api.pop_event(channel)
            except Exception as e:
                logger.error('Failed to get event: %s' % e)
                time.sleep(3)
                continue
            if msg:
                try:
                    message_handler.handle_message(session, channel, msg)
                except Exception as e:
                    logger.error(e)
                finally:
                    session.close()
            else:
                time.sleep(0.5)

    def start(self):
        channels = message_handler.get_channels()
        logger.info('Subscribe to channels: %s', channels)
        for channel in channels:
            event_handler = Thread(target=self.handle_event, args=(channel, ))
            event_handler.setDaemon(True)
            event_handler.start()
