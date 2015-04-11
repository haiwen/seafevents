import logging

import seafevents.events.handlers as events_handlers
import seafevents.stats.handlers as stats_handlers

logger = logging.getLogger(__name__)

class MessageHandler(object):
    def __init__(self):
        # A (type, List<hander>) map. For a given message type, there may be
        # multiple handlers
        self._handlers = {}

    def add_handler(self, mtype, func):
        if mtype in self._handlers:
            funcs = self._handlers[mtype]
        else:
            funcs = []
            self._handlers[mtype] = funcs

        if func not in funcs:
            funcs.append(func)

    def handle_message(self, session, msg):
        pos = msg.body.find('\t')
        if pos == -1:
            logger.warning("invalid message format: %s", msg)
            return

        etype = msg.app + ':' + msg.body[:pos]
        if etype not in self._handlers:
            return

        funcs = self._handlers.get(etype)
        for func in funcs:
            try:
                func (session, msg)
            except:
                logger.exception("error when handle msg %s", msg)

    def get_mqs(self):
        '''Get the message queue names from registered handlers. messaage
        listener will listen to them in ccnet client.

        '''
        types = set()
        for mtype in self._handlers:
            pos = mtype.find(':')
            types.add(mtype[:pos])

        return types

def init_message_handlers(enable_audit):
    events_handlers.register_handlers(message_handler, enable_audit)
    stats_handlers.register_handlers(message_handler)

message_handler = MessageHandler()
