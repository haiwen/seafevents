#coding: utf-8

import threading
import Queue
import logging
import ConfigParser

import seafevents.events.handlers as events_handlers
import seafevents.events_publisher.handlers as publisher_handlers
import seafevents.statistics.handlers as stats_handlers
from seafevents.db import init_db_session_class
from sqlalchemy.orm.scoping import scoped_session

logger = logging.getLogger(__name__)

__all__ = [
    'EventsMQListener',
]


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
    publisher_handlers.register_handlers(message_handler)

message_handler = MessageHandler()


class EventsMQListener(object):
    # SERVER_EVENTS_MQS = [
    #     'seaf_server.event',
    #     'seahub.stats',
    # ]

    def __init__(self, events_conf):
        self._events_queue = Queue.Queue()
        self._db_session_class = init_db_session_class(events_conf)
        self._seafevents_thread = None
        self._mq_client = None
        self.config = ConfigParser.ConfigParser()
        self.config.read(events_conf)

    def start(self, async_client):
        if self._seafevents_thread is None:
            self._start_worker_thread()

        self._mq_client = async_client.create_master_processor('mq-client')
        self._mq_client.set_callback(self.message_cb)
        mqs = message_handler.get_mqs()
        self._mq_client.start(*mqs)
        logging.info('listen to mq: %s' % mqs)

    def message_cb(self, message):
        logging.info('Lenth of events_queue: %d' % self._events_queue.qsize())
        self._events_queue.put(message)

    def _start_worker_thread(self):
        '''Starts the worker thread for saving events'''
        nthreads = 6
        try:
            nthreads = self.config.getint('DEFAULT', 'mq_worker')
        except Exception as e:
            logging.info(e)
            logging.info('set mq_worker to 2')
            pass
        if nthreads < 0:
            logging.info("mq_worker can't less than 0, has been set to 6")
            nthreads = 6

        for i in xrange(nthreads):
            _seafevents_thread = SeafEventsThread(self._db_session_class,
                                                  self._events_queue)
            _seafevents_thread.setDaemon(True)
            _seafevents_thread.start()

class SeafEventsThread(threading.Thread):
    '''Worker thread for saving events to databases'''
    def __init__(self, db_session_class, msg_queue):
        threading.Thread.__init__(self)
        self._db_session_class = db_session_class
        self._msg_queue = msg_queue

    def do_work(self, msg):
        session = scoped_session(self._db_session_class)
        try:
            message_handler.handle_message(session, msg)
        finally:
            session.remove()

    def run(self):
        while True:
            msg = self._msg_queue.get()
            self.do_work(msg)
