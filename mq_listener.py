import threading
import Queue
import logging

from seafevents.events.db import init_db_session_class
from seafevents.events.handler import handle_message

__all__ = [
    'EventsMQListener',
]

class EventsMQListener(object):
    SERVER_EVENTS_MQ = 'seaf_server.event'

    def __init__(self, events_conf):
        self._events_queue = Queue.Queue()
        self._db_session_class = init_db_session_class(events_conf)

        self._seafevents_thread = None
        self._mq_client = None

    def start(self, async_client):
        if self._seafevents_thread is None:
            self._start_worker_thread()

        self._mq_client = async_client.create_master_processor('mq-client')
        self._mq_client.set_callback(self.message_cb)
        self._mq_client.start(self.SERVER_EVENTS_MQ)
        logging.info('listen to mq: %s', self.SERVER_EVENTS_MQ)

    def message_cb(self, message):
        self._events_queue.put(message)

    def _start_worker_thread(self):
        '''Starts the worker thread for saving events'''
        self._seafevents_thread = SeafEventsThread(self._db_session_class,
                                                   self._events_queue)
        self._seafevents_thread.setDaemon(True)
        self._seafevents_thread.start()

class SeafEventsThread(threading.Thread):
    '''Worker thread for saving events to databases'''
    def __init__(self, db_session_class, msg_queue):
        threading.Thread.__init__(self)
        self._db_session_class = db_session_class
        self._msg_queue = msg_queue

    def do_work(self, msg):
        session = self._db_session_class()
        try:
            handle_message(session, msg)
        finally:
            session.close()

    def run(self):
        while True:
            msg = self._msg_queue.get()
            self.do_work(msg)