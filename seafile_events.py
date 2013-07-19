#!/usr/bin/env python
#coding: utf-8

import argparse
import ConfigParser
import os
import sys
import time
import signal
import logging
import libevent
import threading
import Queue

import ccnet
from ccnet.async import AsyncClient, RpcServerProc
from pysearpc import searpc_server

from events.db import init_db_session_class
from events.handler import handle_message
from tasks import IndexUpdateTimer, SendSeahubEmailTimer
from utils import do_exit, write_pidfile, get_seafes_conf, get_office_converter_conf, has_office_tools, \
    get_seahub_email_conf

if has_office_tools():
    from office_converter import office_converter, OFFICE_RPC_SERVICE_NAME

def parse_args():
    parser = argparse.ArgumentParser(
         description='seafile events recorder')

    parser.add_argument(
        '--logfile',
        default=sys.stdout,
        type=argparse.FileType('a'),
        help='log file')

    parser.add_argument(
        '--config-file',
        default=os.path.join(os.getcwd(), 'events.conf'),
        help='seafevents config file')

    parser.add_argument(
        '--loglevel',
        default='debug',
    )

    parser.add_argument(
        '-P',
        '--pidfile',
        help='the location of the pidfile'
    )

    return parser.parse_args()

def init_logging(args):
    """Configure logging module"""
    level = args.loglevel

    if level == 'debug':
        level = logging.DEBUG
    elif level == 'info':
        level = logging.INFO
    else:
        level = logging.WARNING

    kw = {
        'format': '[%(asctime)s] [%(levelname)s] %(message)s',
        'datefmt': '%m/%d/%Y %H:%M:%S',
        'level': level,
        'stream': args.logfile
    }

    logging.basicConfig(**kw)

    logging.getLogger('events.db.GreenQueuePool').setLevel(logging.WARN)

def sigint_handler(*args):
    dummy = args
    do_exit(0)

def sigchild_handler(*args):
    dummy = args
    try:
        os.wait3(os.WNOHANG)
    except:
        pass

def set_signal_handler():
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    signal.signal(signal.SIGQUIT, sigint_handler)

    signal.signal(signal.SIGCHLD, sigchild_handler)

def get_config(config_file):
    config = ConfigParser.ConfigParser()
    try:
        config.read(config_file)
    except Exception, e:
        logging.critical('failed to read config file %s', e)
        do_exit(1)

    return config

def get_ccnet_dir():
    try:
        return os.environ['CCNET_CONF_DIR']
    except KeyError:
        raise RuntimeError('ccnet config dir is not set')

class SeafEventsThread(threading.Thread):
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

class App(object):

    SERVER_EVENTS_MQ = 'seaf_server.event'

    RECONNECT_CCNET_INTERVAL = 2

    DUMMY_SERVICE = 'seafevents-dummy-service'
    DUMMY_SERVICE_GROUP = 'rpc-inner'

    def __init__(self, ccnet_dir, args):

        self._ccnet_dir = ccnet_dir
        self._args = args
        self._app_config = get_config(args.config_file)

        self._seafes_conf = get_seafes_conf(self._app_config)
        self._office_converter_conf = get_office_converter_conf(self._app_config)
        self._seahub_email_conf = get_seahub_email_conf(self._app_config)

        self._db_session_class = init_db_session_class(args.config_file)

        self._ccnet_session = None
        self._mq_client = None
        self._sync_client = None

        self._evbase = libevent.Base()

        self._events_queue = Queue.Queue()
        self._seafevents_thread = None
        self._index_timer = None
        self._sendmail_timer = None

    def ensure_single_instance(self):
        '''Register a dummy service synchronously to ensure only a single
        instance is running

        '''
        self._sync_client = ccnet.SyncClient(self._ccnet_dir)
        self._sync_client.connect_daemon()
        try:
            self._sync_client.register_service_sync(self.DUMMY_SERVICE,
                                                   self.DUMMY_SERVICE_GROUP)
        except:
            logging.exception('Another instance is already running')
            do_exit(1)

    def register_office_rpc(self):
        '''Register office rpc service'''

        searpc_server.create_service(OFFICE_RPC_SERVICE_NAME)
        self._ccnet_session.register_service(OFFICE_RPC_SERVICE_NAME,
                                            'basic',
                                            RpcServerProc)

        searpc_server.register_function(OFFICE_RPC_SERVICE_NAME,
                                        office_converter.query_convert_status)

        searpc_server.register_function(OFFICE_RPC_SERVICE_NAME,
                                        office_converter.query_file_pages)

        searpc_server.register_function(OFFICE_RPC_SERVICE_NAME,
                                        office_converter.add_task)

    def start_search_indexer(self):
        conf = self._seafes_conf
        timeout = conf['interval']
        logging.info('search indexer is started, interval = %s sec, seafesdir = %s',
                     timeout, conf['seafesdir'])
        self._index_timer = IndexUpdateTimer(self._evbase, timeout, conf)

    def start_send_seahub_email(self):
        conf = self._seahub_email_conf
        timeout = conf['interval']
        logging.info('seahub email sender is started, interval = %s sec', timeout)
        self._sendmail_timer = SendSeahubEmailTimer(self._evbase, timeout, conf)

    def start_office_converter(self):
        office_converter.start(self._office_converter_conf)

    def start_events_thread(self):
        '''Starts the worker thread for saving events'''
        self._seafevents_thread = SeafEventsThread(self._db_session_class,
                                                   self._events_queue)
        self._seafevents_thread.setDaemon(True)
        self._seafevents_thread.start()

    def msg_cb(self, msg):
        self._events_queue.put(msg)

    def start_ccnet_session(self):
        '''Connect to ccnet-server, retry util connection is made'''
        self._ccnet_session = AsyncClient(self._ccnet_dir, self._evbase)

        while True:
            logging.info('try to connect to ccnet-server...')
            try:
                self._ccnet_session.connect_daemon()
                logging.info('connected to ccnet server')
                break
            except ccnet.NetworkError:
                time.sleep(self.RECONNECT_CCNET_INTERVAL)

    def start_mq_client(self):
        self._mq_client = self._ccnet_session.create_master_processor('mq-client')
        self._mq_client.set_callback(self.msg_cb)
        self._mq_client.start(self.SERVER_EVENTS_MQ)
        logging.info('listen to mq: %s', self.SERVER_EVENTS_MQ)

    def connect_ccnet(self):
        self.start_ccnet_session()
        self.ensure_single_instance()

        if self.is_office_converter_enabled():
            self.register_office_rpc()
        self.start_mq_client()

    def is_office_converter_enabled(self):
        if not has_office_tools():
            return False

        if self._office_converter_conf and self._office_converter_conf['enabled']:
            return True
        else:
            return False

    def is_search_indexer_enabled(self):
        if self._seafes_conf and self._seafes_conf['enabled']:
            return True
        else:
            return False

    def is_send_seahub_email_enabled(self):
        if self._seahub_email_conf and self._seahub_email_conf['enabled']:
            return True
        else:
            return False

    def _serve(self):
        try:
            self._ccnet_session.main_loop()
        except ccnet.NetworkError:
            logging.warning('connection to ccnet-server is lost')
            self.connect_ccnet()
        except Exception:
            logging.exception('Error in main_loop:')
            do_exit(0)

    def serve_forever(self):
        self.start_events_thread()

        if self.is_search_indexer_enabled():
            self.start_search_indexer()
        else:
            logging.info('search indexer is disabled')

        if self.is_send_seahub_email_enabled():
            self.start_send_seahub_email()
        else:
            logging.info('seahub email sending is disabled')

        if self.is_office_converter_enabled():
            self.start_office_converter()
        else:
            logging.info('office converter is disabled')

        self.connect_ccnet()
        while True:
            self._serve()

def main():
    args = parse_args()
    init_logging(args)
    ccnet_dir = get_ccnet_dir()

    app = App(ccnet_dir, args)

    set_signal_handler()

    if args.pidfile:
        write_pidfile(args.pidfile)

    app.serve_forever()

if __name__ ==  '__main__':
    main()