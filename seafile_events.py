#!/usr/bin/env python
#coding: utf-8

import gevent
from gevent import monkey
monkey.patch_all(thread=False)

import argparse
import ConfigParser
import os
import sys
import time
import signal
import logging

import ccnet
from pysearpc import searpc_server

from events.db import init_db_session_class
from events.handler import handle_message
from office_converter import office_converter, OFFICE_RPC_SERVICE_NAME
from index import index_files
from utils import do_exit, write_pidfile, get_seafes_conf, get_office_converter_conf

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
        help='ccnet server config directory')

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
        'format': '[%(asctime)s] [%(module)s] %(message)s',
        'datefmt': '%m/%d/%Y %H:%M:%S',
        'level': level,
        'stream': args.logfile
    }

    logging.basicConfig(**kw)

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
    gevent.signal(signal.SIGINT, sigint_handler)
    gevent.signal(signal.SIGCHLD, sigchild_handler)
    gevent.signal(signal.SIGQUIT, gevent.shutdown)

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

class App(object):

    SERVER_EVENTS_MQ = 'seaf_server.event'

    RECONNECT_CCNET_INTERVAL = 2

    def __init__(self, ccnet_dir, args):

        self.ccnet_dir = ccnet_dir
        self.args = args
        self.app_config = get_config(args.config_file)

        self.seafes_conf = get_seafes_conf(self.app_config)
        self.office_converter_conf = get_office_converter_conf(self.app_config)

        self.DBSessionClass = init_db_session_class(args.config_file)
        self.db_session = self.DBSessionClass()

        self.ccnet_session = None
        self.mq_client = None

    def ensure_single_instance(self):
        # TODO: register a dummy service synchronously to ensure only a single
        # instance is running
        pass

    def register_office_rpc(self):
        '''Register office rpc service'''

        searpc_server.create_service(OFFICE_RPC_SERVICE_NAME)
        self.ccnet_session.register_service(OFFICE_RPC_SERVICE_NAME,
                                            'basic',
                                            ccnet.RpcServerProc)

        searpc_server.register_function(OFFICE_RPC_SERVICE_NAME,
                                        office_converter.query_convert_status)

        searpc_server.register_function(OFFICE_RPC_SERVICE_NAME,
                                        office_converter.query_file_pages)

        searpc_server.register_function(OFFICE_RPC_SERVICE_NAME,
                                        office_converter.add_task)

    def start_search_indexer(self):
        gevent.spawn(index_files, self.seafes_conf)

    def start_office_converter(self):
        office_converter.start(self.office_converter_conf)

    def msg_cb(self, msg):
        handle_message(self.db_session, msg)
        logging.info('listen to mq: %s', self.SERVER_EVENTS_MQ)

    def start_ccnet_session(self):
        '''Connect to ccnet-server, retry util connection is made'''
        self.ccnet_session = ccnet.AsyncClient(self.ccnet_dir)

        while True:
            logging.info('try to connect to ccnet-server...')
            try:
                self.ccnet_session.connect_daemon()
                logging.info('connected to ccnet server')
                break
            except ccnet.NetworkError:
                time.sleep(self.RECONNECT_CCNET_INTERVAL)


    def start_mq_client(self):
        self.mq_client = self.ccnet_session.create_master_processor('mq-client')
        self.mq_client.set_callback(self.msg_cb)
        self.mq_client.start(self.SERVER_EVENTS_MQ)

    def connect_ccnet(self):
        self.start_ccnet_session()
        self.register_office_rpc()
        self.start_mq_client()

    def is_office_converter_enabled(self):
        if self.office_converter_conf and self.office_converter_conf['enabled']:
            return True
        else:
            return False

    def is_search_indexer_enabled(self):
        if self.seafes_conf and self.seafes_conf['enabled']:
            return True
        else:
            return False

    def _serve(self):
        try:
            self.ccnet_session.main_loop()
        except ccnet.NetworkError:
            logging.warning('connection to ccnet-server is lost')
            self.connect_ccnet()
        except Exception:
            logging.exception('Error in main_loop:')
            do_exit(0)

    def serve_forever(self):
        if self.is_search_indexer_enabled():
            self.start_search_indexer()

        if self.is_office_converter_enabled():
            self.start_office_converter()

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