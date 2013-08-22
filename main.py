#!/usr/bin/env python
#coding: utf-8

import argparse
import ConfigParser
import os
import sys
import logging
import libevent

import ccnet
from ccnet.async import AsyncClient

from seafevents.tasks import IndexUpdater, SeahubEmailSender
from seafevents.utils import do_exit, write_pidfile, ClientConnector, has_office_tools
from seafevents.utils.config import get_office_converter_conf
from seafevents.mq_listener import EventsMQListener
from seafevents.signal_handler import SignalHandler

if has_office_tools():
    from seafevents.office_converter import OfficeConverter
from seafevents.log import LogConfigurator

class AppArgParser(object):
    def __init__(self):
        self._parser = argparse.ArgumentParser(
            description='seafevents main program')

        self._add_args()

    def parse_args(self):
        return self._parser.parse_args()

    def _add_args(self):
        self._parser.add_argument(
            '--logfile',
            help='log file')

        self._parser.add_argument(
            '--config-file',
            default=os.path.join(os.getcwd(), 'events.conf'),
            help='seafevents config file')

        self._parser.add_argument(
            '--loglevel',
            default='debug',
        )

        self._parser.add_argument(
            '-P',
            '--pidfile',
            help='the location of the pidfile'
        )

        self._parser.add_argument(
            '-R',
            '--reconnect',
            action='store_true',
            help='try to reconnect to daemon when disconnected'
        )

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

    DUMMY_SERVICE = 'seafevents-dummy-service'
    DUMMY_SERVICE_GROUP = 'rpc-inner'

    def __init__(self, ccnet_dir, args):

        self._ccnet_dir = ccnet_dir
        self._args = args
        self._app_config = get_config(args.config_file)

        self._index_updater = IndexUpdater(self._app_config)
        self._seahub_email_sender = SeahubEmailSender(self._app_config)

        office_config = get_office_converter_conf(self._app_config)
        if has_office_tools():
            self._office_converter = OfficeConverter(office_config)

        self._ccnet_session = None
        self._sync_client = None

        self._evbase = libevent.Base() #pylint: disable=E1101
        self._mq_listener = EventsMQListener(self._args.config_file)
        self._sighandler = SignalHandler(self._evbase)

    def ensure_single_instance(self):
        '''Register a dummy service synchronously to ensure only a single
        instance is running

        '''
        self._sync_client = ccnet.SyncClient(self._ccnet_dir)
        self._sync_client.connect_daemon()
        try:
            self._sync_client.register_service_sync(self.DUMMY_SERVICE, self.DUMMY_SERVICE_GROUP)
        except:
            logging.exception('Another instance is already running')
            do_exit(1)

    def start_ccnet_session(self):
        '''Connect to ccnet-server, retry util connection is made'''
        self._ccnet_session = AsyncClient(self._ccnet_dir, self._evbase)
        connector = ClientConnector(self._ccnet_session)
        connector.connect_daemon_with_retry()

    def connect_ccnet(self):
        self.start_ccnet_session()
        self.ensure_single_instance()

        if has_office_tools() and self._office_converter.is_enabled():
            self._office_converter.register_rpc(self._ccnet_session)
        self._mq_listener.start(self._ccnet_session)

    def _serve(self):
        try:
            self._ccnet_session.main_loop()
        except ccnet.NetworkError:
            logging.warning('connection to ccnet-server is lost')
            if self._args.reconnect:
                self.connect_ccnet()
            else:
                do_exit(0)
        except Exception:
            logging.exception('Error in main_loop:')
            do_exit(0)

    def serve_forever(self):
        if self._index_updater.is_enabled():
            self._index_updater.start(self._evbase)
        else:
            logging.info('search indexer is disabled')

        if self._seahub_email_sender.is_enabled():
            self._seahub_email_sender.start(self._evbase)
        else:
            logging.info('seahub email sender is disabled')

        if has_office_tools() and self._office_converter.is_enabled():
            self._office_converter.start()
        else:
            logging.info('office converter is disabled')

        self.connect_ccnet()
        while True:
            self._serve()

def main():
    args = AppArgParser().parse_args()
    app_logger = LogConfigurator(args.loglevel, args.logfile)

    app = App(get_ccnet_dir(), args)

    if args.pidfile:
        write_pidfile(args.pidfile)

    app.serve_forever()

if __name__ ==  '__main__':
    main()