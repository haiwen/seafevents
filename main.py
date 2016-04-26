#!/usr/bin/env python
#coding: utf-8

import argparse
import ConfigParser
import os
import logging
import libevent

import ccnet
from ccnet.async import AsyncClient

from seafevents import is_audit_enabled
from seafevents.tasks import IndexUpdater, SeahubEmailSender, LdapSyncer, VirusScanner
from seafevents.utils import do_exit, write_pidfile, ClientConnector, has_office_tools
from seafevents.utils.config import get_office_converter_conf
from seafevents.mq_listener import EventsMQListener
from seafevents.signal_handler import SignalHandler
from seafevents.message_handler import init_message_handlers

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
    def __init__(self, ccnet_dir, args, events_listener_enabled=True, background_tasks_enabled=True):
        self._ccnet_dir = ccnet_dir
        self._central_config_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR')
        self._args = args
        self._events_listener_enabled = events_listener_enabled
        self._bg_tasks_enabled = background_tasks_enabled

        self._events_listener = None
        if self._events_listener_enabled:
            self._events_listener = EventsListener(args.config_file)

        self._bg_tasks = None
        if self._bg_tasks_enabled:
            self._bg_tasks = BackgroundTasks(args.config_file)

        self._ccnet_session = None
        self._sync_client = None

        self._evbase = libevent.Base() #pylint: disable=E1101
        self._mq_listener = EventsMQListener(self._args.config_file)
        self._sighandler = SignalHandler(self._evbase)

    def start_ccnet_session(self):
        '''Connect to ccnet-server, retry util connection is made'''
        self._ccnet_session = AsyncClient(self._ccnet_dir,
                                          self._evbase,
                                          central_config_dir=self._central_config_dir)
        connector = ClientConnector(self._ccnet_session)
        connector.connect_daemon_with_retry()

        self._sync_client = ccnet.SyncClient(self._ccnet_dir,
                                             central_config_dir=self._central_config_dir)
        self._sync_client.connect_daemon()

    def connect_ccnet(self):
        self.start_ccnet_session()

        if self._events_listener:
            self._events_listener.on_ccnet_connected(self._ccnet_session, self._sync_client)
        if self._bg_tasks:
            self._bg_tasks.on_ccnet_connected(self._ccnet_session, self._sync_client)

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
        self.connect_ccnet()

        if self._bg_tasks:
            self._bg_tasks.start(self._evbase)

        if self._events_listener:
            self._events_listener.start(self._evbase)

        while True:
            self._serve()

class EventsListener(object):
    DUMMY_SERVICE = 'seafevents-events-dummy-service'
    DUMMY_SERVICE_GROUP = 'rpc-inner'

    def __init__(self, config_file):
        self._mq_listener = EventsMQListener(config_file)

    def _ensure_single_instance(self, sync_client):
        try:
            sync_client.register_service_sync(self.DUMMY_SERVICE, self.DUMMY_SERVICE_GROUP)
        except:
            logging.exception('Another instance is already running')
            do_exit(1)

    def on_ccnet_connected(self, async_client, sync_client):
        self._ensure_single_instance(sync_client)
        self._mq_listener.start(async_client)

    def start(self, base):
        pass

class BackgroundTasks(object):
    DUMMY_SERVICE = 'seafevents-background-tasks-dummy-service'
    DUMMY_SERVICE_GROUP = 'rpc-inner'
    def __init__(self, config_file):

        self._app_config = get_config(config_file)

        self._index_updater = IndexUpdater(self._app_config)
        self._seahub_email_sender = SeahubEmailSender(self._app_config)
        self._ldap_syncer = LdapSyncer()
        self._virus_scanner = VirusScanner(os.environ['EVENTS_CONFIG_FILE'])

        self._office_converter = None
        if has_office_tools():
            self._office_converter = OfficeConverter(get_office_converter_conf(self._app_config))

    def _ensure_single_instance(self, sync_client):
        try:
            sync_client.register_service_sync(self.DUMMY_SERVICE, self.DUMMY_SERVICE_GROUP)
        except:
            logging.exception('Another instance is already running')
            do_exit(1)

    def on_ccnet_connected(self, async_client, sync_client):
        self._ensure_single_instance(sync_client)
        if self._office_converter and self._office_converter.is_enabled():
            self._office_converter.register_rpc(async_client)

    def start(self, base):
        logging.info('staring background tasks')
        if self._index_updater.is_enabled():
            self._index_updater.start(base)
        else:
            logging.info('search indexer is disabled')

        if self._seahub_email_sender.is_enabled():
            self._seahub_email_sender.start(base)
        else:
            logging.info('seahub email sender is disabled')

        if self._ldap_syncer.enable_sync():
            self._ldap_syncer.start()
        else:
            logging.info('ldap sync is disabled')

        if self._virus_scanner.is_enabled():
            self._virus_scanner.start()
        else:
            logging.info('virus scan is disabled')

        if self._office_converter and self._office_converter.is_enabled():
            self._office_converter.start()

def is_cluster_enabled():
    cfg = ConfigParser.ConfigParser()
    if 'SEAFILE_CENTRAL_CONF_DIR' in os.environ:
        confdir = os.environ['SEAFILE_CENTRAL_CONF_DIR']
    else:
        confdir = os.environ['SEAFILE_CONF_DIR']
    conf = os.path.join(confdir, 'seafile.conf')
    cfg.read(conf)
    if cfg.has_option('cluster', 'enabled'):
        return cfg.getboolean('cluster', 'enabled')
    else:
        return False

def is_syslog_enabled(config):
    if config.has_option('Syslog', 'enabled'):
        try:
            return config.getboolean('Syslog', 'enabled')
        except ValueError:
            return False
    return False

def main(background_tasks_only=False):
    args = AppArgParser().parse_args()
    app_logger = LogConfigurator(args.loglevel, args.logfile) # pylint: disable=W0612
    if args.logfile:
        logdir = os.path.dirname(os.path.realpath(args.logfile))
        os.environ['SEAFEVENTS_LOG_DIR'] = logdir

    os.environ['EVENTS_CONFIG_FILE'] = os.path.expanduser(args.config_file)

    if args.pidfile:
        write_pidfile(args.pidfile)

    config = get_config(args.config_file)
    enable_audit = is_audit_enabled(config)
    init_message_handlers(enable_audit)

    if is_syslog_enabled(config):
        app_logger.add_syslog_handler()

    events_listener_enabled = True
    background_tasks_enabled = True

    if background_tasks_only:
        events_listener_enabled = False
        background_tasks_enabled = True
    elif is_cluster_enabled():
        events_listener_enabled = True
        background_tasks_enabled = False

    app = App(get_ccnet_dir(), args, events_listener_enabled=events_listener_enabled,
              background_tasks_enabled=background_tasks_enabled)

    app.serve_forever()

def run_background_tasks():
    main(background_tasks_only=True)

if __name__ == '__main__':
    main()
