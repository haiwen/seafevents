import os
import libevent
import ConfigParser
import logging

from sqlalchemy.ext.declarative import declarative_base

import ccnet
from ccnet.async import AsyncClient

from seafevents.db import init_db_session_class
from seafevents.app.config import appconfig, AppConfig
from seafevents.app.signal_handler import SignalHandler
from seafevents.app.mq_listener import EventsMQListener
from seafevents.events_publisher.events_publisher import events_publisher
from seafevents.utils.config import get_office_converter_conf
from seafevents.utils import do_exit, ClientConnector, has_office_tools
from seafevents.tasks import IndexUpdater, SeahubEmailSender, LdapSyncer,\
        VirusScanner, Statistics, UpdateLoginRecordTask

if has_office_tools():
    from seafevents.office_converter import OfficeConverter

Base = declarative_base()


class App(object):
    def __init__(self, ccnet_dir, args, events_listener_enabled=True, background_tasks_enabled=True):
        self._ccnet_dir = ccnet_dir
        self._central_config_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR')
        self._args = args
        self._events_listener_enabled = events_listener_enabled
        self._bg_tasks_enabled = background_tasks_enabled
        try:
            self.load_config(appconfig, args.config_file)
        except Exception as e:
            logging.error('Error loading seafevents config. Detial: %s' % e)
            raise RuntimeError("Error loading seafevents config")

        self._events_listener = None
        if self._events_listener_enabled:
            self._events_listener = EventsMQListener(self._args.config_file)

        if appconfig.publish_enabled:
            events_publisher.init()

        self._bg_tasks = None
        if self._bg_tasks_enabled:
            self._bg_tasks = BackgroundTasks(args.config_file)
        else:
            self.update_login_record_task = UpdateLoginRecordTask()

        self._ccnet_session = None
        self._sync_client = None

        self._evbase = libevent.Base() #pylint: disable=E1101
        self._sighandler = SignalHandler(self._evbase)


    def load_config(self, appconfig, config_file):
        appconfig.event_session = init_db_session_class(config_file)

        config = ConfigParser.ConfigParser()
        config.read(config_file)
        appconfig.publish_enabled = False
        try:
            appconfig.publish_enabled = config.getboolean('EVENTS PUBLISH', 'enabled')
        except:
            # prevent hasn't EVENTS PUBLISH section.
            pass
        if appconfig.publish_enabled:
            appconfig.publish_mq_type = config.get('EVENTS PUBLISH', 'mq_type').upper()
            if appconfig.publish_mq_type != 'REDIS':
                raise RuntimeError("Unknown database backend: %s" % self.config['publish_mq_type'])

            appconfig.publish_mq_server = config.get(appconfig.publish_mq_type,
                                                     'server')
            appconfig.publish_mq_port = config.getint(appconfig.publish_mq_type,
                                                      'port')
            # prevent needn't password
            appconfig.publish_mq_password = ""
            if config.has_option(appconfig.publish_mq_type, 'password'):
                appconfig.publish_mq_password = config.get(appconfig.publish_mq_type,
                                                           'password')

        self.load_statistics_config(config)

    def load_statistics_config(self, config):
        appconfig.statistics = AppConfig()
        appconfig.statistics.enabled = False
        try:
            if config.has_option('STATISTICS', 'enabled'):
                appconfig.statistics.enabled = config.get('STATISTICS', 'enabled')
        except Exception as e:
            logging.info(e)

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
            try:
                self._sync_client.register_service_sync('seafevents-events-dummy-service', 'rpc-inner')
            except:
                logging.exception('Another instance is already running')
                do_exit(1)
            self._events_listener.start(self._ccnet_session)

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
        else:
            self.update_login_record_task.start()

        while True:
            self._serve()


class BackgroundTasks(object):
    DUMMY_SERVICE = 'seafevents-background-tasks-dummy-service'
    DUMMY_SERVICE_GROUP = 'rpc-inner'
    def __init__(self, config_file):

        self._app_config = get_config(config_file)

        self._index_updater = IndexUpdater(self._app_config)
        self._seahub_email_sender = SeahubEmailSender(self._app_config)
        self._ldap_syncer = LdapSyncer()
        self._virus_scanner = VirusScanner(os.environ['EVENTS_CONFIG_FILE'])
        self._statistics = Statistics(os.environ['EVENTS_CONFIG_FILE'])

        self._office_converter = None
        if has_office_tools():
            self._office_converter = OfficeConverter(get_office_converter_conf(self._app_config))

        if appconfig.statistics.enabled:
            self.update_login_record_task = UpdateLoginRecordTask()
        else:
            logging.info('login record updater disabled')

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

        if hasattr(self, 'update_login_record_task'):
            self.update_login_record_task.start()

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

        if self._statistics.is_enabled():
            self._statistics.start()
        else:
            logging.info('data statistics is disabled')

        if self._office_converter and self._office_converter.is_enabled():
            self._office_converter.start()


def get_config(config_file):
    config = ConfigParser.ConfigParser()
    try:
        config.read(config_file)
    except Exception, e:
        logging.critical('failed to read config file %s', e)
        do_exit(1)

    return config

