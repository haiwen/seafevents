import os
import logging
from seafevents.db import init_db_session_class
from seafevents.utils import get_config

class AppConfig(object):
    def __init__(self):
        pass

    def set(self, key, value):
        self.key = value

    def get(self, key):
        if hasattr(self, key):
            return self.__dict__[key]
        else:
            return ''

appconfig = AppConfig()

def load_config(config_file):
    # seafevent config file
    appconfig.session_cls = init_db_session_class(config_file)
    config = get_config(config_file)

    load_env_config()
    appconfig.seaf_session_cls = init_db_session_class(appconfig.seaf_conf_path, db = 'seafile')
    load_publish_config(config)
    load_statistics_config(config)

def load_env_config():
    # get central config dir
    appconfig.central_confdir = ""
    if 'SEAFILE_CENTRAL_CONF_DIR' in os.environ:
        appconfig.central_confdir = os.environ['SEAFILE_CENTRAL_CONF_DIR']

    # get seafile config path
    appconfig.seaf_conf_path = ""
    if appconfig.central_confdir:
        appconfig.seaf_conf_path = os.path.join(appconfig.central_confdir, 'seafile.conf')
    elif 'SEAFILE_CONF_DIR' in os.environ:
        appconfig.seaf_conf_path = os.path.join(os.environ['SEAFILE_CONF_DIR'], 'seafile.conf')

    # get ccnet config path
    appconfig.ccnet_conf_path = ""
    if appconfig.central_confdir:
        appconfig.ccnet_conf_path = os.path.join(appconfig.central_confdir, 'ccnet.conf')
    elif 'CCNET_CONF_DIR' in os.environ:
        appconfig.ccnet_conf_path = os.path.join(os.environ['CCNET_CONF_DIR'], 'ccnet.conf')

def load_publish_config(config):
    appconfig.publish_enabled = False
    try:
        appconfig.publish_enabled = config.getboolean('EVENTS PUBLISH', 'enabled')
    except:
        # prevent hasn't EVENTS PUBLISH section.
        pass
    if appconfig.publish_enabled:
        try:
            appconfig.publish_mq_type = config.get('EVENTS PUBLISH', 'mq_type').upper()
            if appconfig.publish_mq_type != 'REDIS':
                raise RuntimeError("Unknown database backend: %s" % config['publish_mq_type'])

            appconfig.publish_mq_server = config.get(appconfig.publish_mq_type,
                                                     'server')
            appconfig.publish_mq_port = config.getint(appconfig.publish_mq_type,
                                                      'port')
            # prevent needn't password
            appconfig.publish_mq_password = ""
            if config.has_option(appconfig.publish_mq_type, 'password'):
                appconfig.publish_mq_password = config.get(appconfig.publish_mq_type,
                                                           'password')
        except:
            appconfig.publish_enabled = False

def load_statistics_config(config):
    appconfig.enable_statistics = False
    try:
        if config.has_option('STATISTICS', 'enabled'):
            appconfig.enable_statistics = config.getboolean('STATISTICS', 'enabled')
        if appconfig.enable_statistics:
            appconfig.count_all_file_types = False
            appconfig.type_list = []
            if config.has_option('STATISTICS', 'file_types_to_count'):
                file_types_to_count = config.get('STATISTICS', 'file_types_to_count').replace(' ', '')
                if file_types_to_count == 'all':
                    appconfig.count_all_file_types = True
                else:
                    appconfig.type_list = file_types_to_count.split(',')

            if config.has_option('STATISTICS', 'file_types_count_interval'):
                appconfig.file_types_interval = config.getint('STATISTICS', 'file_types_count_interval') * 3600
            else:
                appconfig.file_types_interval = 24 * 3600
    except Exception as e:
        logging.info(e)
