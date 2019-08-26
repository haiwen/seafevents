import os
import logging
import configparser
from seafevents.db import init_db_session_class
from seafevents.utils import get_config, do_exit

class AppConfig(object):
    def __init__(self):
        pass

    def set(self, key, value):
        self.key = value

    def get(self, key, default=''):
        if hasattr(self, key):
            return self.__dict__[key]
        else:
            return default

appconfig = AppConfig()

def load_config(config_file):
    config = get_config(config_file)
    load_content_scan_config(config)
    if not appconfig.enabled_scan:
        do_exit(0)

    load_env_config()
    appconfig.session_cls = init_db_session_class(config_file)
    appconfig.seaf_session_cls = init_db_session_class(appconfig.seaf_conf_path, db = 'seafile')

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

def load_content_scan_config(config):
    appconfig.enabled_scan = False
    if config.has_option('CONTENT SCAN', 'enabled'):
        appconfig.enabled_scan = config.getboolean('CONTENT SCAN', 'enabled')

    if appconfig.enabled_scan:
        try:
            suffix_str = config.get('CONTENT SCAN', 'suffix')
            suffix = suffix_str.strip(',')
            appconfig.suffix_list = suffix.split(',') if suffix else []
            size_limit_MB = 20
            if config.has_option('CONTENT SCAN', 'size_limit'):
                size_limit_MB = config.getint('CONTENT SCAN', 'size_limit')
            appconfig.size_limit = size_limit_MB * 1024 * 1024
            appconfig.platform = config.get('CONTENT SCAN', 'platform')
            if appconfig.platform.lower() == 'ali':
                appconfig.key = config.get('CONTENT SCAN', 'key')
                appconfig.key_id = config.get('CONTENT SCAN', 'key_id')
                appconfig.region = 'cn-shanghai'
                if config.has_option('CONTENT SCAN', 'region'):
                    appconfig.region = config.get('CONTENT SCAN', 'region')
            appconfig.thread_num = 3
            if config.has_option('CONTENT SCAN', 'thread_num'):
                appconfig.thread_num = config.getint('CONTENT SCAN', 'thread_num')

            logging.info( '''The files with the following suffixes will be scanned by %s API: [%s], size limit: %dM ''' \
            % (appconfig.platform, suffix, size_limit_MB))
        except Exception as e:
            logging.warning('content scan config error: %s', e)
            appconfig.enabled_scan = False
    else:
        logging.info('Disenabled Content Scan.')

