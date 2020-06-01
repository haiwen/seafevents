import os
import logging
from seafevents.db import init_db_session_class
from seafevents.utils import get_config
from seafevents.utils.config import get_opt_from_conf_or_env

logger = logging.getLogger(__name__)


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

def exception_catch(conf_module):
    """Catch exceptions for functions and log them
    """
    def func_wrapper(func):
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                logger.info('%s module configuration loading failed: %s' % (conf_module, e))
        return wrapper
    return func_wrapper

def load_config(config_file):
    # seafevent config file
    appconfig.session_cls = init_db_session_class(config_file)
    config = get_config(config_file)

    load_env_config()
    appconfig.seaf_session_cls = init_db_session_class(appconfig.seaf_conf_path, db = 'seafile')
    load_publish_config(config)
    load_statistics_config(config)
    load_file_history_config(config)
    load_collab_server_config(config)

@exception_catch('env')
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

@exception_catch('publish')
def load_publish_config(config):
    appconfig.publish_enabled = False
    try:
        appconfig.publish_enabled = config.getboolean('EVENTS PUBLISH', 'enabled')
    except Exception as e:
        # prevent hasn't EVENTS PUBLISH section.
        pass
    if appconfig.publish_enabled:
        try:
            appconfig.publish_mq_type = config.get('EVENTS PUBLISH', 'mq_type').upper()
            if appconfig.publish_mq_type != 'REDIS':
                logger.error("Unknown database backend: %s" % config['publish_mq_type'])
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
        except Exception as e:
            appconfig.publish_enabled = False

@exception_catch('statistics')
def load_statistics_config(config):
    appconfig.enable_statistics = False
    try:
        if config.has_option('STATISTICS', 'enabled'):
            appconfig.enable_statistics = config.getboolean('STATISTICS', 'enabled')
    except Exception as e:
        logger.info(e)

@exception_catch('file history')
def load_file_history_config(config):
    appconfig.fh = AppConfig()
    if config.has_option('FILE HISTORY', 'enabled'):
        appconfig.fh.enabled = config.getboolean('FILE HISTORY', 'enabled')
        if appconfig.fh.enabled:
            appconfig.fh.threshold = int(get_opt_from_conf_or_env(config, 'FILE HISTORY', 'threshold', default=5))
            default_suffix = 'md,txt,doc,docx,xls,xlsx,ppt,pptx'
            appconfig.fh.suffix = get_opt_from_conf_or_env(config, 'FILE HISTORY', 'suffix', default=default_suffix)
            suffix = appconfig.fh.suffix.strip(',')
            appconfig.fh.suffix_list = suffix.split(',') if suffix else []
            logger.info('The file with the following suffix will be recorded into the file history: %s' % suffix)
        else:
            logger.info('Disable File History Features.')
    else:
        appconfig.fh.enabled = True
        appconfig.fh.threshold = 5
        suffix = 'md,txt,doc,docx,xls,xlsx,ppt,pptx'
        appconfig.fh.suffix_list = suffix.split(',')
        logger.info('The file with the following suffix will be recorded into the file history: %s' % suffix)


@exception_catch('collab server')
def load_collab_server_config(config):
    appconfig.enable_collab_server = False
    if not config.has_option('COLLAB_SERVER', 'enabled'):
        return
    appconfig.enable_collab_server = config.getboolean('COLLAB_SERVER', 'enabled')
    if appconfig.enable_collab_server:
        appconfig.collab_server = config.get('COLLAB_SERVER', 'server_url')
        appconfig.collab_key = config.get('COLLAB_SERVER', 'key')
