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
    appconfig.event_session = init_db_session_class(config_file)

    config = get_config(config_file)
    appconfig.publish_enabled = False
    try:
        appconfig.publish_enabled = config.getboolean('EVENTS PUBLISH', 'enabled')
    except:
        # prevent hasn't EVENTS PUBLISH section.
        pass
    if appconfig.publish_enabled:
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

    load_statistics_config(config)

def load_statistics_config(config):
    appconfig.statistics = AppConfig()
    appconfig.statistics.enabled = False
    try:
        if config.has_option('STATISTICS', 'enabled'):
            appconfig.statistics.enabled = config.get('STATISTICS', 'enabled')
    except Exception as e:
        logging.info(e)
