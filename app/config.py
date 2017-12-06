import logging
import urlparse
import ConfigParser

from urllib import quote_plus
from seafevents.utils.config import get_boolean_from_conf, get_opt_from_conf_or_env, get_int_from_conf


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
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    appconfig.events_config_file = config_file
    appconfig.publish_enabled = get_boolean_from_conf(config, 'EVENTS PUBLISH', 'enabled', False)

    if appconfig.publish_enabled:
        appconfig.publish_mq_type = get_opt_from_conf_or_env(config, 'EVENTS PUBLISH', 'mq_type').upper()
        if appconfig.publish_mq_type != 'REDIS':
            raise RuntimeError("Unknown database backend: %s" % appconfig.publish_mq_type)

        appconfig.publish_mq_server = config.get(appconfig.publish_mq_type,
                                                 'server')
        appconfig.publish_mq_port = config.getint(appconfig.publish_mq_type,
                                                  'port')
        # prevent needn't password
        appconfig.publish_mq_password = ""
        if config.has_option(appconfig.publish_mq_type, 'password'):
            appconfig.publish_mq_password = config.get(appconfig.publish_mq_type,
                                                       'password')
    else:
        logging.info('Disenabled Publish Features.')

    appconfig.engine = get_opt_from_conf_or_env(config, 'DATABASE', 'type', default='')
    if appconfig.engine == 'mysql':
        host = get_opt_from_conf_or_env(config, 'DATABASE', 'host', default='localhost').lower()
        port = get_int_from_conf(config, 'DATABASE', 'port', default=3306)

        username = get_opt_from_conf_or_env(config, 'DATABASE', 'username')
        passwd = get_opt_from_conf_or_env(config, 'DATABASE', 'password')
        dbname = get_opt_from_conf_or_env(config, 'DATABASE', 'name')
        appconfig.db_url = "mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8" % (username, quote_plus(passwd), host, port, dbname)
    else:
        logging.info('Seafile does not use mysql db, disable statistics.')

    _load_file_history_config(config)
    _load_aliyun_config(config)

def _load_file_history_config(config):
    appconfig.fh = AppConfig()
    appconfig.fh.enabled = get_boolean_from_conf(config, 'FILE HISTORY', 'enabled', False)
    if appconfig.fh.enabled:
        appconfig.fh.suffix = get_opt_from_conf_or_env(config, 'FILE HISTORY', 'suffix')
    else:
        logging.info('Disenabled File History Features.')

def _load_aliyun_config(config):
    appconfig.ali = AppConfig()
    appconfig.ali.url = get_opt_from_conf_or_env(config, 'Aliyun MQ', 'url')
    appconfig.ali.host = urlparse.urlparse(appconfig.ali.url).netloc
    appconfig.ali.producer_id = get_opt_from_conf_or_env(config, 'Aliyun MQ', 'producer_id')
    appconfig.ali.topic = get_opt_from_conf_or_env(config, 'Aliyun MQ', 'topic')
    appconfig.ali.tag = get_opt_from_conf_or_env(config, 'Aliyun MQ', 'tag')
    appconfig.ali.ak = get_opt_from_conf_or_env(config, 'Aliyun MQ', 'access_key')
    appconfig.ali.sk = get_opt_from_conf_or_env(config, 'Aliyun MQ', 'secret_key')

