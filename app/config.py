#coding: utf-8

import os
import logging
import urlparse
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
    appconfig.event_session = init_db_session_class(config_file)
    config = get_config(config_file)

    load_env_config()
    load_publish_config(config)
    load_statistics_config(config)
    load_aliyun_config(config)

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
    appconfig.statistics = AppConfig()
    appconfig.statistics.enabled = False
    try:
        if config.has_option('STATISTICS', 'enabled'):
            appconfig.statistics.enabled = config.getboolean('STATISTICS', 'enabled')
    except Exception as e:
        logging.info(e)

def load_aliyun_config(config):
    appconfig.ali = AppConfig()
    try:
        appconfig.ali.url = config.get('ALIYUN MQ', 'url')
        appconfig.ali.host = urlparse.urlparse(appconfig.ali.url).netloc
        appconfig.ali.producer_id = config.get('ALIYUN MQ', 'producer_id')
        appconfig.ali.topic = config.get('ALIYUN MQ', 'topic')
        appconfig.ali.tag = config.get('ALIYUN MQ', 'tag')
        appconfig.ali.ak = config.get('ALIYUN MQ', 'access_key')
        appconfig.ali.sk = config.get('ALIYUN MQ', 'secret_key')
    except Exception as e:
        logging.error(e)
        appconfig.ali = None
