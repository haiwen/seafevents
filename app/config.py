import os
import sys
import logging
import configparser

logger = logging.getLogger(__name__)

# SEAHUB_DIR
SEAHUB_DIR = os.environ.get('SEAHUB_DIR', '')
if not SEAHUB_DIR:
    logging.critical('SEAHUB_DIR is not set')
    raise RuntimeError('SEAHUB_DIR is not set')
if not os.path.exists(SEAHUB_DIR):
    logging.critical('SEAHUB_DIR %s does not exist' % SEAHUB_DIR)
    raise RuntimeError('SEAHUB_DIR does not exist.')
sys.path.insert(0, SEAHUB_DIR)

try:
    import seahub.settings as seahub_settings
    TIME_ZONE = getattr(seahub_settings, 'TIME_ZONE', 'UTC')
    ENABLE_WORK_WEIXIN = getattr(seahub_settings, 'ENABLE_WORK_WEIXIN', False)
    DTABLE_WEB_SERVER = getattr(seahub_settings, 'DTABLE_WEB_SERVER', None)
    SEATABLE_EX_PROPS_BASE_API_TOKEN = getattr(seahub_settings, 'SEATABLE_EX_PROPS_BASE_API_TOKEN', None)
    EX_PROPS_TABLE = getattr(seahub_settings, 'EX_PROPS_TABLE', None)
except ImportError:
    logger.critical("Can not import seahub settings.")
    raise RuntimeError("Can not import seahub settings.")


def get_config(config_file):
    config = configparser.ConfigParser()
    try:
        config.read(config_file)
    except Exception as e:
        logger.critical("Failed to read config file %s: %s" % (config_file, e))
        raise RuntimeError("Failed to read config file %s: %s" % (config_file, e))

    return config


def is_cluster_enabled(seafile_config):
    if seafile_config.has_option('cluster', 'enabled'):
        return seafile_config.getboolean('cluster', 'enabled')
    else:
        return False


def is_syslog_enabled(config):
    if config.has_option('Syslog', 'enabled'):
        try:
            return config.getboolean('Syslog', 'enabled')
        except ValueError:
            return False
    return False


def is_repo_auto_del_enabled(config):
    if config.has_option('AUTO DELETION', 'enabled'):
        try:
            return config.getboolean('AUTO DELETION', 'enabled')
        except ValueError:
            return False
    return False


def is_search_enabled(config):
    if config.has_option('INDEX FILES', 'enabled'):
        try:
            return config.getboolean('INDEX FILES', 'enabled')
        except ValueError:
            return False
    return False


def is_audit_enabled(config):
    if config.has_option('Audit', 'enabled'):
        try:
            return config.getboolean('Audit', 'enabled')
        except ValueError:
            return False
    elif config.has_option('AUDIT', 'enabled'):
        try:
            return config.getboolean('AUDIT', 'enabled')
        except ValueError:
            return False
    return False
