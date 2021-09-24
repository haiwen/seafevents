import os
import logging
import configparser


def parse_workers(workers, default_workers):
    try:
        workers = int(workers)
    except ValueError:
        logging.warning('invalid workers value "%s"' % workers)
        workers = default_workers

    if workers <= 0 or workers > 5:
        logging.warning('insane workers value "%s"' % workers)
        workers = default_workers

    return workers

def parse_max_size(val, default):
    try:
        val = int(val.lower().rstrip('mb')) * 1024 * 1024
    except:
        logging.exception('xxx:')
        val = default

    return val

def parse_max_pages(val, default):
    try:
        val = int(val)
        if val <= 0:
            val = default
    except:
        val = default

    return val

def get_opt_from_conf_or_env(config, section, key, env_key=None, default=None):
    '''Get option value from events.conf. If not specified in events.conf,
    check the environment variable.

    '''
    try:
        return config.get(section, key)
    except configparser.NoOptionError:
        if env_key is None:
            return default
        else:
            return os.environ.get(env_key.upper(), default)

def parse_bool(v):
    if isinstance(v, bool):
        return v

    v = str(v).lower()

    if v == '1' or v == 'true':
        return True
    else:
        return False

def parse_interval(interval, default):
    if isinstance(interval, (int, int)):
        return interval

    interval = interval.lower()

    unit = 1
    if interval.endswith('s'):
        pass
    elif interval.endswith('m'):
        unit *= 60
    elif interval.endswith('h'):
        unit *= 60 * 60
    elif interval.endswith('d'):
        unit *= 60 * 60 * 24
    else:
        pass

    val = int(interval.rstrip('smhd')) * unit
    if val < 10:
        logging.warning('insane interval %s', val)
        return default
    else:
        return val
