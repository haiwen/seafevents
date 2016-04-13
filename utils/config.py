import os
import logging
import ConfigParser
import tempfile

from seafevents.utils import has_office_tools

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
    except ConfigParser.NoOptionError:
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
    if isinstance(interval, (int, long)):
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

def get_office_converter_conf(config):
    '''Parse search related options from events.conf'''

    if not has_office_tools():
        logging.debug('office converter is not enabled because libreoffice or python-uno is not found')
        return dict(enabled=False)

    section_name = 'OFFICE CONVERTER'
    key_enabled = 'enabled'

    key_outputdir = 'outputdir'
    default_outputdir = os.path.join(tempfile.gettempdir(), 'seafile-office-output')

    key_workers = 'workers'
    default_workers = 2

    key_max_pages = 'max-pages'
    default_max_pages = 50

    key_max_size = 'max-size'
    default_max_size = 2 * 1024 * 1024

    d = { 'enabled': False }
    if not config.has_section(section_name):
        return d

    def get_option(key, default=None):
        try:
            value = config.get(section_name, key)
        except ConfigParser.NoOptionError:
            value = default

        return value

    enabled = get_option(key_enabled, default=False)
    enabled = parse_bool(enabled)

    d['enabled'] = enabled
    logging.debug('office enabled: %s', enabled)

    if not enabled:
        return d

    # [ outputdir ]
    outputdir = get_option(key_outputdir, default=default_outputdir)
    if not os.path.exists(outputdir):
        os.mkdir(outputdir)

    # [ workers ]
    workers = get_option(key_workers, default=default_workers)
    workers = parse_workers(workers, default_workers)


    # [ max_size ]
    max_size = get_option(key_max_size, default=default_max_size)
    if max_size != default_max_size:
        max_size = parse_max_size(max_size, default=default_max_size)

    # [ max_pages ]
    max_pages = get_option(key_max_pages, default=default_max_pages)
    if max_pages != default_max_pages:
        max_pages = parse_max_pages(max_pages, default=default_max_pages)

    logging.debug('office convert workers: %s', workers)
    logging.debug('office outputdir: %s', outputdir)
    logging.debug('office convert max pages: %s', max_pages)
    logging.debug('office convert max size: %s MB', max_size / 1024 / 1024)

    d['outputdir'] = outputdir
    d['workers'] = workers
    d['max_pages'] = max_pages
    d['max_size'] = max_size

    return d
