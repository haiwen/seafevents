import os
import sys
import logging
import atexit
import tempfile
import ConfigParser

def find_in_path(prog):
    '''Test whether prog exists in system path'''
    dirs = os.environ['PATH'].split(':')
    for d in dirs:
        if d == '':
            continue
        path = os.path.join(d, prog)
        if os.path.exists(path):
            return path

    return None

def check_office_tools():
    """Check if requried executables can be found in PATH. If not, error
    and exit.

    """
    tools = [
        'soffice',
        'pdf2htmlEX',
    ]

    for prog in tools:
        if find_in_path(prog) is None:
            logging.debug("Can't find the %s executable in PATH\n" % prog)
            return False

    return True

def check_python_uno():
    try:
        import uno
        del uno
    except ImportError:
        return False
    else:
        return True

HAS_OFFICE_TOOLS = None
def has_office_tools():
    '''Test whether office converter can be enabled by checking the
    libreoffice executable and python-uno library.

    python-uno has an known bug about monkey patching the "__import__" builtin
    function, which can make django fail to start. So we use a function to
    defer the test of uno import until it is really need (which is after
    django is started) to avoid the bug.

    See https://code.djangoproject.com/ticket/11098

    '''

    global HAS_OFFICE_TOOLS
    if HAS_OFFICE_TOOLS is None:
        if check_python_uno() and check_python_uno():
            HAS_OFFICE_TOOLS = True
        else:
            HAS_OFFICE_TOOLS = False

    return HAS_OFFICE_TOOLS

def do_exit(code=0):
    if has_office_tools():
        from office_converter import office_converter
        office_converter.stop()

    logging.info('exit with code %s', code)
    sys.exit(code)

def parse_bool(v):
    if isinstance(v, bool):
        return v

    v = str(v).lower()

    if v == '1' or v == 'true':
        return True
    else:
        return False

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

def parse_interval(interval, default):
    if isinstance(interval, int):
        return interval

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
    if val < 30:
        logging.warning('insane interval %s', val)
        return default
    else:
        return val

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

def write_pidfile(pidfile):
    pid = os.getpid()
    with open(pidfile, 'w') as fp:
        fp.write(str(pid))

    def remove_pidfile():
        '''Remove the pidfile when exit'''
        logging.info('remove pidfile %s' % pidfile)
        try:
            os.remove(pidfile)
        except:
            pass

    atexit.register(remove_pidfile)

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

    d = {}
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

    if enabled:
        outputdir = get_option(key_outputdir, default=default_outputdir)

        if not os.path.exists(outputdir):
            os.mkdir(outputdir)

        d['outputdir'] = outputdir

        logging.debug('office outputdir: %s', outputdir)

    workers = get_option(key_workers, default=default_workers)
    workers = parse_workers(workers, default_workers)

    d['workers'] = workers
    logging.debug('office convert workers: %s', workers)

    max_size = get_option(key_max_size, default=default_max_size)
    if max_size != default_max_size:
        max_size = parse_max_size(max_size, default=default_max_size)

    max_pages = get_option(key_max_pages, default=default_max_pages)
    if max_pages != default_max_pages:
        max_pages = parse_max_pages(max_pages, default=default_max_pages)

    logging.debug('office convert max pages: %s', max_pages)
    logging.debug('office convert max size: %s MB', max_size / 1024 / 1024)

    d['max_pages'] = max_pages
    d['max_size'] = max_size

    return d

def get_seafes_conf(config):
    '''Parse search related options from events.conf'''
    section_name = 'INDEX FILES'
    key_enabled = 'enabled'
    key_seafesdir = 'seafesdir'
    key_index_logfile = 'logfile'
    key_index_interval = 'interval'
    key_index_office_pdf = 'index_office_pdf'

    default_index_interval = 30 * 60 # 30 min

    d = {}
    if not config.has_section(section_name):
        return d

    def get_option_from_conf_or_env(key, env_key=None, default=None):
        '''Get option value from events.conf. If not specified in events.conf,
        check the environment variable.

        '''
        try:
            return config.get(section_name, key)
        except ConfigParser.NoOptionError:
            if env_key is None:
                return default
            else:
                return os.environ.get(env_key.upper(), default)

    # [ enabled ]
    enabled = get_option_from_conf_or_env(key_enabled, default=False)
    enabled = parse_bool(enabled)
    logging.debug('seafes enabled: %s', enabled)

    d['enabled'] = enabled
    if not enabled:
        return d

    # [ seafesdir ]
    seafesdir = get_option_from_conf_or_env(key_seafesdir, 'SEAFES_DIR', None)
    if not seafesdir:
        logging.critical('seafesdir is not set')
        raise RuntimeError('seafesdir is not set')
    if not os.path.exists(seafesdir):
        logging.critical('seafesdir %s does not exist' % seafesdir)
        do_exit(1)

    # [ index logfile ]

    # default index file is 'index.log' in the seafes dir
    default_index_logfile = os.path.join(seafesdir, 'index.log')
    index_logfile = get_option_from_conf_or_env (key_index_logfile,
                                                 'SEAFES_INDEX_LOGFILE',
                                                 default=default_index_logfile)

    # [ index interval ]
    interval = get_option_from_conf_or_env(key_index_interval, default=default_index_interval).lower()
    val = parse_interval(interval, default_index_interval)

    # [ index office/pdf files  ]
    index_office_pdf = False
    try:
        index_office_pdf = config.get(section_name, key_index_office_pdf)
    except ConfigParser.NoOptionError, ConfigParser.NoSectionError:
        pass
    else:
        index_office_pdf = index_office_pdf.lower()
        if index_office_pdf == 'true' or index_office_pdf == '1':
            index_office_pdf = True

    logging.debug('seafes dir: %s', seafesdir)
    logging.debug('seafes logfile: %s', index_logfile)
    logging.debug('seafes index interval: %s sec', interval)
    logging.debug('seafes index office/pdf: %s', index_office_pdf)

    d['interval'] = val
    d['seafesdir'] = seafesdir
    d['index_office_pdf'] = index_office_pdf
    d['logfile'] = index_logfile

    return d

def get_seahub_email_conf(config):
    '''Parse send email related options from events.conf'''
    section_name = 'SEAHUB EMAIL'
    key_enabled = 'enabled'
    key_seahubdir = 'seahubdir'

    key_interval = 'interval'
    default_interval = 30 * 60  # 30min

    d = {}
    if not config.has_section(section_name):
        return d

    def get_option_from_conf_or_env(key, env_key=None, default=None):
        '''Get option value from events.conf. If not specified in events.conf,
        check the environment variable.

        '''
        try:
            return config.get(section_name, key)
        except ConfigParser.NoOptionError:
            if env_key is None:
                return default
            else:
                return os.environ.get(env_key.upper(), default)

    # [ enabled ]
    enabled = get_option_from_conf_or_env(key_enabled, default=False)
    enabled = parse_bool(enabled)
    logging.debug('seahub email enabled: %s', enabled)

    d['enabled'] = enabled
    if not enabled:
        return d

    # seahubdir
    seahubdir = get_option_from_conf_or_env(key_seahubdir, 'SEAHUB_DIR')
    if not seahubdir:
        logging.critical('seahubdir is not set')
        raise RuntimeError('seahubdir is not set')
    if not os.path.exists(seahubdir):
        logging.critical('seahubdir %s does not exist' % seahubdir)
        do_exit(1)

    d['seahubdir'] = seahubdir
    logging.debug('seahub dir: %s', seahubdir)

    # [ send email interval ]
    interval = get_option_from_conf_or_env(key_interval, default=default_interval).lower()
    interval = parse_interval(interval, default_interval)

    logging.debug('send seahub email interval: %s sec', interval)

    d['interval'] = interval

    return d