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

def parse_interval(interval):
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
        logging.critical('invalid index interval "%s"' % interval)
        do_exit(1)

    return int(interval.rstrip('smhd')) * unit

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
    key_workers = 'workers'

    default_outputdir = os.path.join(tempfile.gettempdir(),
                                     'seafile-office-output')
    default_workers = 2

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
    logging.info('office enabled: %s', enabled)

    if enabled:
        outputdir = get_option(key_outputdir, default=default_outputdir)

        if not os.path.exists(outputdir):
            os.mkdir(outputdir)

        d['outputdir'] = outputdir

        logging.info('office outputdir: %s', outputdir)

    workers = get_option(key_workers, default=default_workers)
    workers = parse_workers(workers, default_workers)

    d['workers'] = workers
    logging.info('office convert workers: %s', workers)

    return d

def get_seafes_conf(config):
    '''Parse search related options from events.conf'''
    section_name = 'INDEX FILES'
    key_enabled = 'enabled'
    key_seafesdir = 'seafesdir'
    key_index_logfile = 'logfile'
    key_index_interval = 'interval'
    key_index_office_pdf = 'index_office_pdf'

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
    logging.info('seafes enabled: %s', enabled)

    d['enabled'] = enabled
    if not enabled:
        return d

    # [ seafesdir ]
    seafesdir = get_option_from_conf_or_env(key_seafesdir, 'SEAFES_DIR', None)
    if not seafesdir:
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
    interval = config.get(section_name, key_index_interval).lower()
    val = parse_interval(interval)
    if val < 0:
        logging.critical('invalid index interval %s' % val)
        do_exit(1)
    elif val < 60:
        logging.warning('index interval too short')

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

    logging.info('seafes dir: %s', seafesdir)
    logging.info('seafes logfile: %s', index_logfile)
    logging.info('seafes index interval: %s', interval)
    logging.info('seafes index office/pdf: %s', index_office_pdf)

    d['interval'] = val
    d['seafesdir'] = seafesdir
    d['index_office_pdf'] = index_office_pdf
    d['logfile'] = index_logfile

    return d
