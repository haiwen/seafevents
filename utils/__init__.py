import os
import sys
import logging
import atexit
import configparser
import time
import subprocess

logger = logging.getLogger(__name__)
pyexec = None
HAS_OFFICE_TOOLS = None

def find_in_path(prog):
    if 'win32' in sys.platform:
        sep = ';'
    else:
        sep = ':'

    dirs = os.environ['PATH'].split(sep)
    for d in dirs:
        d = d.strip()
        if d == '':
            continue
        path = os.path.join(d, prog)
        if os.path.exists(path):
            return path

    return None

def check_office_tools():
    return True
    """Check if requried executables can be found in PATH. If not, error
    and exit.

    """
    tools = [
        'soffice',
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
        # if check_office_tools() and check_python_uno():
        if check_office_tools():
            HAS_OFFICE_TOOLS = True
        else:
            HAS_OFFICE_TOOLS = False

    return HAS_OFFICE_TOOLS

def do_exit(code=0):
    logging.info('exit with code %s', code)
    # os._exit: Exit the process with status n, without calling cleanup handlers, flushing stdio buffers, etc
    # sys.exit: This is implemented by raising the SystemExit exception. So only kill the current thread.
    # we need to make sure that the process exits.
    os._exit(code)

def write_pidfile(pidfile):
    pid = os.getpid()
    with open(pidfile, 'w') as fp:
        fp.write(str(pid))

    def remove_pidfile():
        '''Remove the pidfile when exit'''
        logging.info('remove pidfile %s' % pidfile)
        try:
            os.remove(pidfile)
        except Exception as e:
            pass

    atexit.register(remove_pidfile)

def _get_python_executable():
    if sys.executable and os.path.isabs(sys.executable) and os.path.exists(sys.executable):
        return sys.executable

    try_list = [
        'python3.7',
        'python37',
        'python2.7',
        'python27',
        'python2.6',
        'python26',
    ]

    for prog in try_list:
        path = find_in_path(prog)
        if path is not None:
            return path

    path = os.environ.get('PYTHON', 'python')

    return path

def get_python_executable():
    '''Find a suitable python executable'''
    global pyexec
    if pyexec is not None:
        return pyexec

    pyexec = _get_python_executable()
    return pyexec

def run(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False, output=None):
    def quote(args):
        return ' '.join([ '"%s"' % arg for arg in args])

    cmdline = quote(argv)
    if cwd:
        logger.debug('Running command: %s, cwd = %s', cmdline, cwd)
    else:
        logger.debug('Running command: %s', cmdline)

    with open(os.devnull, 'w') as devnull:
        kwargs = dict(cwd=cwd, env=env, shell=True)

        if suppress_stdout:
            kwargs['stdout'] = devnull
        if suppress_stderr:
            kwargs['stderr'] = devnull

        if output:
            kwargs['stdout'] = output
            kwargs['stderr'] = output

        return subprocess.Popen(cmdline, **kwargs)

def run_and_wait(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False, output=None):
    proc = run(argv, cwd, env, suppress_stdout, suppress_stderr, output)
    return proc.wait()


def get_config(config_file):
    config = configparser.ConfigParser()
    try:
        config.read(config_file)
    except Exception as e:
        logging.critical('failed to read config file %s', e)
        do_exit(1)

    return config

def get_env_without_thirdpart():
    """ because unoconv.py be executed by python3, some same packpage in seahub/thirdpart folder.
        so need remove thirdpart from PYTHONPATH env.
    """
    envs = dict(os.environ)
    python_envs = envs.get('PYTHONPATH').split(':')
    envs['PYTHONPATH'] = ':'.join([x for x in python_envs if 'thirdpart' not in x])
    return envs

