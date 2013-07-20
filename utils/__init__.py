import os
import sys
import logging
import atexit
import tempfile
import ConfigParser
import ccnet
import time
import subprocess

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
        if check_office_tools() and check_python_uno():
            HAS_OFFICE_TOOLS = True
        else:
            HAS_OFFICE_TOOLS = False

    return HAS_OFFICE_TOOLS

def do_exit(code=0):
    logging.info('exit with code %s', code)
    sys.exit(code)

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

def _get_python_executable():
    if sys.executable and os.path.isabs(sys.executable) and os.path.exists(sys.executable):
        return sys.executable

    try_list = [
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

pyexec = None
def get_python_executable():
    '''Find a suitable python executable'''
    global pyexec
    if pyexec is not None:
        return pyexec

    pyexec = _get_python_executable()
    return pyexec

def run(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False):
    '''Run a program and wait it to finish, and return its exit code. The
    standard output of this program is supressed.

    '''
    with open(os.devnull, 'w') as devnull:
        if suppress_stdout:
            stdout = devnull
        else:
            stdout = sys.stdout

        if suppress_stderr:
            stderr = devnull
        else:
            stderr = sys.stderr

        subprocess.Popen(argv,
                         cwd=cwd,
                         stdout=stdout,
                         stderr=stderr,
                         env=env)

class ClientConnector(object):
    RECONNECT_CCNET_INTERVAL = 2

    def __init__(self, client):
        self._client = client

    def connect_daemon_with_retry(self):
        while True:
            logging.info('try to connect to ccnet-server...')
            try:
                self._client.connect_daemon()
                logging.info('connected to ccnet server')
                break
            except ccnet.NetworkError:
                time.sleep(self.RECONNECT_CCNET_INTERVAL)

        return self._client
