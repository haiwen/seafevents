import os
import sys
import atexit
import signal
import logging

from twisted.internet import ssl, reactor
from twisted.web import server
from twisted.python import usage, log

import api
from task_manager import task_manager

class Options(usage.Options):
    optFlags = [
        ['https', 'T', 'use http or not'],
    ]

    optParameters = [
        ["port", "p", 9082, "the port to listen on", int],
        ["loglevel", "l", "debug", "log level"],
        ["logfile", "f", "-", "log file"],
        ["pdfdir", "d", "/tmp/seafile-pdf-dir", "directory to store converted pdf files"],
        ["htmldir", "s", "/tmp/seafile-html-dir", "directory to store converted html files"],
        ['privkey', 'K', None, 'the private key for https'],
        ['pemfile', 'M', None, 'the pem file for https'],
        ['pidfile', 'P', None, "the pidfile"],
    ]

    def postOptions(self):
        # validate loglevel
        levels = ('debug', 'info', 'warning')
        loglevel = self['loglevel'].lower()

        if loglevel not in levels:
            raise usage.UsageError, "invalid loglevel '%s'" % loglevel

        self['loglevel'] = loglevel

        # validate port
        port = self['port']
        if port < 0 or port > 65535:
            raise usage.UsageError, "invalid port '%s'" % port

        # validate https
        if self['https']:
            privkey = self['privkey']
            pemfile = self['pemfile']
            if not privkey:
                raise usage.UsageError, 'you must specify the SSL privkey when use https'
            if not pemfile:
                raise usage.UsageError, 'you must specify the SSL pemfile when use https'

            if not os.path.exists(privkey):
                raise usage.UsageError, 'privkey %s does not exist' % privkey
            if not os.path.exists(pemfile):
                raise usage.UsageError, 'pemfile %s does not exist' % pemfile

def parse_options():
    config = Options()
    # Parse command line options
    try:
        config.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.exit(1)
    else:
        return config

def init_twisted_logging():
    """Ask twisted to use standard logging module"""
    observer = log.PythonLoggingObserver()
    observer.start()

def init_logging(config):
    init_twisted_logging()

    level = config['loglevel']
    logfile = config['logfile']

    if level == 'debug':
        level = logging.DEBUG
    elif level == 'info':
        level = logging.INFO
    elif level == 'warning':
        level = logging.WARNING

    kwargs = {
        'format': '[%(asctime)s] %(message)s',
        'datefmt': '%m/%d/%Y %H:%M:%S',
        'level': level
    }

    if logfile == '-':
        kwargs['stream'] = sys.stdout
    else:
        kwargs['filename'] = logfile

    logging.basicConfig(**kwargs)

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

def check_tools():
    """Check if libreoffice/pdf2htmlEX can be found in PATH. If not, error and
    exit.

    """
    tools = [
        'soffice',
        'pdf2htmlEX',
    ]

    for prog in tools:
        if find_in_path(prog) is None:
            sys.stderr.write("Can't find the %s executable in PATH\n" % prog)
            sys.exit(1)

def sighandler(signum, frame):
    """Kill itself when Ctrl-C, for development"""
    os.kill(os.getpid(), signal.SIGKILL)

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

if __name__ == '__main__':
    config = parse_options()
    if config['pidfile']:
        write_pidfile(config['pidfile'])

    check_tools()
    init_logging(config)

    signal.signal(signal.SIGINT, sighandler)

    # Init task manager and run it in a seperate thread
    task_manager.init(num_workers=2, pdf_dir=config['pdfdir'], html_dir=config['htmldir'])

    task_manager.run()

    # Serve pdf files on pdf_dir
    html_dir = api.StaticFile(config['htmldir'])
    api.root.putChild('html', html_dir)

    # Start twisted event loop
    if config['https']:
        context_factory = ssl.DefaultOpenSSLContextFactory(config['privkey'], config['pemfile'])
        reactor.listenSSL(config['port'], server.Site(api.root), context_factory)
    else:
        reactor.listenTCP(config['port'], server.Site(api.root))

    logging.info("starts to serve")
    reactor.run()