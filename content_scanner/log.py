import logging
import logging.handlers
import sys

class LogConfigurator(object):
    def __init__(self, level, logfile=None):
        self._level = self._get_log_level(level)
        self._logfile = logfile

        if logfile is None:
            self._basic_config()
        else:
            self._rotating_config()

    def _rotating_config(self):
        '''Rotating log'''
        handler = logging.handlers.TimedRotatingFileHandler(self._logfile, when='W0', interval=1)
        handler.setLevel(self._level)
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
        handler.setFormatter(formatter)

        logging.root.setLevel(self._level)
        logging.root.addHandler(handler)

    def _basic_config(self):
        '''Log to stdout. Mainly for development.'''
        kw = {
            'format': '[%(asctime)s] [%(levelname)s] %(message)s',
            'datefmt': '%m/%d/%Y %H:%M:%S',
            'level': self._level,
            'stream': sys.stdout
        }

        logging.basicConfig(**kw)

    def add_syslog_handler(self):
        handler = logging.handlers.SysLogHandler(address='/dev/log')
        handler.setLevel(self._level)
        formatter = logging.Formatter('seafevents[%(process)d]: %(message)s')
        handler.setFormatter(formatter)
        logging.root.addHandler(handler)

    def _get_log_level(self, level):
        if level == 'debug':
            return logging.DEBUG
        elif level == 'info':
            return logging.INFO
        else:
            return logging.WARNING

