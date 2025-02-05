import logging
import logging.handlers
import sys

class LogConfigurator(object):

    # 日志配置
    def __init__(self, level, logfile=None):
        # 配置等级和文件
        self._level = self._get_log_level(level)
        self._logfile = logfile

        # 如果文件不存在，需要基础配置；如果已经存在，需要旋转配置
        if logfile is None:
            self._basic_config()
        else:
            self._rotating_config()

    # 该代码设置了一个日志文件轮转处理器。
    # 它以特定格式记录消息到一个文件 (self._logfile)，并每周轮转一次日志文件 (when='W0', interval=1 表示每周)。
    # 日志级别被设置为 self._level，适用于处理器和根记录器。
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

