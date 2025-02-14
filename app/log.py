import os
import logging
import logging.handlers
import sys

# 日志配置
class LogConfigurator(object):
    def __init__(self, level, logfile=None):
        self._level = self._get_log_level(level)
        self._logfile = logfile

        if logfile is None:
            self._basic_config()
        else:
            self._rotating_config()

    # 轮转日志文件的配置。它创建了一个 TimedRotatingFileHandler，：
    def _rotating_config(self):
        '''Rotating log'''
        # 将日志写入由 self._logfile 指定的文件中
        # 每周轮转一次日志文件（when='W0'），每周间隔为 1 周（interval=1）
        handler = logging.handlers.TimedRotatingFileHandler(self._logfile, when='W0', interval=1)
        # 将日志级别设置为 self._level
        handler.setLevel(self._level)
        # 使用特定的格式格式化日志消息，包括时间戳、日志级别、日志器名称、行号和日志消息。
        # 配置的处理程序然后被添加到根日志器中。
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(name)s:%(lineno)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)

        logging.root.setLevel(self._level)
        logging.root.addHandler(handler)

    def _basic_config(self):
        '''Log to stdout. Mainly for development.'''
        kw = {
            'format': '[seafevents] [%(asctime)s] [%(levelname)s] %(name)s:%(lineno)s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
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

    def add_face_recognition_logger(self, logfile):
        logger = logging.getLogger('face_recognition')
        
        seafile_log_to_stdout = os.getenv('SEAFILE_LOG_TO_STDOUT', 'false') == 'true'
        if seafile_log_to_stdout:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('[seafevents] [%(asctime)s] [%(levelname)s] %(name)s:%(lineno)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        else:
            handler = logging.handlers.TimedRotatingFileHandler(logfile, when='W0', interval=1)
            formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(name)s:%(lineno)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        handler.setLevel(self._level)
        handler.setFormatter(formatter)

        logger.setLevel(self._level)
        logger.addHandler(handler)
        logger.propagate = False
