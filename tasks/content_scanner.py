# coding: UTF-8

import os
import logging
from threading import Thread, Event

from seafevents.utils import get_python_executable, run, parse_bool, parse_interval, get_opt_from_conf_or_env


class ContentScanner(object):
    # 用配置初始化 ContentScanner 对象
    def __init__(self, config):
        self._enabled = False
        self._interval = None
        self._logfile = None
        self._timer = None

        # 调用 _parse_config 方法设置对象的属性。
        self._parse_config(config)

    # 解析配置以确定内容扫描器是否启用，并设置扫描间隔和日志文件路径。
    def _parse_config(self, config):
        section_name = 'CONTENT SCAN'
        if not config.has_section(section_name):
            return

        enabled = get_opt_from_conf_or_env(config, section_name, 'enabled', default=False)
        enabled = parse_bool(enabled)
        logging.debug('content scan enabled: %s', enabled)
        if not enabled:
            return
        self._enabled = True

        default_index_interval = '1d'
        interval = get_opt_from_conf_or_env(config, section_name, 'interval',
                                            default=default_index_interval)
        self._interval = parse_interval(interval, default_index_interval)

        self._logfile = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''), 'content_scan.log')

    # 如果内容扫描器启用，则启动它，并记录一条警告消息，如果它未启用。
    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start content scanner: it is not enabled!')
            return

        logging.info('content scanner is started, interval = %s sec', self._interval)
        ContentScanTimer(self._interval, self._logfile).start()

    # 返回一个布尔值，指示内容扫描器是否启用。
    def is_enabled(self):
        return self._enabled


# 内容扫描定时器
class ContentScanTimer(Thread):

    def __init__(self, interval, log_file):
        Thread.__init__(self)
        self._interval = interval
        self._logfile = log_file
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('start to scan files')
                try:
                    cmd = [
                        get_python_executable(),
                        '-m', 'seafevents.content_scanner.main',
                        '--logfile', self._logfile,
                        '--config-file', os.environ['EVENTS_CONFIG_FILE']
                    ]
                    env = dict(os.environ)
                    run(cmd, env=env)
                except Exception as e:
                    logging.exception('error when scan files: %s', e)

    def cancel(self):
        self.finished.set()
