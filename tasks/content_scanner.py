#coding: UTF-8

import os
import logging
import configparser

from ccnet.async import Timer
from seafevents.utils import get_config, get_python_executable, run
from seafevents.utils.config import parse_bool, parse_interval, get_opt_from_conf_or_env

class ContentScanner(object):
    def __init__(self, config_file):
        self._enabled = False
        self._interval = None
        self._config_file = config_file
        self._logfile = None
        self._timer = None

        config = get_config(config_file)
        self._parse_config(config)

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

    def start(self, ev_base):
        if not self.is_enabled():
            logging.warning('Can not start content scanner: it is not enabled!')
            return

        logging.info('content scanner is started, interval = %s sec', self._interval)
        self._timer = ContentScanTimer(ev_base, self._interval,
                                       self._config_file, self._logfile)

    def is_enabled(self):
        return self._enabled

class ContentScanTimer(Timer):
    def __init__(self, ev_base, timeout, config_file, log_file):
        Timer.__init__(self, ev_base, timeout)
        self._config_file = config_file
        self._logfile = log_file

    def callback(self):
        self.scan_files()

    def scan_files(self):
        logging.info('start to scan files')
        try:
            cmd = [
                get_python_executable(),
                '-m', 'seafevents.content_scanner.main',
                '--logfile', self._logfile,
                '--config-file', self._config_file
            ]
            env = dict(os.environ)
            run(cmd, env=env)
        except Exception as e:
            logging.exception('error when scan files: %s', e)
