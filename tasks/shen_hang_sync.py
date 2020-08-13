# coding: UTF-8

import os
import logging
from threading import Thread, Event

from seafevents.utils import get_config, get_python_executable, run
from seafevents.utils.config import parse_bool, parse_interval, get_opt_from_conf_or_env


class ShenHangDeptSyncer(object):
    def __init__(self, config_file):
        self._enabled = False
        self._interval = None
        self._config_file = config_file
        self._logfile = None
        self._timer = None

        config = get_config(config_file)
        self._parse_config(config)

    def _parse_config(self, config):
        section_name = 'SHEN HANG DEPT SYNC'
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
        # seahub_dir
        seahub_dir = os.environ.get('SEAHUB_DIR', '')

        if not seahub_dir:
            logging.critical('seahub_dir is not set')
            raise RuntimeError('seahub_dir is not set')
        if not os.path.exists(seahub_dir):
            logging.critical('seahub_dir %s does not exist' % seahub_dir)
            raise RuntimeError('seahub_dir does not exist')
        self._seahub_dir = seahub_dir
        self._logfile = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''), 'shen-hang-dept-sync.log')

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start shen-hang org syncer: it is not enabled!')
            return

        logging.info(' shen-hang dept syncer is started, interval = %s sec', self._interval)
        ShenHangDeptSyncerTimer(self._interval, self._config_file, self._logfile, self._seahub_dir).start()

    def is_enabled(self):
        return self._enabled


class ShenHangDeptSyncerTimer(Thread):

    def __init__(self, interval, config_file, log_file, seahub_dir):
        Thread.__init__(self)
        self._interval = interval
        self._config_file = config_file
        self._logfile = log_file
        self._seahub_dir = seahub_dir
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('start to sync orgs')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(self._seahub_dir, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'shen_hang_dept_sync',
                    ]
                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=self._seahub_dir, output=fp)
                except Exception as e:
                    logging.exception('error when sync shen hang depts: %s', e)

    def cancel(self):
        self.finished.set()
