import os
import logging
from threading import Thread, Event

from seafevents.utils import get_python_executable, run, get_config
from seafevents.utils.config import parse_bool, parse_interval, get_opt_from_conf_or_env



class RepoOldFileAutoDelScanner(object):
    def __init__(self, config_file):
        self._enabled = False

        self._interval = None
        self._seahubdir = None
        self._logfile = None

        self._timer = None

        config = get_config(config_file)
        self._parse_config(config)
        self._prepare_logdir()
        self._prepare_seahub_dir()

    def _prepare_seahub_dir(self):
        seahub_dir = os.environ.get('SEAHUB_DIR', '')

        if not seahub_dir:
            logging.critical('seahub_dir is not set')
            raise RuntimeError('seahub_dir is not set')
        if not os.path.exists(seahub_dir):
            logging.critical('seahub_dir %s does not exist' % seahub_dir)
            raise RuntimeError('seahub_dir does not exist')


        self._seahubdir = seahub_dir

    def _prepare_logdir(self):
        logdir = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'repo_old_file_auto_del_scan.log')

    def _parse_config(self, config):
        '''Parse send email related options from events.conf'''
        section_name = 'AUTO DELETION'
        key_enabled = 'enabled'

        key_interval = 'interval'
        default_interval = 60 * 60 * 24  # 1 day

        if not config.has_section(section_name):
            return

        # [ enabled ]
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)

        if not enabled:
            return

        self._enabled = True
        interval = get_opt_from_conf_or_env(config, section_name, key_interval,
                                               default=default_interval)
        self._interval = parse_interval(interval, default_interval)


    def start(self):
        if not self.is_enabled():
            logging.warning('Can not scan repo old files auto del days: it is not enabled!')
            return

        RepoOldFileAutoDelScannerTimer(self._interval, self._seahubdir, self._logfile).start()

    def is_enabled(self):
        return self._enabled


class RepoOldFileAutoDelScannerTimer(Thread):

    def __init__(self, interval, seahubdir, logfile):

        Thread.__init__(self)
        self._interval = interval
        self._seahubdir = seahubdir
        self._logfile = logfile

        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('start scan repo old files auto del days')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(self._seahubdir, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'scan_repo_auto_delete',
                    ]
                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=self._seahubdir, output=fp)
                except Exception as e:
                    logging.exception('error when scan repo old files auto del days: %s', e)

    def cancel(self):
        self.finished.set()
