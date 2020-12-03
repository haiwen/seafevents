# -*- coding: utf-8 -*-
import os
import logging
from threading import Thread, Event

from seafevents.utils import get_python_executable, run_and_wait


__all__ = [
    'FileUpdatesSender',
]


class FileUpdatesSender(object):

    def __init__(self):
        self._interval = 300
        self._seahub_dir = None
        self._logfile = None
        self._timer = None

        self._prepare_seahub_dir()
        self._prepare_logfile()

    def _prepare_seahub_dir(self):
        seahub_dir = os.environ.get('SEAHUB_DIR', '')

        if not seahub_dir:
            logging.critical('seahub_dir is not set')
            raise RuntimeError('seahub_dir is not set')
        if not os.path.exists(seahub_dir):
            logging.critical('seahub_dir %s does not exist' % seahub_dir)
            raise RuntimeError('seahub_dir does not exist')

        self._seahub_dir = seahub_dir

    def _prepare_logfile(self):
        log_dir = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''))
        self._logfile = os.path.join(log_dir, 'file_updates_sender.log')

    def start(self):
        logging.info('Start file updates sender, interval = %s sec', self._interval)

        FileUpdatesSenderTimer(self._interval, self._seahub_dir, self._logfile).start()


class FileUpdatesSenderTimer(Thread):

    def __init__(self, interval, seahub_dir, logfile):
        Thread.__init__(self)
        self._interval = interval
        self._seahub_dir = seahub_dir
        self._logfile = logfile
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(self._seahub_dir, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'send_file_updates',
                    ]
                    with open(self._logfile, 'a') as fp:
                        run_and_wait(cmd, cwd=self._seahub_dir, output=fp)
                except Exception as e:
                    logging.exception('send file updates email error: %s', e)

    def cancel(self):
        self.finished.set()
