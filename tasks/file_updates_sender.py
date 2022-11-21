# -*- coding: utf-8 -*-
import os
import logging
from threading import Thread, Event

from seafevents.utils import get_python_executable, run_and_wait
from seafevents.app.config import SEAHUB_DIR


__all__ = [
    'FileUpdatesSender',
]


class FileUpdatesSender(object):

    def __init__(self):
        self._interval = 300
        self._logfile = None
        self._timer = None

        self._prepare_logfile()

    def _prepare_logfile(self):
        log_dir = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''))
        self._logfile = os.path.join(log_dir, 'file_updates_sender.log')

    def start(self):
        logging.info('Start file updates sender, interval = %s sec', self._interval)

        FileUpdatesSenderTimer(self._interval, self._logfile).start()


class FileUpdatesSenderTimer(Thread):

    def __init__(self, interval, logfile):
        Thread.__init__(self)
        self._interval = interval
        self._logfile = logfile
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(SEAHUB_DIR, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'send_file_updates',
                    ]
                    with open(self._logfile, 'a') as fp:
                        run_and_wait(cmd, cwd=SEAHUB_DIR, output=fp)
                except Exception as e:
                    logging.exception('send file updates email error: %s', e)

    def cancel(self):
        self.finished.set()
