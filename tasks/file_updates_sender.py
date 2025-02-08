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

    def start(self):
        logging.info('Start file updates sender, interval = %s sec', self._interval)

        FileUpdatesSenderTimer(self._interval).start()


class FileUpdatesSenderTimer(Thread):

    def __init__(self, interval):
        Thread.__init__(self)
        self._interval = interval
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
                    run_and_wait(cmd, cwd=SEAHUB_DIR)
                except Exception as e:
                    logging.exception('send file updates email error: %s', e)

    def cancel(self):
        self.finished.set()
