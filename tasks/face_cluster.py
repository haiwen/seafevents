# coding: UTF-8
import os
import logging
from threading import Thread, Event

from seafevents.utils import get_python_executable, run


class FaceCluster(object):
    def __init__(self):
        self._interval = 20
        self._logfile = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''), 'face_recognition.log')

    def start(self):
        FaceClusterTimer(self._interval, self._logfile).start()


class FaceClusterTimer(Thread):

    def __init__(self, interval, log_file):
        Thread.__init__(self)
        self._interval = interval
        self._logfile = '/data/dev/seafevents/face.log'
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    cmd = [
                        get_python_executable(),
                        '-m', 'seafevents.face_recognition.main',
                        '--logfile', self._logfile,
                        '--config-file', os.environ['EVENTS_CONFIG_FILE']
                    ]
                    env = dict(os.environ)
                    run(cmd, env=env)
                except Exception as e:
                    logging.exception('error when face cluster: %s', e)

    def cancel(self):
        self.finished.set()
