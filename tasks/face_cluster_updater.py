# coding: UTF-8
import os
import logging
import time
from threading import Thread, Event

from seafevents.utils import get_python_executable, run


__all__ = [
    'FaceClusterUpdater',
]


class FaceClusterUpdater(object):
    def __init__(self, config):
        self._enabled = True
        self._logfile = None
        self._loglevel = None

        self._parse_config(config)

    def _parse_config(self, config):
        # default face cluster log file is 'face_recognition.log' in SEAFEVENTS_LOG_DIR
        logfile = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''), 'face_recognition.log')
        loglevel = 'info'
        self._logfile = os.path.abspath(logfile)
        self._loglevel = loglevel

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start face cluster updater: it is not enabled!')
            return

        logging.info('face cluster updater is started')
        FaceClusterInitiator(self._logfile, self._loglevel).start()

    def is_enabled(self):
        return self._enabled


class FaceClusterInitiator(Thread):
    def __init__(self, logfile, loglevel):
        Thread.__init__(self)
        self._logfile = logfile
        self._loglevel = loglevel
        self.finished = Event()

    def run(self):
        try:
            events_config_file = os.environ.get('EVENTS_CONFIG_FILE')
            cmd = [
                get_python_executable(),
                '-m', 'seafevents.face_recognition.face_cluster',
                '--config-file', events_config_file,
                '--logfile', self._logfile,
                '--loglevel', self._loglevel,
            ]

            run(cmd, )
        except Exception as e:
            logging.exception('error when start face cluster updater: %s', e)

    def cancel(self):
        self.finished.set()
