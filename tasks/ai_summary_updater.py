# coding: UTF-8
import os
import logging
from threading import Thread, Event

from seafevents.utils import get_python_executable, run
from seafevents.app.config import ENABLE_SEAFILE_AI


__all__ = [
    'AISummaryUpdater',
]


class AISummaryUpdater(object):
    def __init__(self, config):
        self._enabled = ENABLE_SEAFILE_AI
        self._logfile = None
        self._loglevel = None

        self._parse_config(config)

    def _parse_config(self, config):
        logfile = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''), 'ai_summary.log')
        self._logfile = os.path.abspath(logfile)
        self._loglevel = 'info'

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start ai summary updater: it is not enabled!')
            return

        logging.info('ai summary updater is started')
        AISummaryInitiator(self._logfile, self._loglevel).start()

    def is_enabled(self):
        return self._enabled


class AISummaryInitiator(Thread):
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
                '-m', 'seafevents.ai_summary.ai_summary',
                '--config-file', events_config_file,
                '--logfile', self._logfile,
                '--loglevel', self._loglevel,
            ]

            run(cmd)
        except Exception as e:
            logging.exception('error when start ai summary updater: %s', e)

    def cancel(self):
        self.finished.set()
