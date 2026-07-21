# coding: UTF-8
import logging
from threading import Thread, Event

from seafevents.ai_summary.ai_summary import AISummary
from seafevents.app.config import ENABLE_SEAFILE_AI


__all__ = [
    'AISummaryUpdater',
]


class AISummaryUpdater(object):
    def __init__(self, config):
        self._enabled = ENABLE_SEAFILE_AI
        self._config = config
        self._initiator = None

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start ai summary updater: it is not enabled!')
            return

        logging.info('ai summary updater is started')
        self._initiator = AISummaryInitiator(self._config)
        self._initiator.start()

    def is_enabled(self):
        return self._enabled

    def cancel(self):
        if self._initiator:
            self._initiator.cancel()


class AISummaryInitiator(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.daemon = True
        self._config = config
        self._ai_summary = None
        self.finished = Event()

    def run(self):
        try:
            self._ai_summary = AISummary(self._config)
            self._ai_summary.start()
        except Exception as e:
            logging.exception('error when start ai summary updater: %s', e)

    def cancel(self):
        self.finished.set()
        if self._ai_summary:
            self._ai_summary.clear()
