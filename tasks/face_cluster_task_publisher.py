# coding: UTF-8
import logging
from threading import Thread, Event

from seafevents.face_recognition.face_cluster_publisher import FaceClusterPublisher
from seafevents.app.config import ENABLE_SEAFILE_AI

logger = logging.getLogger('face_recognition')


class FaceClusterTaskPublisher(object):
    def __init__(self, config):
        self._interval = 60 * 60
        self.config = config
        self._enabled = ENABLE_SEAFILE_AI

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start face cluster timer: please check you config!')
            return

        logging.info('Face cluster timer is started, interval = %s sec', self._interval)
        FaceClusterTaskPublishTimer(self._interval, self.config).start()

    def is_enabled(self):
        return self._enabled


class FaceClusterTaskPublishTimer(Thread):

    def __init__(self, interval, config):
        Thread.__init__(self)
        self._interval = interval
        self._config = config
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    FaceClusterPublisher(self._config).start()
                except Exception as e:
                    logger.exception('error when face cluster: %s', e)

    def cancel(self):
        self.finished.set()
