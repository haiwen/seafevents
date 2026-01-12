# coding: UTF-8
import logging
from threading import Thread, Event

from seafevents.face_recognition.face_cluster_publisher import FaceClusterPublisher
from seafevents.app.config import ENABLE_FACE_RECOGNITION

logger = logging.getLogger('face_recognition')


class FaceClusterTaskPublisher(object):
    def __init__(self):
        self._interval = 60 * 60
        self._enabled = ENABLE_FACE_RECOGNITION

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start face cluster timer: please check you config!')
            return

        logging.info('Face cluster timer is started, interval = %s sec', self._interval)
        FaceClusterTaskPublishTimer(self._interval).start()

    def is_enabled(self):
        return self._enabled


class FaceClusterTaskPublishTimer(Thread):

    def __init__(self, interval):
        Thread.__init__(self)
        self._interval = interval
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    FaceClusterPublisher().start()
                except Exception as e:
                    logger.exception('error when face cluster: %s', e)

    def cancel(self):
        self.finished.set()
