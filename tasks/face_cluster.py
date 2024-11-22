# coding: UTF-8
import logging
from threading import Thread, Event
from seafevents.utils import get_opt_from_conf_or_env

from seafevents.face_recognition.face_cluster_updater import RepoFaceClusterUpdater

logger = logging.getLogger('face_recognition')


class FaceCluster(object):
    def __init__(self, config):
        self._interval = 60 * 60
        self.config = config
        self._enabled = True

    def _parse_config(self, config):
        ai_section_name = 'AI'
        if config.has_section(ai_section_name):
            image_embedding_service_url = get_opt_from_conf_or_env(config, ai_section_name, 'image_embedding_service_url')
            image_embedding_secret_key = get_opt_from_conf_or_env(config, ai_section_name, 'image_embedding_secret_key')
            if not image_embedding_service_url or not image_embedding_secret_key:
                self._enabled = False

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start face cluster: please check you config!')
            return

        logging.info('Face cluster is started, interval = %s sec', self._interval)
        FaceClusterTimer(self._interval, self.config).start()

    def is_enabled(self):
        return self._enabled


class FaceClusterTimer(Thread):

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
                    RepoFaceClusterUpdater(self._config).start()
                except Exception as e:
                    logger.exception('error when face cluster: %s', e)

    def cancel(self):
        self.finished.set()
