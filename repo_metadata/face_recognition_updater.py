import logging
from threading import Thread

from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.gevent import GeventScheduler
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.face_recognition_manager import FaceRecognitionManager
from seafevents.repo_metadata.utils import get_face_recognition_enabled_repo_list

logger = logging.getLogger(__name__)


class RepoFaceClusterUpdater(object):
    def __init__(self, config):
        self._interval = 3600 * 24  # 24 hours
        self._face_recognition_manager = FaceRecognitionManager(config)
        self._session = init_db_session_class(config)

    def start(self):
        logging.info('Start to update face cluster, interval = %s sec', self._interval)
        FaceClusterUpdaterTimer(
            self._face_recognition_manager,
            self._session,
            self._interval
        ).start()


def update_face_cluster(face_recognition_manager, session):
    start, count = 0, 1000
    while True:
        try:
            repos = get_face_recognition_enabled_repo_list(session, start, count)
        except Exception as e:
            logger.error("Error: %s" % e)
            return
        start += 1000

        if len(repos) == 0:
            break

        repo_ids = [repo[0] for repo in repos]
        for repo_id in repo_ids:
            face_recognition_manager.face_cluster(repo_id)

    logger.info("Finish update face cluster")


class FaceClusterUpdaterTimer(Thread):
    def __init__(self, face_recognition_manager, session, interval):
        super(FaceClusterUpdaterTimer, self).__init__()
        self.face_recognition_manager = face_recognition_manager
        self.session = session
        self.interval = interval

    def run(self):
        sched = GeventScheduler()
        logging.info('Start to update face cluster...')
        try:
            sched.add_job(update_face_cluster, CronTrigger(day_of_week='*'),
                          args=(self.face_recognition_manager, self.session))
        except Exception as e:
            logging.exception('periodical update face cluster error: %s', e)

        sched.start()
