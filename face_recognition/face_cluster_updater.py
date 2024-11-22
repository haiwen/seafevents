import logging

from seafevents.db import init_db_session_class
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager

logger = logging.getLogger('face_recognition')


class RepoFaceClusterUpdater(object):
    def __init__(self, config):
        self._face_recognition_manager = FaceRecognitionManager(config)
        self._session = init_db_session_class(config)

    def start(self):
        try:
            self.update_face_cluster()
        except Exception as e:
            logger.exception("Error: %s" % e)

    def update_face_cluster(self):
        logger.info("Start update face cluster")

        start, count = 0, 1000
        while True:
            try:
                repos = self._face_recognition_manager.get_pending_face_cluster_repo_list(start, count)
            except Exception as e:
                logger.error("Fail to get cluster repo list, Error: %s" % e)
                return
            start += 1000

            if len(repos) == 0:
                break

            for repo in repos:
                repo_id = repo[0]
                face_creator = repo[1]
                face_commit = repo[2]
                metadata_from_commit = repo[3]

                try:
                    self._face_recognition_manager.update_face_cluster(repo_id, face_commit, face_creator)
                except Exception as e:
                    logger.exception("repo: %s, update face cluster error: %s" % (repo_id, e))

        logger.info("Finish update face cluster")
