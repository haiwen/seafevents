import logging

from seafevents.db import init_db_session_class
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager
from seafevents.repo_data import repo_data

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
        logger.info("Start timer update face cluster")

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

            repo_ids = [repo[0] for repo in repos]
            repos_mtime = repo_data.get_mtime_by_repo_ids(repo_ids)
            repo_id_to_mtime = {repo[0]: repo[1] for repo in repos_mtime}

            for repo in repos:
                repo_id = repo[0]
                last_face_cluster_time = repo[1]
                mtime = repo_id_to_mtime.get(repo_id)
                if not mtime:
                    continue

                if last_face_cluster_time and int(mtime) <= int(last_face_cluster_time.timestamp()):
                    continue

                try:
                    self._face_recognition_manager.update_face_cluster(repo_id)
                except Exception as e:
                    logger.exception("repo: %s, update face cluster error: %s" % (repo_id, e))

        logger.info("Finish update face cluster")
