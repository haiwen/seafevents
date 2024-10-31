import logging

from seafevents.db import init_db_session_class
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager
from seafevents.face_recognition.db import get_mtime_by_repo_ids, get_face_recognition_enabled_repo_list, update_face_cluster_time

logger = logging.getLogger(__name__)


class RepoFaceClusterUpdater(object):
    def __init__(self, config, seafile_config):
        self._face_recognition_manager = FaceRecognitionManager(config)
        self._session = init_db_session_class(config)
        self._seafdb_session = init_db_session_class(seafile_config, db='seafile')

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
                repos = get_face_recognition_enabled_repo_list(self._session, start, count)
            except Exception as e:
                logger.error("Error: %s" % e)
                return
            start += 1000

            if len(repos) == 0:
                break

            repo_ids = [repo[0] for repo in repos]
            repos_mtime = get_mtime_by_repo_ids(self._seafdb_session, repo_ids)
            repo_id_to_mtime = {repo[0]: repo[1] for repo in repos_mtime}

            for repo in repos:
                repo_id = repo[0]
                last_face_cluster_time = repo[1]
                mtime = repo_id_to_mtime.get(repo_id)
                if not mtime:
                    continue

                if last_face_cluster_time and int(mtime) <= int(last_face_cluster_time.timestamp()):
                    continue
                self._face_recognition_manager.face_cluster(repo_id)

        logger.info("Finish update face cluster")
