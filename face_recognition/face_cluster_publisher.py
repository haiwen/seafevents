import logging
import json

from seafevents.db import init_db_session_class
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager
from seafevents.repo_data import repo_data
from seafevents.mq import get_mq
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger('face_recognition')


class FaceClusterPublisher(object):
    def __init__(self):
        self._face_recognition_manager = FaceRecognitionManager()
        self._session = init_db_session_class()
        self.mq_server = REDIS_HOST
        self.mq_port = REDIS_PORT
        self.mq_password = REDIS_PASSWORD

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)

    def start(self):
        if not self.mq:
            return
        try:
            self.publish_face_cluster_task()
        except Exception as e:
            logger.exception("Error: %s" % e)

    def publish_face_cluster_task(self):
        logger.info("Start publish face cluster")

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
                    msg_content = {
                        'msg_type': 'update_face_recognition',
                        'repo_id': repo_id
                    }
                    if self.mq.publish('metadata_update', json.dumps(msg_content)) > 0:
                        logger.debug('Publish metadata_update event: %s' % msg_content)
                    else:
                        logger.info(
                            'No one subscribed to metadata_update channel, event (%s) has not been send' % msg_content)
                except Exception as e:
                    logger.exception("repo: %s, update face cluster error: %s" % (repo_id, e))
