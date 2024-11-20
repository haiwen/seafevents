import logging

from seafevents.db import init_db_session_class
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager
from seafevents.face_recognition.db import get_mtime_by_repo_ids, get_face_recognition_enabled_repo_list, update_face_cluster_time

logger = logging.getLogger(__name__)


class RepoFaceClusterUpdater(object):
    def __init__(self, config, seafile_config):
        self._face_recognition_manager = FaceRecognitionManager(config)
        self._session = init_db_session_class(config)

        # 这个可以换成 from seafevents.repo_data import repo_data, 或者 from seafevents.utils.seafile_db import SeafileDB
        # 其实后面这个也是多余的
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
                repos = self._face_recognition_manager.get_pending_face_cluster_repo_list(start, count)
            except Exception as e:
                logger.error("Fail to get enabled repo list, Error: %s" % e)
                return
            start += 1000

            if len(repos) == 0:
                break

            # repo_ids = [repo[0] for repo in repos]
            # repos_mtime = get_mtime_by_repo_ids(self._seafdb_session, repo_ids)
            # # 这里是获取了资料库的更新时间，根据更新时间判断是否触发这个资料库的更新，
            # # 这里是不是可以改成根据 metadata ，查询出file_mtime > 某时刻，且file_type是picture类型的数据呢？
            # # 且face_vectors不为Null,或0，不行，因为还有删除的情况
            #
            # repo_id_to_mtime = {repo[0]: repo[1] for repo in repos_mtime}

            for repo in repos:
                repo_id = repo[0]
                face_creator = repo[1]
                face_commit = repo[2]
                metadata_from_commit = repo[3]

                try:
                    self._face_recognition_manager.update_face_cluster(repo_id, face_commit, metadata_from_commit, face_creator)
                except Exception as e:
                    logger.exception("repo: %s, update face cluster error: %s" % (repo_id, e))

        logger.info("Finish update face cluster")
