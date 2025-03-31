import logging
import json

from seafevents.db import init_db_session_class
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager
from seafevents.repo_data import repo_data
from seafevents.mq import get_mq
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger('face_recognition')


# 负责更新资料库中的面部聚类。它使用消息队列（MQ）发布面部识别更新事件。
class FaceClusterPublisher(object):
    def __init__(self, config):
        # 使用配置对象初始化类，设置面部识别管理器、数据库会话和消息队列。
        self._face_recognition_manager = FaceRecognitionManager(config)
        self._session = init_db_session_class(config)
        self.mq_server = REDIS_HOST
        self.mq_port = REDIS_PORT
        self.mq_password = REDIS_PASSWORD

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)

# 解析配置对象，以提取 Redis 设置（服务器、端口、密码），并更新类属性。
    def start(self):
        if not self.mq:
            return
        try:
            self.publish_face_cluster_task()
        except Exception as e:
            logger.exception("Error: %s" % e)

    # 更新资料库中的面部聚类（核心方法）
    def publish_face_cluster_task(self):
        logger.info("Start publish face cluster")

        start, count = 0, 1000
        while True:
            # 检索待处理的面部聚类资料库列表。
            try:
                # 获取待处理的面部聚类资料库列表
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

            # 遍历列表并发布面部识别更新事件到消息队列。
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
                # 处理异常并记录错误。
                except Exception as e:
                    logger.exception("repo: %s, update face cluster error: %s" % (repo_id, e))
