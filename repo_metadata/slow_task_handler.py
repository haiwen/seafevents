import time
import logging
import threading
import json

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager
from seafevents.repo_metadata.utils import add_file_details
from seafevents.db import init_db_session_class
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger(__name__)


# 是 Redis 消息队列的处理器，负责处理慢速元数据任务。
class SlowMetadataTaskHandler(object):
    """ The handler for redis message queue
    """

    def __init__(self, config):
        # 使用配置对象初始化处理器。
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.face_recognition_manager = FaceRecognitionManager(config)

        # 设置元数据服务器 API、人脸识别管理器和 Redis 连接。
        self.should_stop = threading.Event()
        self.mq_server = REDIS_HOST
        self.mq_port = REDIS_PORT
        self.mq_password = REDIS_PASSWORD
        self.worker_num = 3
        self.session = init_db_session_class(config)
        self._parse_config(config)

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)

    def _parse_config(self, config):
        metadata_section_name = 'METADATA'
        key_index_workers = 'index_workers'
        if config.has_section(metadata_section_name):
            self.worker_num = get_opt_from_conf_or_env(config, metadata_section_name, key_index_workers, default=3)

    @property
    def tname(self):
        # 返回当前线程的名称。
        return threading.current_thread().name

    def start(self):
        if not self.mq:
            return
        for i in range(int(self.worker_num)):
            threading.Thread(target=self.worker_handler, name='slow_task_handler_thread_' + str(i), daemon=True).start()

    def worker_handler(self):
        # 在每个工作线程中运行，处理 Redis 队列中的慢速元数据任务。
        # 处理任务执行、错误日志和 Redis 连接问题。
        logger.info('%s starting update metadata work' % self.tname)
        try:
            while not self.should_stop.isSet():
                try:
                    res = self.mq.brpop('metadata_slow_task', timeout=30)
                    if res is not None:
                        key, value = res
                        try:
                            data = json.loads(value)
                        except:
                            data = None

                        if not data:
                            logger.warning('metadata_slow_task: invalid.', res)
                        else:
                            repo_id = data.get('repo_id')
                            self.slow_task_handler(repo_id, data)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s Handle slow Task Error' % self.tname)
            logger.error(e, exc_info=True)
            # prevent case that redis break at program running.
            time.sleep(0.3)

    def slow_task_handler(self, repo_id, data):
        # 根据任务类型（例如文件信息提取）处理特定的慢速元数据任务。
        task_type = data.get('task_type')
        if task_type == 'file_info_extract':
            self.extract_file_info(repo_id, data)

    def extract_file_info(self, repo_id, data):
        logger.info('%s start extract file info repo %s' % (threading.current_thread().name, repo_id))

        try:
            # 为给定的存储库和对象 ID 提取文件信息。
            obj_ids = data.get('obj_ids')
            # 使用元数据服务器 API 和人脸识别管理器，更新元数据（增加细节信息）。
            add_file_details(repo_id, obj_ids, self.metadata_server_api, self.face_recognition_manager)
        except Exception as e:
            logger.exception('repo: %s, update metadata file info error: %s', repo_id, e)

        logger.info('%s finish extract file info repo %s' % (threading.current_thread().name, repo_id))
