import time
import logging
import threading
import json

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq
from seafevents.utils import get_opt_from_conf_or_env
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.image_embedding_api import ImageEmbeddingAPI
from seafevents.repo_metadata.utils import add_file_details
from seafevents.seafevent_server.face_recognition_task_manager import face_recognition_task_manager

logger = logging.getLogger(__name__)


class SlowTaskHandler(object):
    """ The handler for redis message queue
    """

    def __init__(self, config):
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.image_embedding_api = None

        self.should_stop = threading.Event()
        self.mq_server = '127.0.0.1'
        self.mq_port = 6379
        self.mq_password = ''
        self.worker_num = 3
        self._parse_config(config)

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)

    def _parse_config(self, config):
        redis_section_name = 'REDIS'
        key_server = 'server'
        key_port = 'port'
        key_password = 'password'

        if config.has_section(redis_section_name):
            self.mq_server = get_opt_from_conf_or_env(config, redis_section_name, key_server, default='')
            self.mq_port = get_opt_from_conf_or_env(config, redis_section_name, key_port, default=6379)
            self.mq_password = get_opt_from_conf_or_env(config, redis_section_name, key_password, default='')

        metadata_section_name = 'METADATA'
        key_index_workers = 'index_workers'
        if config.has_section(metadata_section_name):
            self.worker_num = get_opt_from_conf_or_env(config, metadata_section_name, key_index_workers, default=3)

        ai_section_name = 'AI'
        if config.has_section(ai_section_name):
            image_embedding_service_url = get_opt_from_conf_or_env(config, ai_section_name, 'image_embedding_service_url')
            image_embedding_secret_key = get_opt_from_conf_or_env(config, ai_section_name, 'image_embedding_secret_key')
            self.image_embedding_api = ImageEmbeddingAPI(image_embedding_service_url, image_embedding_secret_key)

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        for i in range(int(self.worker_num)):
            threading.Thread(target=self.worker_handler, name='slow_task_handler_thread_' + str(i), daemon=True).start()

    def worker_handler(self):
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
        task_type = data.get('task_type')
        if task_type == 'file_info_extract':
            self.extract_file_info(repo_id, data)

    def extract_file_info(self, repo_id, data):
        logger.info('%s start extract file info repo %s' % (threading.currentThread().getName(), repo_id))

        try:
            obj_ids = data.get('obj_ids')
            add_file_details(repo_id, obj_ids, self.metadata_server_api, face_recognition_task_manager)
        except Exception as e:
            logger.exception('repo: %s, update metadata image info error: %s', repo_id, e)
