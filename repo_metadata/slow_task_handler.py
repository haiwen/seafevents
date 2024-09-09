import os.path
import time
import logging
import threading
import json

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq
from seafevents.utils import get_opt_from_conf_or_env
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.repo_metadata import METADATA_OP_LIMIT
from seafevents.repo_metadata.utils import METADATA_TABLE, get_latlng
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI
from seafevents.seasearch.utils.seasearch_api import SeaSearchAPI
from seafevents.utils import parse_bool
from seafevents.seasearch.index_store.repo_image_index import RepoImageIndex
from seafevents.seasearch.utils.constants import REPO_IMAGE_INDEX_PREFIX

logger = logging.getLogger(__name__)


class SlowTaskHandler(object):
    """ The handler for redis message queue
    """

    def __init__(self, config):
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.seafile_ai_api = SeafileAIAPI()
        self.seasearch_api = None

        self.should_stop = threading.Event()
        self.mq_server = '127.0.0.1'
        self.mq_port = 6379
        self.mq_password = ''
        self.worker_num = 3
        self._parse_config(config)

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)
        self.repo_image_index = RepoImageIndex(self.seasearch_api)

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

        seasearch_section_name = 'SEASEARCH'
        seasearch_key_enabled = 'enabled'
        if config.has_section(seasearch_section_name):
            enabled = get_opt_from_conf_or_env(config, seasearch_section_name, seasearch_key_enabled, default=False)
            if parse_bool(enabled):
                seasearch_url = get_opt_from_conf_or_env(config, seasearch_section_name, 'seasearch_url')
                seasearch_token = get_opt_from_conf_or_env(config, seasearch_section_name, 'seasearch_token')
                self.seasearch_api = SeaSearchAPI(seasearch_url, seasearch_token)

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
        if task_type == 'image_info_extract':
            self.extract_image_info(repo_id, data)
        elif task_type == 'delete_image_index':
            self.delete_image_index(repo_id, data)
        elif task_type == 'modify_image_index':
            self.modify_image_index(repo_id, data)

    def delete_image_index(self, repo_id, data):
        logger.info('%s start delete image index repo %s' % (threading.currentThread().getName(), repo_id))

        try:
            paths = data.get('paths')
            if paths:
                repo_image_index_name = REPO_IMAGE_INDEX_PREFIX + repo_id
                self.repo_image_index.delete_images(repo_image_index_name, paths)
        except Exception as e:
            logger.exception('repo: %s, delete image index error: %s', repo_id, e)

    def modify_image_index(self, repo_id, data):
        logger.info('%s start modify image index repo %s' % (threading.currentThread().getName(), repo_id))

        try:
            obj_ids = data.get('obj_ids')
            sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}`, `{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}`, `{METADATA_TABLE.columns.image_feature.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.obj_id.name}` IN ('
            parameters = []

            for obj_id in obj_ids:
                sql += '?, '
                parameters.append(obj_id)

            if not parameters:
                return
            sql = sql.rstrip(', ') + ');'
            query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])
            if not query_result:
                return

            images_data = []
            for row in query_result:
                parent_dir = row[METADATA_TABLE.columns.parent_dir.name]
                file_name = row[METADATA_TABLE.columns.file_name.name]
                image_feature = row[METADATA_TABLE.columns.image_feature.name]
                images_data.append({
                    'path': os.path.join(parent_dir, file_name),
                    'embedding': json.loads(image_feature) if image_feature else '',
                })

            if images_data:
                repo_image_index_name = REPO_IMAGE_INDEX_PREFIX + repo_id
                self.repo_image_index.create_index_if_missing(repo_image_index_name)
                paths = [image['path'] for image in images_data]
                self.repo_image_index.delete_images(repo_image_index_name, paths)
                self.repo_image_index.add_images(repo_image_index_name, images_data)
        except Exception as e:
            logger.exception('repo: %s, modify image index error: %s', repo_id, e)

    def extract_image_info(self, repo_id, data):
        logger.info('%s start extract image info repo %s' % (threading.currentThread().getName(), repo_id))

        try:
            obj_ids = data.get('obj_ids')
            sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}`, `{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.obj_id.name}` IN ('
            parameters = []

            obj_id_to_extract_info = {}
            updated_rows = []
            for obj_id in obj_ids:
                obj_id_to_extract_info[obj_id] = {
                    'location': get_latlng(repo_id, obj_id)
                }
                sql += '?, '
                parameters.append(obj_id)

            embeddings = self.seafile_ai_api.images_embedding(repo_id, obj_ids).get('data', [])
            for embedding in embeddings:
                obj_id = embedding['obj_id']
                if obj_id in obj_id_to_extract_info:
                    obj_id_to_extract_info[obj_id]['embedding'] = embedding['embedding']

            if not parameters:
                return
            sql = sql.rstrip(', ') + ');'
            query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])
            if not query_result:
                return

            images_data = []
            for row in query_result:
                row_id = row[METADATA_TABLE.columns.id.name]
                obj_id = row[METADATA_TABLE.columns.obj_id.name]
                parent_dir = row[METADATA_TABLE.columns.parent_dir.name]
                file_name = row[METADATA_TABLE.columns.file_name.name]
                lat, lng = obj_id_to_extract_info.get(obj_id, {}).get('location')
                embedding = obj_id_to_extract_info.get(obj_id, {}).get('embedding')
                update_row = {
                    METADATA_TABLE.columns.id.name: row_id,
                    METADATA_TABLE.columns.location.name: {'lng': lng, 'lat': lat},
                    METADATA_TABLE.columns.image_feature.name: json.dumps(embedding) if embedding else '',
                }
                updated_rows.append(update_row)
                images_data.append({
                    'path': os.path.join(parent_dir, file_name),
                    'embedding': embedding,
                })

                if len(updated_rows) >= METADATA_OP_LIMIT:
                    self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
                    updated_rows = []

            if images_data:
                repo_image_index_name = REPO_IMAGE_INDEX_PREFIX + repo_id
                self.repo_image_index.create_index_if_missing(repo_image_index_name)
                self.repo_image_index.add_images(repo_image_index_name, images_data)
            if not updated_rows:
                return
            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
        except Exception as e:
            logger.exception('repo: %s, update metadata image info error: %s', repo_id, e)
