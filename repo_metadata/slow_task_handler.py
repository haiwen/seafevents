import time
import logging
import threading
import json

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq
from seafevents.utils import get_opt_from_conf_or_env
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.image_embedding_api import ImageEmbeddingAPI
from seafevents.repo_metadata.repo_metadata import METADATA_OP_LIMIT
from seafevents.repo_metadata.utils import METADATA_TABLE, get_file_content, get_image_details, FACES_TABLE, query_metadata_rows, face_recognition
from seafevents.repo_metadata.constants import PrivatePropertyKeys

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
            image_embedding_server_url = get_opt_from_conf_or_env(config, ai_section_name, 'image_embedding_server_url')
            image_embedding_secret_key = get_opt_from_conf_or_env(config, ai_section_name, 'image_embedding_secret_key')
            self.image_embedding_api = ImageEmbeddingAPI(image_embedding_server_url, image_embedding_secret_key)

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
            obj_ids = data.get('obj_ids')
            commit_id = data.get('commit_id')
            per_size = 50
            for i in range(0, len(obj_ids), per_size):
                self.extract_image_info(repo_id, commit_id, obj_ids[i: i + per_size])

    def extract_image_info(self, repo_id, commit_id, obj_ids):
        logger.info('%s start extract image info repo %s' % (threading.currentThread().getName(), repo_id))

        try:
            sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}` FROM `{METADATA_TABLE.name}` WHERE `{METADATA_TABLE.columns.obj_id.name}` IN ('
            parameters = []

            updated_rows = []
            for obj_id in obj_ids:
                sql += '?, '
                parameters.append(obj_id)

            if not parameters:
                return
            sql = sql.rstrip(', ') + ');'
            query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])
            if not query_result:
                return

            columns = self.metadata_server_api.list_columns(repo_id, METADATA_TABLE.id).get('columns', [])
            capture_time_column = [column for column in columns if column.get('key') == PrivatePropertyKeys.CAPTURE_TIME]

            obj_id_to_rows = {}
            for item in query_result:
                obj_id = item[METADATA_TABLE.columns.obj_id.name]
                if obj_id not in obj_id_to_rows:
                    obj_id_to_rows[obj_id] = []
                obj_id_to_rows[obj_id].append(item)

            metadata = self.metadata_server_api.get_metadata(repo_id)
            tables = metadata.get('tables', [])
            if not tables:
                return
            faces_table_id = [table['id'] for table in tables if table['name'] == FACES_TABLE.name]
            faces_table_id = faces_table_id[0] if faces_table_id else None

            if faces_table_id:
                sql = f'SELECT * FROM `{FACES_TABLE.name}`'
                known_faces = query_metadata_rows(repo_id, self.metadata_server_api, sql)
                embeddings = self.image_embedding_api.face_embeddings(repo_id, obj_ids).get('data', [])

                used_faces = []
                no_used_face_row_ids = []
                for item in known_faces:
                    if item.get(FACES_TABLE.columns.photo_links.name):
                        used_faces.append(item)
                    else:
                        no_used_face_row_ids.append(item[FACES_TABLE.columns.id.name])
                if no_used_face_row_ids:
                    self.metadata_server_api.delete_rows(repo_id, faces_table_id, no_used_face_row_ids)

                for item in embeddings:
                    obj_id = item['obj_id']
                    face_embeddings = item['embeddings']
                    recognized_faces = []
                    for face_embedding in face_embeddings:
                        face = face_recognition(face_embedding, used_faces, 1.24)
                        if not face:
                            row = {
                                FACES_TABLE.columns.vector.name: json.dumps(face_embedding),
                            }
                            result = self.metadata_server_api.insert_rows(repo_id, faces_table_id, [row])
                            row_id = result.get('row_ids')[0]
                            used_faces.append({
                                FACES_TABLE.columns.id.name: row_id,
                                FACES_TABLE.columns.vector.name: json.dumps(face_embedding),
                            })
                            row_id_map = {
                                row_id: [item.get(METADATA_TABLE.columns.id.name) for item in obj_id_to_rows.get(obj_id, [])]
                            }
                            self.metadata_server_api.insert_link(repo_id, FACES_TABLE.link_id, faces_table_id, row_id_map)
                        else:
                            recognized_faces.append(face)

                    if recognized_faces:
                        row_ids = [item[FACES_TABLE.columns.id.name] for item in recognized_faces]
                        row_id_map = dict()
                        for row in obj_id_to_rows.get(obj_id, []):
                            row_id_map[row[METADATA_TABLE.columns.id.name]] = row_ids
                        self.metadata_server_api.insert_link(repo_id, FACES_TABLE.link_id, METADATA_TABLE.id, row_id_map)

            for row in query_result:
                row_id = row[METADATA_TABLE.columns.id.name]
                obj_id = row[METADATA_TABLE.columns.obj_id.name]

                content = get_file_content(repo_id, commit_id, obj_id)
                image_details, location = get_image_details(content)
                row[METADATA_TABLE.columns.location.name] = {'lng': location.get('lng', ''),
                                                             'lat': location.get('lat', '')}
                row[METADATA_TABLE.columns.file_details.name] = f'\n\n```json\n{json.dumps(image_details)}\n```\n\n\n'

                update_row = {
                    METADATA_TABLE.columns.id.name: row_id,
                    METADATA_TABLE.columns.location.name: {'lng': location.get('lng', ''), 'lat': location.get('lat', '')},
                    METADATA_TABLE.columns.file_details.name: f'\n\n```json\n{json.dumps(image_details)}\n```\n\n\n',
                }

                if capture_time_column:
                    capture_time = image_details.get('Capture time')
                    if capture_time:
                        update_row[PrivatePropertyKeys.CAPTURE_TIME] = capture_time
                updated_rows.append(update_row)

                if len(updated_rows) >= METADATA_OP_LIMIT:
                    self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
                    updated_rows = []

            if not updated_rows:
                return
            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
        except Exception as e:
            logger.exception('repo: %s, update metadata info error: %s', repo_id, e)
