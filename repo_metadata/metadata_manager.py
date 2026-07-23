import json
import logging
import time
import threading

from collections import OrderedDict
from copy import deepcopy
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq, NoMessageException
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager
from seafevents.repo_metadata.utils import add_file_details, parse_iso_datetime, add_ai_summary, is_summary_enabled
from seafevents.db import init_db_session_class
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, AI_SUMMARY_WORKERS, ENABLE_SEAFILE_AI, AI_SUMMARY_BATCH_SIZE, SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL
from seafevents.repo_metadata.constants import METADATA_TABLE, SUMMARY_SUPPORTED_FILE_EXTENSIONS
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI


logger = logging.getLogger(__name__)
ai_summary_logger = logging.getLogger('ai_summary')

class MetadataManager(object):
    def __init__(self):
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.face_recognition_manager = FaceRecognitionManager()
        self.seafile_ai_api = SeafileAIAPI(SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)

        self.should_stop = threading.Event()
        self.mq_server = REDIS_HOST
        self.mq_port = REDIS_PORT
        self.mq_password = REDIS_PASSWORD
        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)
        self.pending_tasks = OrderedDict()
        self.no_message_check_interval = 5 * 60
        self.slow_task_worker_num = 3
        self.session = init_db_session_class()
        self.worker_list = []
        self.ai_summary_worker_num = AI_SUMMARY_WORKERS
        self.batch_size = AI_SUMMARY_BATCH_SIZE
        self.lock_timeout = 3600
        self.query_page_size = 1000


    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        if not self.mq:
            return

        t = threading.Thread(target=self.index_master_handler, name='metadata_index_master', daemon=True)
        t.start()
        self.worker_list.append(t)

        for i in range(int(self.slow_task_worker_num)):
            t = threading.Thread(target=self.slow_task_worker_handler, name='slow_task_handler_thread_' + str(i), daemon=True)
            t.start()
            self.worker_list.append(t)

        if ENABLE_SEAFILE_AI:
            for i in range(int(self.ai_summary_worker_num)):
                t = threading.Thread(target=self.ai_summary_worker_handler, name='ai_summary_worker_thread_' + str(i), daemon=True)
                t.start()
                self.worker_list.append(t)

    ########################### metadata update handler thread ########################
    def index_master_handler(self):
        logger.info('metadata master event receive thread started')
        while True:
            try:
                self._consume_metadata_updates()
            except NoMessageException:
                logger.warning('long time no message, reconnect to redis.')
            except (ResponseError, NoMQAvailable, TimeoutError) as e:
                logger.error('The connection to the redis server failed: %s' % e)
            except Exception as e:
                logger.error('Error handing master task: %s' % e)
                time.sleep(0.2)

    def _consume_metadata_updates(self):
        with self.mq.pubsub(ignore_subscribe_messages=True) as p:
            try:
                p.subscribe('metadata_update')
            except Exception as e:
                logger.error('The connection to the redis server failed: %s' % e)
            else:
                logger.info('metadata master starting listen')

            message_check_time = time.time()
            while True:
                while True:
                    message = p.get_message()
                    if not message:
                        break

                    try:
                        data = json.loads(message['data'])
                    except Exception:
                        logger.warning('index master message: invalid.', message)
                        data = None

                    if data:
                        self._route_metadata_update(data, message)

                if len(self.pending_tasks) > 0:
                    copied_pending_tasks = deepcopy(self.pending_tasks)
                    for repo_id, commit_id in copied_pending_tasks.items():
                        op_type = 'update-metadata'
                        data = op_type + '\t' + repo_id
                        self.mq.lpush('metadata_task', data)
                        self.pending_tasks.pop(repo_id)

                if (time.time() - message_check_time) > self.no_message_check_interval:
                    raise NoMessageException
                time.sleep(0.5)

    def _route_metadata_update(self, data, message):
        op_type = data.get('msg_type')
        repo_id = data.get('repo_id')
        commit_id = data.get('commit_id')
        if op_type == 'init-metadata':
            data = op_type + '\t' + repo_id
            self.mq.lpush('metadata_task', data)
            logger.debug('init metadata: %s has been add to metadata task queue' % message['data'])
        elif op_type == 'repo-update':
            self.pending_tasks[repo_id] = commit_id
        elif op_type == 'update_face_recognition':
            username = data.get('username', '')
            data = op_type + '\t' + repo_id + '\t' + username
            self.mq.lpush('face_cluster_task', data)
            logger.debug('update face_recognition: %s has been add to metadata task queue' % message['data'])
        elif op_type == 'init_ai_summary':
            self.add_ai_summary_task(repo_id)
            ai_summary_logger.debug('init ai_summary: %s has been add to metadata task queue' % message['data'])
        else:
            logger.warning('op_type invalid, repo_id: %s, op_type: %s' % (repo_id, op_type))
    ########################### metadata update handler thread ########################
    
    ########################### slow task handler thread ########################
    def slow_task_worker_handler(self):
        logger.info('%s starting update metadata work' % self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('metadata_slow_task', timeout=30)
                    if res is not None:
                        key, value = res
                        try:
                            data = json.loads(value)
                        except Exception:
                            data = None

                        if not data:
                            logger.warning('metadata_slow_task: invalid.', res)
                        else:
                            repo_id = data.get('repo_id')
                            self._handle_slow_task(repo_id, data)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s Handle slow Task Error' % self.tname)
            logger.error(e, exc_info=True)
            time.sleep(0.3)

    def _handle_slow_task(self, repo_id, data):
        task_type = data.get('task_type')
        if task_type == 'file_info_extract':
            self.extract_file_info(repo_id, data)
    ########################### slow task handler thread ########################


    ########################### AI summary handler thread ########################
    def ai_summary_worker_handler(self):
        if not ENABLE_SEAFILE_AI:
            return
        ai_summary_logger.info('%s starting ai summary worker', self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('ai_summary_task', timeout=30)
                    if res is None:
                        continue

                    key, value = res
                    try:
                        data = json.loads(value)
                    except Exception:
                        data = None

                    if not data:
                        ai_summary_logger.warning('ai summary repo task: invalid task payload %s', res)
                        continue

                    repo_id = data.get('repo_id')
                    self._handle_ai_summary_task(repo_id)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    ai_summary_logger.error('The connection to the redis server failed: %s', e)
        except Exception as e:
            ai_summary_logger.error('%s Handle ai summary worker task error', self.tname)
            ai_summary_logger.error(e, exc_info=True)
            time.sleep(0.3)

    def _handle_ai_summary_task(self, repo_id):
        if not repo_id or self.should_stop.is_set():
            return

        try:
            ai_summary_logger.info('%s start ai summary repo %s', self.tname, repo_id)
            self.generate_ai_summary(repo_id, batch_size=self.batch_size)
            ai_summary_logger.info('%s finish ai summary repo %s', self.tname, repo_id)
        except Exception as e:
            ai_summary_logger.exception('ai summary repo: %s, error: %s', repo_id, e)
        finally:
            self.delete_repo_lock(repo_id)

    ###########################  AI summary handler thread  ########################



    ############################ TOOLS ########################
    def extract_file_info(self, repo_id, data):
        logger.info('%s start extract file info repo %s' % (threading.current_thread().name, repo_id))

        obj_ids = data.get('obj_ids')
        logger.debug('Extract file info task repo=%s, obj_count=%d', repo_id, len(obj_ids) if isinstance(obj_ids, list) else 0)
        try:
            add_file_details(repo_id, obj_ids, self.metadata_server_api, self.face_recognition_manager)
        except Exception as e:
            logger.exception('repo: %s, update metadata file info error: %s', repo_id, e)

        try:
            self.add_ai_summary_task(repo_id)
            ai_summary_logger.debug('Enqueued incremental ai summary task repo=%s, obj_count=%d',
                         repo_id, len(obj_ids) if isinstance(obj_ids, list) else 0)
        except Exception as e:
            ai_summary_logger.exception('repo: %s, enqueue metadata ai summary task error: %s', repo_id, e)

        logger.info('%s finish extract file info repo %s' % (threading.current_thread().name, repo_id))


    def add_ai_summary_task(self, repo_id):
        if not repo_id or not self.mq:
            return
        
        lock_key = self.get_repo_lock_key(repo_id)
        if not self.mq.set(lock_key, time.time(), ex=self.lock_timeout, nx=True):
            ai_summary_logger.debug('repo: %s ai summary is running, skip repo task', repo_id)
            return
        
        msg = {
            'repo_id': repo_id,
        }

        self.mq.lpush('ai_summary_task', json.dumps(msg))
        ai_summary_logger.debug('Enqueued ai summary repo task repo=%s, queue=%s', repo_id, 'ai_summary_task')

    def get_repo_lock_key(self, repo_id):
        return 'ai_summary_' + repo_id
    
    def delete_repo_lock(self, repo_id):
        lock_key = self.get_repo_lock_key(repo_id)
        self.mq.delete(lock_key)

    def generate_ai_summary(self, repo_id, batch_size=50):
        if not self.seafile_ai_api or not self.mq or not is_summary_enabled(repo_id):
            ai_summary_logger.debug('Skip ai summary repo=%s, ai_server=%s, mq=%s, summary_enabled=%s',
                          repo_id, bool(self.seafile_ai_api), bool(self.mq), is_summary_enabled(repo_id))
            return

        ai_summary_logger.info('Generating ai summary for repo %s', repo_id)
        base_sql = (
            f'SELECT `{METADATA_TABLE.columns.obj_id.name}`, '
            f'`{METADATA_TABLE.columns.suffix.name}`, '
            f'`{METADATA_TABLE.columns.file_mtime.name}`, '
            f'`{METADATA_TABLE.columns.ai_summary_mtime.name}` '
            f'FROM `{METADATA_TABLE.name}` '
            f'WHERE `{METADATA_TABLE.columns.is_dir.name}` = False '
            f'AND `{METADATA_TABLE.columns.file_type.name}` = "_document"'
        )

        support_suffixes = set(SUMMARY_SUPPORTED_FILE_EXTENSIONS)
        start = 0

        while True:
            query_sql = f'{base_sql} LIMIT {start}, {self.query_page_size}'
            rows = self.metadata_server_api.query_rows(repo_id, query_sql, []).get('results', [])
            if not rows:
                ai_summary_logger.debug('No more ai summary rows repo=%s, start=%d', repo_id, start)
                break

            ai_summary_logger.debug('Fetched ai summary rows repo=%s, start=%d, row_count=%d',
                                    repo_id, start, len(rows))
            obj_ids = []
            for row in rows:
                obj_id = row.get(METADATA_TABLE.columns.obj_id.name)
                suffix = (row.get(METADATA_TABLE.columns.suffix.name) or '').lower()
                if not obj_id or suffix not in support_suffixes:
                    ai_summary_logger.debug('Skip ai summary candidate repo=%s, obj_id=%s, suffix=%s', repo_id, obj_id, suffix)
                    continue

                file_mtime = parse_iso_datetime(row.get(METADATA_TABLE.columns.file_mtime.name))
                ai_summary_mtime = parse_iso_datetime(row.get(METADATA_TABLE.columns.ai_summary_mtime.name))
                if file_mtime and ai_summary_mtime and ai_summary_mtime >= file_mtime:
                    ai_summary_logger.debug('Skip up-to-date ai summary repo=%s, obj_id=%s, file_mtime=%s, ai_summary_mtime=%s',
                                 repo_id, obj_id, file_mtime, ai_summary_mtime)
                    continue

                obj_ids.append(obj_id)
                if len(obj_ids) >= batch_size:
                    add_ai_summary(repo_id, obj_ids, self.metadata_server_api, self.seafile_ai_api)
                    obj_ids = []

            if obj_ids:
                add_ai_summary(repo_id, obj_ids, self.metadata_server_api, self.seafile_ai_api)

            if len(rows) < self.query_page_size:
                break
            start += self.query_page_size

        ai_summary_logger.info('Finish generating ai summary for repo %s', repo_id)
