import json
import logging
import time
import threading

from collections import OrderedDict
from copy import deepcopy
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq, NoMessageException
from seafevents.ai_summary.ai_summary_manager import AISummaryManager
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager
from seafevents.repo_metadata.utils import add_file_details
from seafevents.db import init_db_session_class
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD


logger = logging.getLogger(__name__)

class MetadataManager(object):
    def __init__(self):
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.face_recognition_manager = FaceRecognitionManager()
        self.ai_summary_manager = AISummaryManager()

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
            username = data.get('username', '')
            data = op_type + '\t' + repo_id + '\t' + username
            self.mq.lpush('init_ai_summary_task', data)
            logger.debug('init ai_summary: %s has been add to metadata task queue' % message['data'])
        else:
            logger.warning('op_type invalid, repo_id: %s, op_type: %s' % (repo_id, op_type))

    def slow_task_worker_handler(self):
        logger.info('%s starting update metadata work' % self.tname)
        try:
            while not self.should_stop.isSet():
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

    def extract_file_info(self, repo_id, data):
        logger.info('%s start extract file info repo %s' % (threading.current_thread().name, repo_id))

        obj_ids = data.get('obj_ids')
        logger.debug('Extract file info task repo=%s, obj_count=%d', repo_id, len(obj_ids) if isinstance(obj_ids, list) else 0)
        try:
            add_file_details(repo_id, obj_ids, self.metadata_server_api, self.face_recognition_manager)
        except Exception as e:
            logger.exception('repo: %s, update metadata file info error: %s', repo_id, e)

        try:
            self.ai_summary_manager.add_ai_summary_task(repo_id, obj_ids)
            logger.debug('Enqueued incremental ai summary task repo=%s, obj_count=%d',
                         repo_id, len(obj_ids) if isinstance(obj_ids, list) else 0)
        except Exception as e:
            logger.exception('repo: %s, enqueue metadata ai summary task error: %s', repo_id, e)

        logger.info('%s finish extract file info repo %s' % (threading.current_thread().name, repo_id))
