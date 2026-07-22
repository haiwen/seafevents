import json
import logging
import threading
import time

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY, \
    AI_SUMMARY_WORKERS, AI_SUMMARY_LOCK_BUSY_RETRY_INTERVAL, AI_SUMMARY_BATCH_SIZE
from seafevents.ai_summary.ai_summary_manager import AISummaryManager
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI
from seafevents.repo_metadata.utils import add_ai_summary

logger = logging.getLogger('ai_summary')


class AISummaryTaskWorker(object):
    HIGH_PRIORITY_TASK_QUEUE = 'ai_summary_task_high'
    LOW_PRIORITY_TASK_QUEUE = 'ai_summary_task_low'
    INIT_TASK_QUEUE = 'init_ai_summary_task'
    INIT_WORKER_NUM = 1

    def __init__(self, should_stop):
        self.should_stop = should_stop
        self.lock_timeout = 1800
        self.refresh_interval = 600
        self.mq_server = REDIS_HOST
        self.mq_port = REDIS_PORT
        self.mq_password = REDIS_PASSWORD
        self.worker_num = AI_SUMMARY_WORKERS
        self.lock_busy_retry_interval = AI_SUMMARY_LOCK_BUSY_RETRY_INTERVAL
        self.batch_size = AI_SUMMARY_BATCH_SIZE

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.seafile_ai_api = SeafileAIAPI(SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)
        self.ai_summary_manager = AISummaryManager()
        self.worker_list = []
        self.locked_keys = set()

    def _get_lock_key(self, repo_id):
        return 'ai_summary_' + repo_id

    def _get_init_lock_key(self, repo_id):
        return 'init_ai_summary_' + repo_id

    def _build_task_payload(self, repo_id, obj_ids, is_init=False):
        return json.dumps({
            'repo_id': repo_id,
            'obj_ids': obj_ids,
            'is_init': is_init,
        })

    def _get_task_queue(self, is_init=False):
        return self.LOW_PRIORITY_TASK_QUEUE if is_init else self.HIGH_PRIORITY_TASK_QUEUE

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        if not self.mq:
            return

        for i in range(self.INIT_WORKER_NUM):
            t = threading.Thread(target=self.init_worker_handler, name='ai_summary_init_worker_' + str(i), daemon=True)
            t.start()
            self.worker_list.append(t)

        for i in range(int(self.worker_num)):
            t = threading.Thread(target=self.batch_worker_handler, name='ai_summary_worker_' + str(i), daemon=True)
            t.start()
            self.worker_list.append(t)

        t = threading.Thread(target=self.refresh_lock, name='ai_summary_refresh_thread', daemon=True)
        t.start()
        self.worker_list.append(t)

    def init_worker_handler(self):
        logger.info('%s starting ai summary init worker', self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    init_res = self.mq.brpop(self.INIT_TASK_QUEUE, timeout=1)
                    if init_res is None:
                        continue

                    key, value = init_res
                    msg = value.split('\t')
                    if len(msg) != 3:
                        logger.info('Bad message: %s' % str(msg))
                    else:
                        op_type, repo_id, username = msg[0], msg[1], msg[2]
                        self.handle_init_task(repo_id, username)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s', e)
        except Exception as e:
            logger.error('%s Handle ai summary init worker task error', self.tname)
            logger.error(e, exc_info=True)
            time.sleep(0.3)

    def batch_worker_handler(self):
        logger.info('%s starting ai summary worker', self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop([self.HIGH_PRIORITY_TASK_QUEUE, self.LOW_PRIORITY_TASK_QUEUE], timeout=30)
                    if res is None:
                        continue

                    key, value = res
                    try:
                        data = json.loads(value)
                    except Exception:
                        data = None

                    if not data:
                        logger.warning('ai summary task queue: invalid task payload %s', res)
                        continue

                    repo_id = data.get('repo_id')
                    obj_ids = data.get('obj_ids')
                    is_init = bool(data.get('is_init'))
                    logger.debug('%s dequeued ai summary task queue=%s, repo=%s, is_init=%s, obj_count=%d',
                                 self.tname, key, repo_id, is_init, len(obj_ids) if isinstance(obj_ids, list) else 0)
                    self.handle_task(repo_id, obj_ids, is_init=is_init)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s', e)
        except Exception as e:
            logger.error('%s Handle ai summary worker task error', self.tname)
            logger.error(e, exc_info=True)
            time.sleep(0.3)

    def handle_task(self, repo_id, obj_ids, is_init=False):
        if not repo_id or not isinstance(obj_ids, list) or not obj_ids:
            logger.warning('ai_summary_task: invalid task, repo_id=%s obj_ids=%s', repo_id, obj_ids)
            return

        if self.should_stop.is_set():
            return

        lock_key = self._get_lock_key(repo_id)
        if not self.mq.set(lock_key, time.time(), ex=self.lock_timeout, nx=True):
            self.mq.rpush(self._get_task_queue(is_init), self._build_task_payload(repo_id, obj_ids, is_init=is_init))
            logger.debug('repo: %s ai summary is running, requeue this batch', repo_id)
            logger.debug('Requeued ai summary task repo=%s, queue=%s, obj_count=%d, retry_interval=%s',
                          repo_id, self._get_task_queue(is_init), len(obj_ids), self.lock_busy_retry_interval)
            time.sleep(self.lock_busy_retry_interval)
            return

        self.locked_keys.add(lock_key)
        logger.debug('%s acquired ai summary lock repo=%s, lock_key=%s', self.tname, repo_id, lock_key)
        try:
            logger.info('%s start ai summary batch repo %s, obj_count=%d', self.tname, repo_id, len(obj_ids))
            add_ai_summary(repo_id, obj_ids, self.metadata_server_api, self.seafile_ai_api)
            logger.info('%s finish ai summary batch repo %s, obj_count=%d', self.tname, repo_id, len(obj_ids))
            if is_init:
                self.ai_summary_manager.finish_init_ai_summary_batch(repo_id)
        except Exception as e:
            logger.exception('repo: %s, update metadata ai summary error: %s', repo_id, e)
        finally:
            try:
                self.locked_keys.remove(lock_key)
            except KeyError:
                logger.error('%s is already removed. SHOULD NOT HAPPEN!', lock_key)
            self.mq.delete(lock_key)
            logger.debug('%s released ai summary lock repo=%s, lock_key=%s', self.tname, repo_id, lock_key)

    def handle_init_task(self, repo_id, username=None):
        if not repo_id or self.should_stop.is_set():
            return

        init_lock_key = self._get_init_lock_key(repo_id)
        if not self.mq.set(init_lock_key, time.time(), ex=self.lock_timeout, nx=True):
            logger.info('repo: %s init ai summary is running, skip this task', repo_id)
            return

        self.locked_keys.add(init_lock_key)
        try:
            logger.info('%s start init ai summary repo %s', self.tname, repo_id)
            self.ai_summary_manager.init_ai_summary(repo_id, username=username, batch_size=self.batch_size)
            logger.info('%s Finish init ai summary repo: %s, delete redis lock %s', self.tname, repo_id, init_lock_key)
        except Exception as e:
            logger.exception('init ai summary repo: %s, error: %s', repo_id, e)
        finally:
            try:
                self.locked_keys.remove(init_lock_key)
            except KeyError:
                logger.error('%s is already removed. SHOULD NOT HAPPEN!', init_lock_key)
            self.mq.delete(init_lock_key)

    def refresh_lock(self):
        logger.info('%s Starting refresh locks', self.tname)
        while not self.should_stop.is_set():
            try:
                copy = self.locked_keys.copy()

                for lock in copy:
                    ttl = self.mq.ttl(lock)
                    new_ttl = ttl + self.refresh_interval
                    self.mq.expire(lock, new_ttl)
                    logger.debug('%s Refresh lock [%s] timeout from %s to %s' %
                                 (self.tname, lock, ttl, new_ttl))
                time.sleep(self.refresh_interval)
            except Exception as e:
                logger.exception(e)
                time.sleep(1)
