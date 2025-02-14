import time
import logging
import threading
import os
import signal
import gevent

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq
from seafevents.utils import get_opt_from_conf_or_env
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager


logger = logging.getLogger(__name__)
face_recognition_logger = logging.getLogger('face_recognition')


def patch_greenlet(f):
    def inner(*args, **kwargs):
        return gevent.spawn(f, *args, **kwargs)

    return inner


class RepoMetadataIndexWorker(object):
    """ The handler for redis message queue
    """

    def __init__(self, config):
        self._db_session_class = init_db_session_class(config)
        self.metadata_server_api = MetadataServerAPI('seafevents')

        self.should_stop = threading.Event()
        self.LOCK_TIMEOUT = 1800  # 30 minutes
        self.REFRESH_INTERVAL = 600
        self.locked_keys = set()
        self.mq_server = '127.0.0.1'
        self.mq_port = 6379
        self.mq_password = ''
        self.worker_num = 3
        self._parse_config(config)

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)
        self.face_recognition_manager = FaceRecognitionManager(config)
        self.set_signal()
        self.worker_list = []

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

    def _get_lock_key(self, repo_id):
        """Return lock key in redis.
        """
        return 'v2_' + repo_id

    def _get_face_cluster_lock_key(self, repo_id):
        return 'face_cluster_' + repo_id

    @property
    def tname(self):
        return threading.current_thread().name

    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All worker threads has stopped.")

    def start(self):
        for i in range(int(self.worker_num)):
            t = threading.Thread(target=self.face_cluster_handler, name='face_cluster_' + str(i), daemon=True)
            t.start()
            self.worker_list.append(t)

        t = threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True)
        t.start()
        self.worker_list.append(t)
        self.clear_worker()

    def face_cluster_handler(self):
        face_recognition_logger.info('%s starting face cluster' % self.tname)
        try:
            while not self.should_stop.isSet():
                try:
                    res = self.mq.brpop('face_cluster_task', timeout=30)
                    if res is not None:
                        key, value = res
                        msg = value.split('\t')
                        if len(msg) != 3:
                            face_recognition_logger.info('Bad message: %s' % str(msg))
                        else:
                            op_type, repo_id, username = msg[0], msg[1], msg[2]
                            self.face_cluster_task_handler(self.mq, repo_id, self.should_stop, op_type, username)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    face_recognition_logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            face_recognition_logger.error('%s Handle face cluster Task Error' % self.tname)
            face_recognition_logger.error(e, exc_info=True)
            # prevent case that redis break at program running.
            time.sleep(0.3)

    def face_cluster_task_handler(self, mq, repo_id, should_stop, op_type, username=None):
        # Python cannot kill threads, so stop it generate more locked key.
        if not should_stop.isSet():
            # set key-value if does not exist which will expire 30 minutes later
            if mq.set(self._get_face_cluster_lock_key(repo_id), time.time(),
                      ex=self.LOCK_TIMEOUT, nx=True):
                # get lock
                face_recognition_logger.info('%s start face cluster repo %s' % (threading.current_thread().name, repo_id))
                lock_key = self._get_face_cluster_lock_key(repo_id)
                self.locked_keys.add(lock_key)
                self.update_face_cluster(repo_id, username)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    face_recognition_logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                mq.delete(lock_key)
                face_recognition_logger.info("%s Finish updating repo: %s, delete redis lock %s" %
                            (self.tname, repo_id, lock_key))
            else:
                # the repo is clustered by other thread, skip it
                face_recognition_logger.info('repo: %s face cluster is running, skip this clustering', repo_id)

    def update_face_cluster(self, repo_id, username):
        try:
            self.face_recognition_manager.update_face_cluster(repo_id, username=username)
        except Exception as e:
            face_recognition_logger.exception('update repo: %s metadata error: %s', repo_id, e)

    def refresh_lock(self):
        logger.info('%s Starting refresh locks' % self.tname)
        while True:
            try:
                # workaround for the RuntimeError: Set changed size during iteration
                copy = self.locked_keys.copy()

                for lock in copy:
                    ttl = self.mq.ttl(lock)
                    new_ttl = ttl + self.REFRESH_INTERVAL
                    self.mq.expire(lock, new_ttl)
                    logger.debug('%s Refresh lock [%s] timeout from %s to %s' %
                                 (self.tname, lock, ttl, new_ttl))
                time.sleep(self.REFRESH_INTERVAL)
            except Exception as e:
                logger.error(e)
                time.sleep(1)

    @patch_greenlet
    def clear(self):
        self.should_stop.set()
        # if a thread just lock key, wait to add the lock to the list.
        time.sleep(1)
        # del redis locked key
        for key in self.locked_keys:
            self.mq.delete(key)
            logger.info("redis lock key %s has been deleted" % key)
        # sys.exit
        logger.info("Exit the process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        # TODO: look like python will add signal to queue when cpu exec c extension code,
        # and will call signal callback method after cpu exec python code
        # ref: https://docs.python.org/2/library/signal.html
        signal.signal(signal.SIGTERM, self.signal_term_handler)
