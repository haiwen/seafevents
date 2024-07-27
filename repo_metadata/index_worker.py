import time
import logging
import threading

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.repo_data import repo_data
from seafevents.mq import get_mq
from seafevents.utils import get_opt_from_conf_or_env
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.repo_metadata import RepoMetadata
from seafevents.repo_metadata.metadata_manager import MetadataManager

logger = logging.getLogger(__name__)


class RepoMetadataIndexWorker(object):
    """ The handler for redis message queue
    """

    def __init__(self, config):
        self._db_session_class = init_db_session_class(config)
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.repo_metadata = RepoMetadata(self.metadata_server_api)

        self.metadata_manager = MetadataManager(self._db_session_class, self.repo_metadata)

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

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        for i in range(int(self.worker_num)):
            threading.Thread(target=self.worker_handler, name='subscribe_' + str(i),
                              daemon=True).start()
        threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True).start()

    def worker_handler(self):
        logger.info('%s starting update metadata work' % self.tname)
        try:
            while not self.should_stop.isSet():
                try:
                    res = self.mq.brpop('metadata_task', timeout=30)
                    if res is not None:
                        key, value = res
                        msg = value.split('\t')
                        if len(msg) != 2:
                            logger.info('Bad message: %s' % str(msg))
                        else:
                            op_type, repo_id = msg[0], msg[1]
                            self.worker_task_handler(self.mq, repo_id, self.should_stop)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s Handle Worker Task Error' % self.tname)
            logger.error(e, exc_info=True)
            # prevent case that redis break at program running.
            time.sleep(0.3)

    def worker_task_handler(self, mq, repo_id, should_stop):
        # Python cannot kill threads, so stop it generate more locked key.
        if not should_stop.isSet():
            # set key-value if does not exist which will expire 30 minutes later
            if mq.set(self._get_lock_key(repo_id), time.time(),
                      ex=self.LOCK_TIMEOUT, nx=True):
                # get lock
                logger.info('%s start updating repo %s' %
                            (threading.currentThread().getName(), repo_id))
                lock_key = self._get_lock_key(repo_id)
                self.locked_keys.add(lock_key)
                self.update_metadata(repo_id)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                mq.delete(lock_key)
                logger.info("%s Finish updating repo: %s, delete redis lock %s" %
                            (self.tname, repo_id, lock_key))
            else:
                # the repo is updated by other thread, push back to the queue
                self.add_to_undo_task(mq, repo_id)

    def update_metadata(self, repo_id):
        commit_id = repo_data.get_repo_head_commit(repo_id)
        if not commit_id:
            # invalid repo without head commit id
            logger.error("invalid repo : %s " % repo_id)
            return
        try:
            self.metadata_manager.update_metadata(repo_id, commit_id)
        except Exception as e:
            logger.exception('update repo: %s metadata error: %s', repo_id, e)

    def add_to_undo_task(self, mq, repo_id):
        """Push task back to the end of the queue.
        """
        mq.lpush('metadata_task', '\t'.join(['repo-update', repo_id]))
        logger.debug('%s push back task (%s,) to the queue' %
                     (self.tname, repo_id))

        # avoid get the same task repeatedly
        time.sleep(0.5)

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
