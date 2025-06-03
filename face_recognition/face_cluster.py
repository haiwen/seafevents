import os
import time
import logging
import argparse
import threading
import signal
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq
from seafevents.utils import get_opt_from_conf_or_env
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.face_recognition.face_recognition_manager import FaceRecognitionManager
from seafevents.app.config import get_config
from seafevents.app.log import LogConfigurator
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger('face_recognition')


class FaceCluster(object):
    """ The handler for redis message queue
    """

    def __init__(self, config):
        self._db_session_class = init_db_session_class()
        self.metadata_server_api = MetadataServerAPI('seafevents')

        self.should_stop = threading.Event()
        self.LOCK_TIMEOUT = 1800  # 30 minutes
        self.REFRESH_INTERVAL = 600
        self.locked_keys = set()
        self.mq_server = REDIS_HOST
        self.mq_port = REDIS_PORT
        self.mq_password = REDIS_PASSWORD
        self.worker_num = 3
        self._parse_config(config)

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)
        self.face_recognition_manager = FaceRecognitionManager(config)
        self.set_signal()
        self.worker_list = []

    def _parse_config(self, config):
        metadata_section_name = 'METADATA'
        key_index_workers = 'index_workers'
        if config.has_section(metadata_section_name):
            self.worker_num = get_opt_from_conf_or_env(config, metadata_section_name, key_index_workers, default=3)

    def _get_face_cluster_lock_key(self, repo_id):
        return 'face_cluster_' + repo_id

    @property
    def tname(self):
        return threading.current_thread().name

    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All face cluster worker threads has stopped.")

    def start(self):
        if not self.mq:
            return
        for i in range(int(self.worker_num)):
            t = threading.Thread(target=self.face_cluster_handler, name='face_cluster_' + str(i), daemon=True)
            t.start()
            self.worker_list.append(t)

        t = threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True)
        t.start()
        self.worker_list.append(t)
        self.clear_worker()

    def face_cluster_handler(self):
        logger.info('%s starting face cluster', self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('face_cluster_task', timeout=30)
                    if res is not None:
                        key, value = res
                        msg = value.split('\t')
                        if len(msg) != 3:
                            logger.info('Bad message: %s' % str(msg))
                        else:
                            op_type, repo_id, username = msg[0], msg[1], msg[2]
                            self.face_cluster_task_handler(self.mq, repo_id, self.should_stop, op_type, username)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s Handle face cluster Task Error' % self.tname)
            logger.error(e, exc_info=True)
            # prevent case that redis break at program running.
            time.sleep(0.3)

    def face_cluster_task_handler(self, mq, repo_id, should_stop, op_type, username=None):
        # Python cannot kill threads, so stop it generate more locked key.
        if not should_stop.is_set():
            # set key-value if does not exist which will expire 30 minutes later
            if mq.set(self._get_face_cluster_lock_key(repo_id), time.time(),
                      ex=self.LOCK_TIMEOUT, nx=True):
                # get lock
                logger.info('%s start face cluster repo %s' % (self.tname, repo_id))
                lock_key = self._get_face_cluster_lock_key(repo_id)
                self.locked_keys.add(lock_key)
                self.update_face_cluster(repo_id, username)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error("%s is already removed. SHOULD NOT HAPPEN!" % lock_key)
                mq.delete(lock_key)
                logger.info("%s Finish clustering face repo: %s, delete redis lock %s" % (self.tname, repo_id, lock_key))
            else:
                # the repo is clustering by other thread, skip it
                logger.info('repo: %s face cluster is running, skip this clustering', repo_id)

    def update_face_cluster(self, repo_id, username):
        try:
            self.face_recognition_manager.update_face_cluster(repo_id, username=username)
        except Exception as e:
            logger.exception('update face cluster repo: %s, error: %s', repo_id, e)

    def refresh_lock(self):
        logger.info('%s Starting refresh locks', self.tname)
        while not self.should_stop.is_set():
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
                logger.exception(e)
                time.sleep(1)

    def clear(self):
        if not self.mq:
            return
        self.should_stop.set()
        # if a thread just lock key, wait to add the lock to the list.
        time.sleep(1)
        # del redis locked key
        for key in self.locked_keys:
            self.mq.delete(key)
            logger.info("redis lock key %s has been deleted", key)
        # sys.exit
        logger.info("Exit face cluster process")
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        # TODO: look like python will add signal to queue when cpu exec c extension code,
        # and will call signal callback method after cpu exec python code
        # ref: https://docs.python.org/2/library/signal.html
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start(config):
    face_cluster = FaceCluster(config)
    logger.info("face cluster worker process initialized.")
    try:
        face_cluster.start()
    except Exception as e:
        logger.exception(e)
        face_cluster.clear()

    while True:
        # if main thread has been quit or join for subthread.
        # signal callback will never be  call.
        time.sleep(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', default=os.path.join(os.getcwd(), 'events.conf'), help='config file')
    parser.add_argument('--logfile', help='log file')
    parser.add_argument('--loglevel', default='info', help='log level')
    args = parser.parse_args()

    config_file = args.config_file
    config = get_config(config_file)
    LogConfigurator(args.loglevel, args.logfile)
    start(config)


if __name__ == "__main__":
    main()
