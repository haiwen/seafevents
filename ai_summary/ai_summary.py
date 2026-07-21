import os
import time
import logging
import argparse
import threading
import signal
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq
from seafevents.ai_summary.ai_summary_manager import AISummaryManager
from seafevents.ai_summary.ai_summary_worker import AISummaryTaskWorker
from seafevents.app.config import get_config
from seafevents.app.log import LogConfigurator
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, AI_SUMMARY_INIT_WORKERS, AI_SUMMARY_BATCH_SIZE

logger = logging.getLogger('ai_summary')


class AISummary(object):
    """ The handler for ai summary init queue
    """

    def __init__(self, config):
        self.config = config
        self.should_stop = threading.Event()
        self.LOCK_TIMEOUT = 1800
        self.REFRESH_INTERVAL = 600
        self.locked_keys = set()
        self.mq_server = REDIS_HOST
        self.mq_port = REDIS_PORT
        self.mq_password = REDIS_PASSWORD
        self.init_worker_num = 1
        self.batch_size = 50
        self._parse_config(config)

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)
        self.ai_summary_manager = AISummaryManager()
        self.ai_summary_task_worker = AISummaryTaskWorker(config, self.should_stop, self.locked_keys)
        self.set_signal()
        self.worker_list = []

    def _parse_config(self, config):
        self.init_worker_num = AI_SUMMARY_INIT_WORKERS
        self.batch_size = AI_SUMMARY_BATCH_SIZE

    def _get_init_ai_summary_lock_key(self, repo_id):
        return 'init_ai_summary_' + repo_id

    @property
    def tname(self):
        return threading.current_thread().name

    def clear_worker(self):
        for th in self.ai_summary_task_worker.worker_list:
            th.join()
        for th in self.worker_list:
            th.join()
        logger.info('All ai summary worker threads has stopped.')

    def start(self):
        if not self.mq:
            return

        self.ai_summary_task_worker.start()

        for i in range(int(self.init_worker_num)):
            t = threading.Thread(target=self.init_ai_summary_handler, name='init_ai_summary_' + str(i), daemon=True)
            t.start()
            self.worker_list.append(t)

        t = threading.Thread(target=self.refresh_lock, name='refresh_thread', daemon=True)
        t.start()
        self.worker_list.append(t)
        self.clear_worker()

    def init_ai_summary_handler(self):
        logger.info('%s starting init ai summary', self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('init_ai_summary_task', timeout=30)
                    if res is not None:
                        key, value = res
                        msg = value.split('\t')
                        if len(msg) != 3:
                            logger.info('Bad message: %s' % str(msg))
                        else:
                            op_type, repo_id, username = msg[0], msg[1], msg[2]
                            self.ai_summary_task_handler(self.mq, repo_id, self.should_stop, op_type, username)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s' % e)
        except Exception as e:
            logger.error('%s Handle init ai summary task error' % self.tname)
            logger.error(e, exc_info=True)
            time.sleep(0.3)

    def ai_summary_task_handler(self, mq, repo_id, should_stop, op_type, username=None):
        if not should_stop.is_set():
            if mq.set(self._get_init_ai_summary_lock_key(repo_id), time.time(), ex=self.LOCK_TIMEOUT, nx=True):
                logger.info('%s start init ai summary repo %s' % (self.tname, repo_id))
                lock_key = self._get_init_ai_summary_lock_key(repo_id)
                self.locked_keys.add(lock_key)
                self.init_ai_summary(repo_id, username)
                try:
                    self.locked_keys.remove(lock_key)
                except KeyError:
                    logger.error('%s is already removed. SHOULD NOT HAPPEN!' % lock_key)
                mq.delete(lock_key)
                logger.info('%s Finish init ai summary repo: %s, delete redis lock %s' % (self.tname, repo_id, lock_key))
            else:
                logger.info('repo: %s init ai summary is running, skip this task', repo_id)

    def init_ai_summary(self, repo_id, username=None):
        try:
            self.ai_summary_manager.init_ai_summary(repo_id, username=username, batch_size=self.batch_size)
        except Exception as e:
            logger.exception('init ai summary repo: %s, error: %s', repo_id, e)

    def refresh_lock(self):
        logger.info('%s Starting refresh locks', self.tname)
        while not self.should_stop.is_set():
            try:
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
        time.sleep(1)
        for key in self.locked_keys:
            self.mq.delete(key)
            logger.info('redis lock key %s has been deleted', key)
        logger.info('Exit ai summary process')
        os._exit(0)

    def signal_term_handler(self, signal, frame):
        self.clear()

    def set_signal(self):
        signal.signal(signal.SIGTERM, self.signal_term_handler)


def start(config):
    ai_summary = AISummary(config)
    logger.info('ai summary worker process initialized.')
    try:
        ai_summary.start()
    except Exception as e:
        logger.exception(e)
        ai_summary.clear()

    while True:
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
