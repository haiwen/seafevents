import time
import logging
from threading import Thread

from collections import OrderedDict

from seafevents.mq import get_mq
from seafevents.utils import get_opt_from_conf_or_env

logger = logging.getLogger(__name__)


class RepoMetadataIndexMaster(object):
    """ Publish the news of the events obtained from ccnet 
    """
    def __init__(self, config):
        self.mq_server = '127.0.0.1'
        self.mq_port = 6379
        self.mq_password = ''
        self._interval = 5 * 60
        self._parse_config(config)

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)
        self.executed_tasks = OrderedDict()  # repo_id: event_add_time
        self.pending_tasks = OrderedDict()  # repo_id: event_add_time
        self.max_updated_num_limit = 1000  # max repo num limit

    def _parse_config(self, config):
        section_name = 'REDIS'
        key_server = 'server'
        key_port = 'port'
        key_password = 'password'

        if not config.has_section(section_name):
            return

        self.mq_server = get_opt_from_conf_or_env(config, section_name, key_server, default='')
        self.mq_port = get_opt_from_conf_or_env(config, section_name, key_port, default=6379)
        self.mq_password = get_opt_from_conf_or_env(config, section_name, key_password, default='')

    def start(self):
        Thread(target=self.task_check, name='task_check_thread', daemon=True).start()
        Thread(target=self.event_receive, name='event_receive_thread', daemon=True).start()

    def task_check(self):
        logger.info('metadata master task check start')

        while True:
            task_num = len(self.pending_tasks)
            now_time = time.time()
            n = 0
            while n < task_num:
                repo_id, event_time = self.pending_tasks.popitem(last=False)

                last_updated_time = self.executed_tasks.get(repo_id, 0)

                op_type = 'update_metadata'
                data = op_type + '\t' + repo_id
                if (last_updated_time + self._interval) < now_time:
                    self.mq.lpush('metadata_task', data)
                    self.executed_tasks[repo_id] = time.time()
                    if len(self.executed_tasks) > self.max_updated_num_limit:
                        self.executed_tasks.popitem(last=False)
                else:
                    self.pending_tasks[repo_id] = event_time

                n += 1

            time.sleep(self._interval)

    def event_receive(self):
        logger.info('metadata master starting work')
        while True:
            try:
                self.master_handler()
            except Exception as e:
                logger.error('Error handing master task: %s' % e)
                #prevent waste resource if redis or others didn't connectioned
                time.sleep(0.2)

    def master_handler(self):
        p = self.mq.pubsub(ignore_subscribe_messages=True)
        try:
            p.subscribe('metadata_update')
        except Exception as e:
            logger.error('The connection to the redis server failed: %s' % e)
        else:
            logger.info('metadata master starting listen')
        while True:
            message = p.get_message()

            if message is None:
                # prevent waste resource when no message has been send
                time.sleep(5)
                continue

            if not isinstance(message['data'], str) or message['data'].count('\t') != 2:
                continue

            msg = message['data'].split('\t')
            op_type, repo_id, commit_id = msg[0], msg[1], msg[2]
            if op_type == 'init_metadata':
                data = op_type + '\t' + repo_id
                self.mq.lpush('metadata_task', data)
            elif op_type == 'repo-update':
                self.pending_tasks[repo_id] = time.time()
            else:
                logger.warning('op_type invalid, repo_id: %s, op_type: %s' % (repo_id, op_type))

            logger.debug('%s has been add to metadata task queue' % message['data'])
