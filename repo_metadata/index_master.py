import json
import time
import logging
from threading import Thread
from collections import OrderedDict
from copy import deepcopy

from seafevents.mq import get_mq
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger(__name__)


class RepoMetadataIndexMaster(Thread):
    """ Publish the news of the events obtained from ccnet
    """
    def __init__(self, config):
        Thread.__init__(self)
        self.mq_server = REDIS_HOST
        self.mq_port = REDIS_PORT
        self.mq_password = REDIS_PASSWORD

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)
        self.pending_tasks = OrderedDict()  # repo_id: commit_id

    def now(self):
        return time.time()

    def run(self):
        if not self.mq:
            return
        logger.info('metadata master event receive thread started')
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
            # get all messages
            while True:
                message = p.get_message()
                if not message:
                    break

                try:
                    data = json.loads(message['data'])
                except:
                    logger.warning('index master message: invalid.', message)
                    data = None

                if data:
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
                    else:
                        logger.warning('op_type invalid, repo_id: %s, op_type: %s' % (repo_id, op_type))

            # check task
            if len(self.pending_tasks) > 0:
                copied_pending_tasks = deepcopy(self.pending_tasks)
                for repo_id, commit_id in copied_pending_tasks.items():
                    op_type = 'update-metadata'
                    data = op_type + '\t' + repo_id
                    self.mq.lpush('metadata_task', data)
                    self.pending_tasks.pop(repo_id)

            time.sleep(0.1)
