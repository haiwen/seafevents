import json
import time
import logging
from threading import Thread
from collections import OrderedDict
from copy import deepcopy

from seafevents.mq import get_mq
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger(__name__)


# 用于监听 Redis 消息队列并处理仓库元数据更新的线程（其目的就是监听消息队列并触发更新。）。
class RepoMetadataIndexMaster(Thread):
    """ Publish the news of the events obtained from ccnet
    """
    # 初始化线程，配置 Redis 连接，并解析配置以覆盖默认的 Redis 设置。
    def __init__(self, config):
        Thread.__init__(self)
        self.mq_server = REDIS_HOST
        self.mq_port = REDIS_PORT
        self.mq_password = REDIS_PASSWORD

        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)
        self.pending_tasks = OrderedDict()  # repo_id: commit_id

    def now(self):
        return time.time()

    # 启动线程并持续调用 master_handler 方法来处理元数据更新。
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

    # 核心方法：监听 Redis 消息队列中的元数据更新，处理传入的消息，并根据需要更新待处理任务。
    # 这个代码片段是类方法`master_handler`的一部分，它处理来自Redis消息队列的元数据更新。以下是简要的解释：
    def master_handler(self):
        p = self.mq.pubsub(ignore_subscribe_messages=True)

        # 1. 它订阅了`metadata_update`频道并监听传入的消息。
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

                # 2. 对于每个消息，它从消息数据中提取操作类型（`op_type`）、存储库ID（`repo_id`）和提交ID（`commit_id`）。
                if data:
                    op_type = data.get('msg_type')
                    repo_id = data.get('repo_id')
                    commit_id = data.get('commit_id')

                    # 3. 根据`op_type`，它执行以下操作：
                    #     * `init-metadata`：将任务添加到`metadata_task`队列中。
                    #     * `repo-update`：更新待处理任务字典中的新提交ID。
                    #     * `update_face_recognition`：将任务添加到`face_cluster_task`队列中。
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

            # 4. 如果有待处理任务，它会通过将它们添加到`metadata_task`队列中并从待处理任务字典中删除它们来处理它们。
            # check task
            if len(self.pending_tasks) > 0:
                copied_pending_tasks = deepcopy(self.pending_tasks)
                for repo_id, commit_id in copied_pending_tasks.items():
                    op_type = 'update-metadata'
                    data = op_type + '\t' + repo_id
                    self.mq.lpush('metadata_task', data)
                    self.pending_tasks.pop(repo_id)

            # 5. 循环运行无限次，每次迭代之间有0.1秒的延迟。
            time.sleep(0.1)
