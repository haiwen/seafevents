import os
import time
import logging
import threading
from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError

from seafevents.mq import get_mq
from seafevents.ai_summary.ai_summary_manager import AISummaryManager
from seafevents.ai_summary.ai_summary_worker import AISummaryTaskWorker
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, AI_SUMMARY_WORKERS, AI_SUMMARY_LOCK_BUSY_RETRY_INTERVAL

logger = logging.getLogger('ai_summary')


class AISummary(object):
    """ The handler for ai summary init queue
    """

    def __init__(self):
        self.should_stop = threading.Event()
        self.ai_summary_task_worker = AISummaryTaskWorker(self.should_stop)

    def start(self):
        self.ai_summary_task_worker.start()
        logger.info('AI summary task worker started with %d threads', len(self.ai_summary_task_worker.worker_list))

    def clear(self):
        self.should_stop.set()
        logger.info('Exit ai summary worker')
