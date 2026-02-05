
import json
import time
import logging
from threading import Thread, Event
from seafevents.app.event_redis import redis_cache, RedisClient, REDIS_METRIC_KEY
from seafobj import storage_cache_clear

REPO_STORAGE_TASK_CHANNEL = "repo_storage_task"

class RepoStorageTask(Thread):
    """
    Collect repo storage tasks from redis channel and process them
    """
    def __init__(self):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_client = RedisClient()

    def run(self):
        logging.info('Starting handle repo storage task redis channel')
        if not self._redis_client.connection:
            logging.warning('Can not start repo storage task handler: redis connection is not initialized')
            return
        subscriber = self._redis_client.get_subscriber(REPO_STORAGE_TASK_CHANNEL)

        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    data = json.loads(message['data'])
                    repo_id = data.get('repo_id', None)
                    try:
                        if repo_id:
                            storage_cache_clear(repo_id)
                    except Exception as e:
                        logging.error('Handle repo storage task failed: %s' % e)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logging.error('Failed handle repo storage task: %s' % e)
                subscriber = self._redis_client.get_subscriber(REPO_STORAGE_TASK_CHANNEL)
