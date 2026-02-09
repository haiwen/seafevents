import json
import time
import logging
from threading import Thread
from seafevents.mq import get_mq, NoMessageException
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seafobj import storage_cache_clear

# Redis subscribe channel for repository storage tasks
REPO_STORAGE_TASK_CHANNEL = "repo_storage_task"
# No message timeout threshold: 30 seconds (adjustable by business, recommended to configure in the config file)
NO_MSG_TIMEOUT = 5 * 60
# Redis reconnection sleep time: 1 second to avoid frequent reconnection attempts
RECONNECT_SLEEP = 1


class RepoStorageTask(Thread):
    """
    Collect repo storage tasks from Redis pub/sub channel and process cache clearing logic.
    Additions: Auto recreate Redis connection when no message received for a long time
    """
    def __init__(self):
        Thread.__init__(self)
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
    def handle_repo_storage_task(self):
        """
        Main entry point for repo storage task handling.
        This method is called by the main process to start the thread.
        """
        p = self.mq.pubsub(ignore_subscribe_messages=True)
        p.subscribe(REPO_STORAGE_TASK_CHANNEL)
        logging.info('repo storage task handler starting listen')
        message_check_time = time.time()
        while True:
            message = p.get_message()
            if message is not None:
                message_check_time = time.time()
                data = json.loads(message['data'])
                repo_id = data.get('repo_id', None)
                if repo_id:
                    try:
                        storage_cache_clear(repo_id)
                        logging.info(f'Successfully cleared storage cache for repo: {repo_id}')  # Short ID for concise log
                    except Exception as e:
                        logging.error(f'Failed to handle repo storage task for repo {repo_id}: {str(e)}')

            if not message:
                no_msg_duration = time.time() - message_check_time
                if no_msg_duration > NO_MSG_TIMEOUT:
                    logging.warning(
                        f'No message received for {no_msg_duration:.1f}s (timeout threshold: {NO_MSG_TIMEOUT}s), '
                        f'attempting to recreate redis connection'
                    )
                    raise NoMessageException('No message received for a long time, trigger redis reconnection')
                time.sleep(RECONNECT_SLEEP)  # Short sleep to avoid CPU idle loop when no timeout

    def run(self):
        logging.info('Starting to handle repo storage task from redis channel')
        if not self.mq:
            logging.warning('Cannot start repo storage task handler: redis connection is not initialized')
            return
        while True:
            try:
                self.handle_repo_storage_task()
            except NoMessageException:
                logging.warning('Long time no message, reconnecting to redis.')
            except Exception as e:
                logging.error(f'Error in repo storage task handler: {str(e)}', exc_info=True)
            # Sleep before next reconnection attempt to avoid tight loop in case of persistent connection issues
            time.sleep(RECONNECT_SLEEP)    
