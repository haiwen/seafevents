import json
import time
import logging
from threading import Thread, Event
from mq import NoMessageException
from seafevents.mq import get_mq
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seafevents.app.event_redis import RedisClient
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
    Additions: Auto recreate Redis connection when no message received for a long time, 
    provide a graceful stop interface for external calls.
    """
    def __init__(self):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_client = RedisClient()
        # Set as daemon thread: auto destroy when main process exits to avoid zombie threads
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)


    def stop(self):
        """Graceful stop interface for external calls: trigger thread stop by setting the finish Event"""
        logging.info('Try to stop repo storage task handler thread')
        self._finished.set()

    def _create_redis_subscriber(self):
        """
        Extract subscriber creation logic for reusability, add Redis connection validation.
        Return: Redis subscriber object if created successfully, None otherwise
        """
        if not self._redis_client or not self._redis_client.connection:
            logging.warning('Redis connection is not initialized, cannot create channel subscriber')
            return None
        try:
            subscriber = self._redis_client.get_subscriber(REPO_STORAGE_TASK_CHANNEL)
            logging.info(f'Successfully created redis subscriber for channel: {REPO_STORAGE_TASK_CHANNEL}')
            return subscriber
        except Exception as e:
            logging.error(f'Failed to create redis subscriber: {str(e)}', exc_info=True)
            return None
        
    def handle_repo_storage_task(self):
        """
        Main entry point for repo storage task handling.
        This method is called by the main process to start the thread.
        """
        p = self.mq.pubsub(ignore_subscribe_messages=True)
        p.subscribe(REPO_STORAGE_TASK_CHANNEL)
        logging.info('repo storage task handler starting listen')
        message_check_time = time.time()
        while not self._finished.is_set():
            message = p.get_message()
            if message is not None:
                message_check_time = time.time()
                data = json.loads(message['data'])
                repo_id = data.get('repo_id', None)
                if repo_id:
                    try:
                        storage_cache_clear(repo_id)
                        logging.debug(f'Successfully cleared storage cache for repo: {repo_id}')  # Short ID for concise log
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
                time.sleep(1)  # Short sleep to avoid CPU idle loop when no timeout


    def run(self):
        logging.info('Starting to handle repo storage task from redis channel')
        if not self.mq:
            logging.warning('Cannot start repo storage task handler: redis connection is not initialized')
            return
        while not self._finished.is_set():
            try:
                self.handle_repo_storage_task()
            except NoMessageException:
                logging.warning('Long time no message, reconnecting to redis.')
            except Exception as e:
                logging.error(f'Error in repo storage task handler: {str(e)}', exc_info=True)
            # Sleep before next reconnection attempt to avoid tight loop in case of persistent connection issues
            time.sleep(1)    
    def run_1(self):
        logging.info('Starting to handle repo storage task from redis channel')
        # Initial Redis connection validation before running the loop
        if not self._redis_client.connection:
            logging.warning('Cannot start repo storage task handler: redis connection is not initialized')
            return

        # Outer loop: responsible for Redis connection re-creation
        while not self._finished.is_set():
            # Initialize/reset the timestamp of the last received message (reset on each reconnection)
            last_msg_time = time.time()
            # Create Redis subscriber with the extracted method
            subscriber = self._create_redis_subscriber()
            # Retry after sleep if subscriber creation failed
            if not subscriber:
                logging.warning('Failed to create redis subscriber, retrying after sleep')
                time.sleep(RECONNECT_SLEEP)
                continue

            # Inner loop: responsible for message consumption under a single connection, exit on timeout/exception
            while not self._finished.is_set():
                try:
                    message = subscriber.get_message()
                    # 1. Process valid received message and clear storage cache
                    if message is not None:
                        # Filter Redis initial subscription confirmation message (empty data from some Redis clients)
                        if message.get('data') in [None, b'']:
                            continue
                        # Parse message data and handle cache clear
                        data = json.loads(message['data'])
                        repo_id = data.get('repo_id', None)
                        if repo_id:
                            try:
                                storage_cache_clear(repo_id)
                                logging.debug(f'Successfully cleared storage cache for repo: {repo_id}')  # Short ID for concise log
                            except Exception as e:
                                logging.error(f'Failed to handle repo storage task for repo {repo_id}: {str(e)}')
                        # Core: Update last message timestamp to mark normal connection status
                        last_msg_time = time.time()
                    # 2. No message received, check if timeout is triggered
                    else:
                        # Calculate the duration of no message reception
                        no_msg_duration = time.time() - last_msg_time
                        # Core: Timeout trigger - exit inner loop and recreate Redis connection
                        if no_msg_duration > NO_MSG_TIMEOUT:
                            logging.warning(
                                f'No message received for {no_msg_duration:.1f}s (timeout threshold: {NO_MSG_TIMEOUT}s), '
                                f'attempting to recreate redis connection'
                            )
                            break  # Exit inner loop and return to outer loop for reconnection

                        # Short sleep to avoid CPU idle loop when no timeout
                        time.sleep(0.5)

                except json.JSONDecodeError as e:
                    logging.error(f'Failed to parse repo storage task message: invalid JSON format, error: {str(e)}', exc_info=True)
                    time.sleep(0.5)
                except Exception as e:
                    # Catch other Redis/network exceptions, log and recreate connection
                    logging.error(f'Unexpected error in message handling loop: {str(e)}', exc_info=True)
                    break  # Exit inner loop and return to outer loop for reconnection

            # Sleep before reconnection to avoid frequent retries after inner loop exit
            if not self._finished.is_set():
                time.sleep(RECONNECT_SLEEP)

        # Log after thread stops normally
        logging.info('Repo storage task handler thread stopped successfully')
