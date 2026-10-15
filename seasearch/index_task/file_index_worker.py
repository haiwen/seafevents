import json
import logging
import time
from threading import Thread, Event

from seaserv import seafile_api

from seafevents.app.event_redis import RedisClient
from seafevents.seasearch.utils.constants import REPO_TYPE_WIKI


logger = logging.getLogger('seasearch')


class RepoFileIndexWorker(object):
    def __init__(self, index_manager, repo_file_index, repo_status_file_index, lock):
        self.index_manager = index_manager
        self.repo_file_index = repo_file_index
        self.repo_status_file_index = repo_status_file_index
        self.mq = RedisClient().connection
        self.lock = lock
        self.finished = Event()
        self.pending_repos = {}

    def start(self):
        if not self.mq:
            logger.warning('Can not start realtime file indexing: Redis has not been set up')
            return

        Thread(target=self.handle_events, name='seasearch_file_index_worker', daemon=True).start()

    def handle_events(self):
        while not self.finished.is_set():
            try:
                with self.mq.pubsub(ignore_subscribe_messages=True) as subscriber:
                    subscriber.subscribe('repo_update')
                    while not self.finished.is_set():
                        message = subscriber.get_message(timeout=0.5)
                        if message:
                            self.handle_message(message['data'])
                        now = time.monotonic()
                        for repo_id, retry_time in list(self.pending_repos.items()):
                            if self.finished.is_set():
                                break
                            if retry_time <= now:
                                self.update_repo_index(repo_id)
            except Exception as e:
                logger.exception('Failed to subscribe to realtime file index events: %s', e)
                self.finished.wait(1)

    def handle_message(self, value):
        try:
            data = json.loads(value)
        except (TypeError, ValueError):
            logger.warning('Invalid file index event: %s', value)
            return

        if not isinstance(data, dict) or data.get('msg_type') != 'repo-update':
            return
        repo_id = data.get('repo_id')
        if not repo_id or not data.get('commit_id'):
            return

        self.update_repo_index(repo_id)

    def update_repo_index(self, repo_id):
        try:
            with self.lock:
                # Read the current head after waiting for the periodic updater.
                repo = seafile_api.get_repo(repo_id)
                if not repo or repo.repo_type == REPO_TYPE_WIKI or repo.is_virtual:
                    self.pending_repos.pop(repo_id, None)
                    return
                updated = self.index_manager.update_library_file_index(
                    repo_id, repo.head_cmmt_id, self.repo_file_index,
                    self.repo_status_file_index, None, update_metadata=False
                )
        except Exception as e:
            logger.exception('Failed to update realtime file index for repo %s: %s', repo_id, e)
            updated = False

        if updated:
            self.pending_repos.pop(repo_id, None)
        else:
            self.pending_repos[repo_id] = time.monotonic() + 1
