import json
import logging
import os
import time
from threading import Event, Thread, Lock, local, current_thread
from urllib.parse import urlsplit

from seafevents.app.config import ENABLE_SEARCH, IS_PRO_VERSION, SEARCH_ENGINE, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seafevents.mq import get_mq
from seafevents.repo_data import repo_data
from seafevents.seasearch.index_store.index_manager import IndexManager
from seafevents.seasearch.index_store.repo_file_index import RepoFileIndex
from seafevents.seasearch.index_store.repo_status_index import RepoStatusIndex
from seafevents.seasearch.index_store.wiki_index import WikiIndex
from seafevents.seasearch.index_store.wiki_status_index import WikiStatusIndex
from seafevents.seasearch.index_task.index_scheduler import IndexScheduler
from seafevents.seasearch.utils.constants import REPO_STATUS_FILE_INDEX_NAME, WIKI_STATUS_INDEX_NAME, REPO_TYPE_WIKI, SHARD_NUM, REPO_FILE_INDEX_PREFIX, WIKI_INDEX_PREFIX
from seafevents.seasearch.utils.seasearch_api import SeaSearchAPI
from seafevents.utils import get_opt_from_conf_or_env

logger = logging.getLogger('seasearch')


class SeasearchIndexManger(object):
    SUBSCRIPTION_IDLE_TIMEOUT = 5 * 60

    def __init__(self, config):
        self.enabled = IS_PRO_VERSION and ENABLE_SEARCH and SEARCH_ENGINE == 'seasearch'
        self.config = config
        self.finished = Event()
        self.scheduler = IndexScheduler()
        self.threads = []
        self._worker_state = local()
        self._initialization_lock = Lock()
        self.disabled_kinds = set()
        try:
            self.interval = int(os.environ.get('SEASEARCH_INDEX_INTERVAL', '86400'))
            if self.interval <= 0:
                raise ValueError()
        except ValueError:
            logger.warning('Invalid SEASEARCH_INDEX_INTERVAL; using 86400 seconds')
            self.interval = 86400
        try:
            self.worker_count = int(os.environ.get('SEASEARCH_INDEX_WORKERS', '2'))
            if self.worker_count <= 0:
                raise ValueError()
        except ValueError:
            logger.warning('Invalid SEASEARCH_INDEX_WORKERS; using 2 workers')
            self.worker_count = 2
        if self.enabled:
            self._parse_config()
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    @property
    def indexes(self):
        if not hasattr(self._worker_state, 'indexes'):
            self._worker_state.indexes = {}
        return self._worker_state.indexes

    @indexes.setter
    def indexes(self, value):
        self._worker_state.indexes = value

    @property
    def status_indexes(self):
        if not hasattr(self._worker_state, 'status_indexes'):
            self._worker_state.status_indexes = {}
        return self._worker_state.status_indexes

    @status_indexes.setter
    def status_indexes(self, value):
        self._worker_state.status_indexes = value

    @property
    def index_manager(self):
        return getattr(self._worker_state, 'index_manager', None)

    @index_manager.setter
    def index_manager(self, value):
        self._worker_state.index_manager = value

    def _parse_config(self):
        self.url = get_opt_from_conf_or_env(self.config, 'SEASEARCH', 'seasearch_url', 'SEASEARCH_URL')
        self.token = get_opt_from_conf_or_env(self.config, 'SEASEARCH', 'seasearch_token', 'SEASEARCH_TOKEN')
        try:
            if not isinstance(self.url, str) or not self.url.strip():
                raise ValueError('seasearch_url / SEASEARCH_URL is required')
            self.url = self.url.strip().rstrip('/')
            parsed = urlsplit(self.url)
            if (parsed.scheme not in ('http', 'https') or not parsed.hostname
                    or parsed.query or parsed.fragment or any(c.isspace() for c in self.url)):
                raise ValueError('SeaSearch URL must be an HTTP(S) URL without query or fragment')
            parsed.port  # Validate malformed or out-of-range port numbers.
            if not isinstance(self.token, str) or not self.token.strip():
                raise ValueError('seasearch_token / SEASEARCH_TOKEN is required')
        except ValueError as e:
            logger.error('SeaSearch indexing disabled: %s', e)
            self.enabled = False
            return
        try:
            self.wiki_size_limit = int(get_opt_from_conf_or_env(
                self.config, 'SEASEARCH', 'wiki_file_size_limit', default=10))
            if self.wiki_size_limit <= 0:
                raise ValueError()
        except (TypeError, ValueError):
            logger.error('Wiki indexing disabled: wiki_file_size_limit must be a positive integer')
            self.disabled_kinds.add('wiki')

    def start(self):
        if not self.enabled or self.threads:
            return
        targets = [(self.handle_events, 'seasearch_events'),
                   (self.run_compensation, 'seasearch_compensation')]
        targets.extend((self.run_worker, 'seasearch_index_worker_%s' % i)
                       for i in range(self.worker_count))
        for target, name in targets:
            thread = Thread(target=target, name=name, daemon=True)
            self.threads.append(thread)
            thread.start()
        logger.info('SeaSearch indexing started, workers=%s, compensation interval=%s seconds',
                    self.worker_count, self.interval)

    def stop(self):
        self.finished.set()
        self.scheduler.stop()

    def handle_events(self):
        while not self.finished.is_set():
            try:
                if self.mq is None:
                    self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)
                    if self.mq is None:
                        self.finished.wait(1)
                        continue
                with self.mq.pubsub(ignore_subscribe_messages=True) as subscriber:
                    subscriber.subscribe('repo_update', 'metadata_update_channel')
                    last_message_time = time.monotonic()
                    while not self.finished.is_set():
                        message = subscriber.get_message(timeout=0.5)
                        if message:
                            last_message_time = time.monotonic()
                            self.handle_message(message['data'])
                        elif time.monotonic() - last_message_time >= self.SUBSCRIPTION_IDLE_TIMEOUT:
                            logger.info('SeaSearch subscription idle for %s seconds; recreating subscription',
                                        self.SUBSCRIPTION_IDLE_TIMEOUT)
                            break
            except Exception:
                logger.exception('SeaSearch event subscription failed')
                self.finished.wait(1)

    def handle_message(self, value):
        try:
            data = json.loads(value)
        except (TypeError, ValueError):
            logger.warning('Invalid SeaSearch event: %s', value)
            return
        if not isinstance(data, dict):
            return
        repo_id = data.get('repo_id')
        if not isinstance(repo_id, str) or len(repo_id) != 36:
            return
        if data.get('msg_type') == 'repo-update' and data.get('commit_id'):
            self.scheduler.submit(repo_id, content=True)
        elif data.get('msg_type') == 'metadata-rows-updated':
            rows = data.get('rows')
            if not isinstance(rows, list):
                return
            row_ids = set()
            for row in rows:
                if not isinstance(row, dict):
                    continue
                columns, row_id = row.get('updated_columns'), row.get('row_id')
                if (isinstance(columns, list) and '_description' in columns
                        and isinstance(row_id, str) and row_id):
                    row_ids.add(row_id)
            if row_ids:
                logger.info('SeaSearch description event queued: repo=%s event_rows=%s description_rows=%s',
                            repo_id, len(rows), len(row_ids))
                self.scheduler.submit(repo_id, row_ids=row_ids)

    def ensure_indexes(self, kind):
        if kind in self.indexes:
            return
        # Serialize check/create of shared status index names, not repository execution.
        with self._initialization_lock:
            self._ensure_indexes(kind)

    def _ensure_indexes(self, kind):
        # Initialization happens in the writing worker and is retried independently per kind.
        if kind in self.indexes:
            return
        api = SeaSearchAPI(self.url, self.token)
        if kind == 'wiki':
            status = WikiStatusIndex(api, WIKI_STATUS_INDEX_NAME)
            index = WikiIndex(api, repo_data, int(SHARD_NUM), wiki_file_size_limit=self.wiki_size_limit * 1024 * 1024)
        else:
            status = RepoStatusIndex(api, REPO_STATUS_FILE_INDEX_NAME)
            index = RepoFileIndex(api, repo_data, int(SHARD_NUM), self.config)
        self.status_indexes[kind] = status
        self.indexes[kind] = index
        logger.info('SeaSearch worker initialized: worker=%s kind=%s', current_thread().name, kind)

    def run_worker(self):
        while not self.finished.is_set():
            item = self.scheduler.take()
            if item is None:
                return
            repo_id, task = item
            started = time.monotonic()
            outcome = 'failed'
            logger.info('SeaSearch task started: repo=%s worker=%s realtime=%s content=%s '
                        'metadata=%s rows=%s cleanup=%s attempt=%s queued_seconds=%.3f',
                        repo_id, current_thread().name, task.realtime, task.content, task.metadata,
                        len(task.row_ids), sorted(task.cleanup), task.failures + 1,
                        max(0, started - task.first_seen))
            try:
                self.process_task(repo_id, task)
                outcome = 'completed'
            except Exception:
                logger.exception('SeaSearch task failed for repo %s; scheduling retry', repo_id)
                self.scheduler.retry(repo_id, task)
                logger.info('SeaSearch retry scheduled: repo=%s attempt=%s retry_in_seconds=%.3f',
                            repo_id, task.failures + 1, max(0, task.due - time.monotonic()))
            finally:
                self.scheduler.finish(repo_id, task)
                logger.info('SeaSearch task ended: repo=%s worker=%s outcome=%s duration_seconds=%.3f',
                            repo_id, current_thread().name, outcome, time.monotonic() - started)

    def process_task(self, repo_id, task):
        repo = repo_data.get_repo_index_info(repo_id)
        if repo is None:
            for kind in task.cleanup:
                if kind in self.disabled_kinds:
                    continue
                if self.index_manager is None:
                    self.index_manager = IndexManager()
                self.ensure_indexes(kind)
                if kind == 'wiki':
                    self.index_manager.delete_wiki_index(repo_id, self.indexes[kind], self.status_indexes[kind])
                else:
                    self.index_manager.delete_repo_file_index(repo_id, self.indexes[kind], self.status_indexes[kind])
                logger.info('SeaSearch deleted repo index: repo=%s kind=%s', repo_id, kind)
            return
        commit_id, repo_type, is_virtual = repo
        if is_virtual:
            logger.info('SeaSearch task skipped: repo=%s reason=virtual', repo_id)
            return
        if not (task.content or task.metadata or task.row_ids):
            logger.info('SeaSearch task skipped: repo=%s reason=cleanup_repo_still_exists', repo_id)
            return
        if not commit_id:
            logger.warning('Skipping SeaSearch indexing for repo %s: no master head', repo_id)
            return
        kind = 'wiki' if repo_type == REPO_TYPE_WIKI else 'file'
        if kind in self.disabled_kinds or (kind == 'wiki' and not task.content):
            logger.info('SeaSearch task skipped: repo=%s kind=%s reason=disabled_or_unsupported_task', repo_id, kind)
            return
        if self.index_manager is None:
            self.index_manager = IndexManager()
        self.ensure_indexes(kind)
        index, status = self.indexes[kind], self.status_indexes[kind]
        if task.content:
            started = time.monotonic()
            logger.info('SeaSearch content check started: repo=%s kind=%s head=%s', repo_id, kind, commit_id)
            if kind == 'wiki':
                updated = self.index_manager.update_wiki_index(repo_id, commit_id, index, status)
            else:
                updated = self.index_manager.update_library_file_index(
                    repo_id, commit_id, index, status, None, update_metadata=False)
            if not updated:
                raise RuntimeError('Content indexing failed')
            logger.info('SeaSearch content check completed: repo=%s kind=%s head=%s duration_seconds=%.3f',
                        repo_id, kind, commit_id, time.monotonic() - started)
        if kind == 'file':
            if task.row_ids:
                started = time.monotonic()
                logger.info('SeaSearch description event started: repo=%s requested_rows=%s',
                            repo_id, len(task.row_ids))
                self.index_manager.update_library_file_descriptions(repo_id, task.row_ids, index)
                logger.info('SeaSearch description event completed: repo=%s requested_rows=%s duration_seconds=%.3f',
                            repo_id, len(task.row_ids), time.monotonic() - started)
            if task.metadata:
                started = time.monotonic()
                self.index_manager.compensate_library_descriptions(repo_id, index, status)
                logger.info('SeaSearch description compensation completed: repo=%s duration_seconds=%.3f',
                            repo_id, time.monotonic() - started)

    def scan_repositories(self):
        # Snapshots are read-only hints. The worker rechecks current head/status/existence.
        started = time.monotonic()
        submitted = content_count = metadata_count = cleanup_count = errors = 0
        logger.info('SeaSearch compensation scan started')
        api = SeaSearchAPI(self.url, self.token)
        snapshots = {}
        for kind, name, cls in [('file', REPO_STATUS_FILE_INDEX_NAME, RepoStatusIndex),
                                ('wiki', WIKI_STATUS_INDEX_NAME, WikiStatusIndex)]:
            if kind in self.disabled_kinds:
                continue
            try:
                status = cls(api, name, create=False)
                rows = status.get_all_repos_from_index() if api.check_index_mapping(name).get('is_exist') else []
                snapshots[kind] = {row['repo_id']: row for row in rows}
                logger.info('SeaSearch compensation snapshot: kind=%s repos=%s', kind, len(rows))
            except Exception:
                errors += 1
                # Failure is not an empty snapshot: avoid scheduling every repo as unindexed.
                logger.exception('Failed to read %s compensation status snapshot', kind)
        metadata_probe = IndexManager()
        start, count = 0, 1000
        existing = set()
        while not self.finished.is_set():
            repos = repo_data.get_repo_id_commit_id(start, count)
            if not repos:
                break
            virtual = {row[0] for row in repo_data.get_virtual_repo_in_repos([row[0] for row in repos])}
            for repo_id, commit_id, repo_type in repos:
                existing.add(repo_id)
                kind = 'wiki' if repo_type == REPO_TYPE_WIKI else 'file'
                if repo_id in virtual or kind not in snapshots or not commit_id:
                    continue
                try:
                    status = snapshots[kind].get(repo_id)
                    content = (status is None or status.get('commit_id') != commit_id
                               or status.get('updatingto') is not None)
                    if not content:
                        prefix = WIKI_INDEX_PREFIX if kind == 'wiki' else REPO_FILE_INDEX_PREFIX
                        content = not api.check_index_mapping(prefix + repo_id).get('is_exist')
                    metadata = kind == 'file' and (content or metadata_probe.has_description_changes(
                        repo_id, status.get('metadata_updated_time')))
                    if content or metadata:
                        self.scheduler.submit(repo_id, content=content, metadata=metadata,
                                              immediate=True, background=True)
                        submitted += 1
                        content_count += bool(content)
                        metadata_count += bool(metadata)
                except Exception:
                    errors += 1
                    logger.exception('Failed to check compensation changes for repo %s', repo_id)
            start += count
            logger.info('SeaSearch compensation progress: scanned=%s submitted=%s errors=%s '
                        'duration_seconds=%.3f', len(existing), submitted, errors, time.monotonic() - started)
        # Only collect cleanup candidates after a complete successful enumeration.
        if self.finished.is_set():
            return
        for kind, statuses in snapshots.items():
            for repo_id in statuses.keys() - existing:
                # Final shared-DB existence check is still required before deletion.
                self.scheduler.submit(repo_id, cleanup=(kind,), immediate=True, background=True)
                cleanup_count += 1
        logger.info('SeaSearch compensation scan completed: scanned=%s submitted=%s content=%s '
                    'metadata=%s cleanup=%s errors=%s duration_seconds=%.3f',
                    len(existing), submitted, content_count, metadata_count, cleanup_count, errors,
                    time.monotonic() - started)

    def run_compensation(self):
        # Wait a full interval after startup and between scans.
        while not self.finished.wait(self.interval):
            try:
                self.scan_repositories()
            except Exception:
                logger.exception('SeaSearch compensation scan failed')
