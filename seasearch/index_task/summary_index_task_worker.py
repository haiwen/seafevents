import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError
from sqlalchemy.sql import text

from seafevents.app.config import SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.constants import METADATA_TABLE
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI
from seafevents.repo_metadata.utils import parse_iso_datetime
from seafevents.seasearch.index_store.summary_vector_index import SummaryVectorIndex
from seafevents.seasearch.utils.constants import SHARD_NUM
from seafevents.seasearch.utils.seasearch_api import SeaSearchAPI
from seafevents.utils import get_opt_from_conf_or_env, parse_bool


logger = logging.getLogger('seasearch')

QUEUE_NAME = 'summary_index_task'
MAX_RETRIES = 3


class SummaryIndexTaskWorker:
    def __init__(self, mq, config):
        self.mq = mq
        self.enabled = False
        self.should_stop = threading.Event()
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.seafile_ai_api = SeafileAIAPI(SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)
        self.db_session_class = init_db_session_class()
        self.summary_vector_index = None
        self._parse_config(config)

    def _parse_config(self, config):
        section_name = 'SEASEARCH'
        if not config.has_section(section_name):
            return
        enabled = get_opt_from_conf_or_env(config, section_name, 'enabled', default=False)
        if not parse_bool(enabled):
            return
        seasearch_api = SeaSearchAPI(
            get_opt_from_conf_or_env(config, section_name, 'seasearch_url'),
            get_opt_from_conf_or_env(config, section_name, 'seasearch_token'),
        )
        self.summary_vector_index = SummaryVectorIndex(seasearch_api, int(SHARD_NUM))
        self.enabled = True

    def start(self):
        if not self.enabled or not self.mq:
            return
        try:
            self.reset_indexing_status()
        except Exception as error:
            self.enabled = False
            logger.error('Cannot start summary index worker: %s', error)
            return
        thread = threading.Thread(target=self._run, name='summary_index_task_worker', daemon=True)
        thread.start()

    def _run(self):
        logger.info('summary index task worker started')
        while not self.should_stop.is_set():
            try:
                result = self.mq.brpop(QUEUE_NAME, timeout=30)
                if result is None:
                    continue
                self._handle_message(result[1])
            except (ResponseError, NoMQAvailable, TimeoutError) as error:
                logger.error('The connection to the redis server failed: %s', error)
            except Exception as error:
                logger.exception('Handle summary index task failed: %s', error)
                time.sleep(0.3)

    def _handle_message(self, value):
        try:
            data = json.loads(value)
        except Exception:
            logger.warning('Invalid summary index task: %s', value)
            return

        repo_id = data.get('repo_id')
        if not repo_id:
            logger.warning('Summary index task has no repo_id: %s', data)
            return

        lock = self.mq.lock('summary_index:%s' % repo_id, timeout=3600, blocking_timeout=1)
        if not lock.acquire(blocking=True):
            self.mq.lpush(QUEUE_NAME, json.dumps(data))
            return
        try:
            self.update_index(repo_id, rebuild=bool(data.get('rebuild')))
        except Exception as error:
            logger.exception('Summary vector index failed, repo_id=%s, error=%s', repo_id, error)
            state = self.get_repo_state(repo_id)
            if state and state['enabled'] and state['summary_enabled']:
                self.set_index_state(repo_id, status='failed')
                retries = int(data.get('retries', 0)) + 1
                if retries <= MAX_RETRIES:
                    data['retries'] = retries
                    self.mq.lpush(QUEUE_NAME, json.dumps(data))
            else:
                self.summary_vector_index.delete_index(
                    self.summary_vector_index.get_index_name(repo_id)
                )
        finally:
            try:
                lock.release()
            except Exception:
                pass

    def update_index(self, repo_id, rebuild=False):
        state = self.get_repo_state(repo_id)
        if not state or not state['enabled'] or not state['summary_enabled']:
            return

        index_name = self.summary_vector_index.get_index_name(repo_id)
        if rebuild:
            self.summary_vector_index.delete_index(index_name)
        self.summary_vector_index.create_index_if_missing(index_name)

        since = state.get('indexed_at')
        if rebuild or since is None:
            since = datetime(1970, 1, 1, tzinfo=timezone.utc)
        elif since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        until = datetime.now(timezone.utc)
        self.set_index_state(repo_id, status='indexing')

        page_size = 1000
        offset = 0
        indexed_count = 0
        deleted_count = 0
        while True:
            sql = (
                f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}`, '
                f'`{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}`, '
                f'`{METADATA_TABLE.columns.file_mtime.name}`, `{METADATA_TABLE.columns.ai_summary.name}`, '
                f'`{METADATA_TABLE.columns.ai_summary_mtime.name}` FROM `{METADATA_TABLE.name}` '
                f'WHERE `{METADATA_TABLE.columns.is_dir.name}` = False '
                f'AND `{METADATA_TABLE.columns.ai_summary_mtime.name}` > ? '
                f'AND `{METADATA_TABLE.columns.ai_summary_mtime.name}` <= ? '
                f'ORDER BY `{METADATA_TABLE.columns.ai_summary_mtime.name}`, `{METADATA_TABLE.columns.id.name}` '
                f'LIMIT {offset}, {page_size}'
            )
            rows = self.metadata_server_api.query_rows(
                repo_id, sql, [since.isoformat(), until.isoformat()]
            ).get('results', [])
            if not rows:
                break

            documents = []
            empty_paths = []
            for row in rows:
                path = os.path.join(
                    row.get(METADATA_TABLE.columns.parent_dir.name) or '/',
                    row.get(METADATA_TABLE.columns.file_name.name) or '',
                )
                summary = row.get(METADATA_TABLE.columns.ai_summary.name) or ''
                if not summary:
                    empty_paths.append(path)
                    continue
                file_mtime = parse_iso_datetime(row.get(METADATA_TABLE.columns.file_mtime.name))
                documents.append({
                    'path': path,
                    'obj_id': row.get(METADATA_TABLE.columns.obj_id.name),
                    'ai_summary': summary,
                    'ai_summary_mtime': row.get(METADATA_TABLE.columns.ai_summary_mtime.name),
                    'mtime': int(file_mtime.timestamp() * 1000) if file_mtime else None,
                })

            for start in range(0, len(documents), 50):
                batch = documents[start:start + 50]
                model, embeddings = self.seafile_ai_api.batch_generate_embeddings([
                    document['ai_summary'] for document in batch
                ])
                self.summary_vector_index.index_documents(
                    index_name, repo_id, batch, embeddings, model
                )
                indexed_count += len(batch)
            if empty_paths:
                self.summary_vector_index.delete_paths(index_name, empty_paths)
                deleted_count += len(empty_paths)

            offset += page_size
            if len(rows) < page_size:
                break

        state = self.get_repo_state(repo_id)
        if not state or not state['enabled'] or not state['summary_enabled']:
            self.summary_vector_index.delete_index(index_name)
            return

        self.set_index_state(repo_id, indexed_at=until, status='completed')
        logger.info(
            'Summary vector index completed, repo_id=%s, indexed=%d, deleted=%d, since=%s, until=%s',
            repo_id, indexed_count, deleted_count, since.isoformat(), until.isoformat()
        )

    def get_repo_state(self, repo_id):
        with self.db_session_class() as session:
            row = session.execute(text(
                'SELECT enabled, summary_enabled, ai_summary_indexed_at '
                'FROM repo_metadata WHERE repo_id=:repo_id LIMIT 1'
            ), {'repo_id': repo_id}).fetchone()
        if not row:
            return None
        return {
            'enabled': row[0],
            'summary_enabled': row[1],
            'indexed_at': row[2],
        }

    def set_index_state(self, repo_id, indexed_at=None, status=None):
        values = {'repo_id': repo_id}
        assignments = []
        if indexed_at is not None:
            assignments.append('ai_summary_indexed_at=:indexed_at')
            values['indexed_at'] = indexed_at
        if status is not None:
            assignments.append('ai_indexing_status=:status')
            values['status'] = status
        if not assignments:
            return
        with self.db_session_class() as session:
            session.execute(text(
                'UPDATE repo_metadata SET %s WHERE repo_id=:repo_id' % ', '.join(assignments)
            ), values)
            session.commit()

    def reset_indexing_status(self):
        with self.db_session_class() as session:
            session.execute(text(
                "UPDATE repo_metadata SET ai_indexing_status='pending' "
                "WHERE ai_indexing_status='indexing'"
            ))
            session.commit()
