import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError
from sqlalchemy.sql import text

from seafevents.app.config import AI_SUMMARY_WORKERS, EMBEDDING_DIMENSIONS, EMBEDDING_MODEL_CONFIGURED, SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.constants import METADATA_TABLE
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI
from seafevents.repo_metadata.utils import parse_iso_datetime
from seafevents.seasearch.index_store.summary_vector_index import SummaryVectorIndex
from seafevents.seasearch.utils.constants import SHARD_NUM
from seafevents.seasearch.utils.seasearch_api import SeaSearchAPI
from seafevents.utils import get_opt_from_conf_or_env, parse_bool


logger = logging.getLogger('ai_summary')

QUEUE_NAME = 'summary_index_task'


class SummaryIndexTaskWorker:
    def __init__(self, mq, config):
        self.mq = mq
        self.enabled = False
        self.should_stop = threading.Event()
        self.worker_list = []
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.seafile_ai_api = SeafileAIAPI(SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)
        self.db_session_class = init_db_session_class()
        self.summary_vector_index = None
        self.worker_num = AI_SUMMARY_WORKERS
        self._parse_config(config)

    def _parse_config(self, config):
        section_name = 'SEASEARCH'
        if not config.has_section(section_name):
            return
        enabled = get_opt_from_conf_or_env(config, section_name, 'enabled', default=False)
        if not parse_bool(enabled):
            logger.warning('Summary vector index worker disabled because SeaSearch is not enabled')
            return
        if not EMBEDDING_MODEL_CONFIGURED:
            logger.warning('Summary vector index worker disabled because embedding model is not configured or invalid')
            return
        seasearch_api = SeaSearchAPI(
            get_opt_from_conf_or_env(config, section_name, 'seasearch_url'),
            get_opt_from_conf_or_env(config, section_name, 'seasearch_token'),
        )
        self.summary_vector_index = SummaryVectorIndex(seasearch_api, int(SHARD_NUM))
        self.enabled = True
        logger.info('Summary vector index worker enabled')

    def start(self):
        if not self.enabled or not self.mq:
            return
        for i in range(self.worker_num):
            thread = threading.Thread(
                target=self._run,
                name='summary_index_task_worker_thread_%d' % i,
                daemon=True,
            )
            thread.start()
            self.worker_list.append(thread)

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

        state = self.get_repo_state(repo_id)
        if not state or state.get('processing_status') != 'indexing':
            logger.info(
                'Skip summary index task, repo_id=%s, processing_status=%s',
                repo_id, state.get('processing_status') if state else 'missing'
            )
            return
        logger.info(
            'Start summary vector index, repo_id=%s, embedding_dimensions=%d',
            repo_id, EMBEDDING_DIMENSIONS
        )
        try:
            self.update_index(repo_id)
        except Exception as error:
            logger.exception('Summary vector index failed, repo_id=%s, error=%s', repo_id, error)
            self.set_ai_processing_status(repo_id, 'index_failed')

    def update_index(self, repo_id):
        state = self.get_repo_state(repo_id)
        if not state or not state['enabled'] or not state['summary_enabled']:
            self.set_ai_processing_status(repo_id, '')
            return

        index_name = self.summary_vector_index.get_index_name(repo_id)
        indexed_at = state.get('indexed_at')
        rebuild = indexed_at is None or not self.summary_vector_index.index_exists(index_name)
        if rebuild:
            self.summary_vector_index.delete_index(index_name)
        self.summary_vector_index.create_index_if_missing(index_name)

        deleted_rows = self.metadata_server_api.get_deleted_rows(
            repo_id, METADATA_TABLE.id
        ).get('deleted_rows', [])
        deleted_row_ids = [
            row.get(METADATA_TABLE.columns.id.name)
            for row in deleted_rows
            if row.get(METADATA_TABLE.columns.id.name)
        ]
        for start in range(0, len(deleted_row_ids), 100):
            self.summary_vector_index.delete_row_ids(
                index_name, deleted_row_ids[start:start + 100]
            )
        if deleted_row_ids:
            logger.info('Deleted summary vectors for metadata rows, repo_id=%s, row_count=%d', repo_id, len(deleted_row_ids))

        since = indexed_at
        if rebuild:
            since = datetime(1970, 1, 1, tzinfo=timezone.utc)
        elif since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        until = datetime.now(timezone.utc)

        page_size = 1000
        offset = 0
        indexed_count = 0
        deleted_count = len(deleted_row_ids)
        while True:
            sql = (
                f'SELECT `{METADATA_TABLE.columns.id.name}`, '
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
            empty_row_ids = []
            for row in rows:
                row_id = row.get(METADATA_TABLE.columns.id.name)
                if not row_id:
                    continue
                path = os.path.join(
                    row.get(METADATA_TABLE.columns.parent_dir.name) or '/',
                    row.get(METADATA_TABLE.columns.file_name.name) or '',
                )
                summary = row.get(METADATA_TABLE.columns.ai_summary.name) or ''
                if not summary:
                    empty_row_ids.append(row_id)
                    continue
                file_mtime = parse_iso_datetime(row.get(METADATA_TABLE.columns.file_mtime.name))
                documents.append({
                    'row_id': row_id,
                    'path': path,
                    'filename': row.get(METADATA_TABLE.columns.file_name.name) or '',
                    'ai_summary': summary,
                    'ai_summary_mtime': row.get(METADATA_TABLE.columns.ai_summary_mtime.name),
                    'mtime': int(file_mtime.timestamp() * 1000) if file_mtime else None,
                })

            logger.info(
                'Summary vector index candidates, repo_id=%s, row_count=%d, index_count=%d, delete_count=%d',
                repo_id, len(rows), len(documents), len(empty_row_ids)
            )
            for start in range(0, len(documents), 50):
                batch = documents[start:start + 50]
                model, embeddings = self.seafile_ai_api.batch_generate_embeddings([
                    document['ai_summary'] for document in batch
                ])
                self.summary_vector_index.index_documents(
                    index_name, repo_id, batch, embeddings, model
                )
                indexed_count += len(batch)
                logger.info('Indexed summary vector batch, repo_id=%s, row_count=%d', repo_id, len(batch))
            if empty_row_ids:
                self.summary_vector_index.delete_row_ids(index_name, empty_row_ids)
                deleted_count += len(empty_row_ids)

            offset += page_size
            if len(rows) < page_size:
                break

        state = self.get_repo_state(repo_id)
        if not state or not state['enabled'] or not state['summary_enabled']:
            self.summary_vector_index.delete_index(index_name)
            self.set_ai_processing_status(repo_id, '')
            return

        self.set_indexed_at(repo_id, until)
        self.set_ai_processing_status(repo_id, '')
        logger.info(
            'Summary vector index completed, repo_id=%s, indexed=%d, deleted=%d, since=%s, until=%s',
            repo_id, indexed_count, deleted_count, since.isoformat(), until.isoformat()
        )

    def get_repo_state(self, repo_id):
        with self.db_session_class() as session:
            row = session.execute(text(
                'SELECT enabled, summary_enabled, ai_summary_indexed_at, ai_processing_status '
                'FROM repo_metadata WHERE repo_id=:repo_id LIMIT 1'
            ), {'repo_id': repo_id}).fetchone()
        if not row:
            return None
        return {
            'enabled': row[0],
            'summary_enabled': row[1],
            'indexed_at': row[2],
            'processing_status': row[3],
        }

    def set_indexed_at(self, repo_id, indexed_at):
        with self.db_session_class() as session:
            session.execute(text(
                "UPDATE repo_metadata SET ai_summary_indexed_at=:indexed_at WHERE repo_id=:repo_id"
            ), {'repo_id': repo_id, 'indexed_at': indexed_at})
            session.commit()

    def set_ai_processing_status(self, repo_id, status=''):
        with self.db_session_class() as session:
            session.execute(text(
                "UPDATE repo_metadata SET ai_processing_status=:status WHERE repo_id=:repo_id"
            ), {'repo_id': repo_id, 'status': status})
            session.commit()
