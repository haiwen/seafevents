import json
import logging
import time
import threading

from redis.exceptions import ConnectionError as NoMQAvailable, ResponseError, TimeoutError
from sqlalchemy.sql import text

from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.utils import parse_iso_datetime, add_ai_summary
from seafevents.db import init_db_session_class
from seafevents.app.config import AI_SUMMARY_WORKERS, ENABLE_SEAFILE_AI, AI_SUMMARY_BATCH_SIZE, SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL
from seafevents.repo_metadata.constants import METADATA_TABLE, SUMMARY_SUPPORTED_FILE_EXTENSIONS
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI


logger = logging.getLogger('ai_summary')


class AISummaryWorker(object):
    def __init__(self, mq):
        self.mq = mq
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.seafile_ai_api = SeafileAIAPI(SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)

        self.should_stop = threading.Event()
        self.worker_list = []
        self.summary_index_enabled = False

        self.ai_summary_worker_num = AI_SUMMARY_WORKERS
        self.batch_size = AI_SUMMARY_BATCH_SIZE
        self.query_page_size = 1000

        self._db_session_class = init_db_session_class()

    @property
    def tname(self):
        return threading.current_thread().name

    def start(self):
        if not ENABLE_SEAFILE_AI:
            return
        if not self.mq:
            return

        for i in range(int(self.ai_summary_worker_num)):
            t = threading.Thread(target=self.ai_summary_worker_handler, name='ai_summary_worker_thread_' + str(i), daemon=True)
            t.start()
            self.worker_list.append(t)

    def ai_summary_worker_handler(self):
        if not ENABLE_SEAFILE_AI:
            return
        logger.info('%s starting ai summary worker', self.tname)
        try:
            while not self.should_stop.is_set():
                try:
                    res = self.mq.brpop('ai_summary_task', timeout=30)
                    if res is None:
                        continue

                    key, value = res
                    try:
                        data = json.loads(value)
                    except Exception:
                        data = None

                    if not data:
                        logger.warning('ai summary repo task: invalid task payload %s', res)
                        continue

                    repo_id = data.get('repo_id')
                    self._handle_ai_summary_task(repo_id)
                except (ResponseError, NoMQAvailable, TimeoutError) as e:
                    logger.error('The connection to the redis server failed: %s', e)
        except Exception as e:
            logger.error('%s Handle ai summary worker task error', self.tname)
            logger.error(e, exc_info=True)
            time.sleep(0.3)

    def _handle_ai_summary_task(self, repo_id):
        if not repo_id:
            return
        if self.get_ai_processing_status(repo_id) != 'in_summary':
            logger.info('Skip stale ai summary task for repo %s', repo_id)
            return

        try:
            if self.should_stop.is_set():
                logger.info('%s skip ai summary repo %s due to stop signal', self.tname, repo_id)
                self.set_ai_processing_status(repo_id, '')
                return
            logger.info('%s start ai summary repo %s', self.tname, repo_id)
            self.generate_ai_summary(repo_id, batch_size=self.batch_size)
            if self.summary_index_enabled:
                self.mq.lpush('summary_index_task', json.dumps({'repo_id': repo_id}))
            else:
                self.set_ai_processing_status(repo_id, '')
            logger.info('%s finish ai summary repo %s', self.tname, repo_id)
        except Exception as e:
            logger.exception('ai summary repo: %s, error: %s', repo_id, e)
            self.set_ai_processing_status(repo_id, '')

    def is_summary_enabled(self, repo_id):
        with self._db_session_class() as session:
            sql = "SELECT summary_enabled FROM repo_metadata WHERE repo_id=:repo_id LIMIT 1"
            record = session.execute(text(sql), {'repo_id': repo_id}).fetchone()
        return record[0] if record else False

    def get_ai_processing_status(self, repo_id):
        with self._db_session_class() as session:
            record = session.execute(text(
                "SELECT ai_processing_status FROM repo_metadata WHERE repo_id = :repo_id LIMIT 1"
            ), {'repo_id': repo_id}).fetchone()
        return record[0] if record else None

    def set_ai_processing_status(self, repo_id, status=''):
        with self._db_session_class() as session:
            sql = text("UPDATE repo_metadata SET ai_processing_status = :status WHERE repo_id = :repo_id")
            session.execute(sql, {'repo_id': repo_id, 'status': status})
            session.commit()

    def reset_ai_processing_status(self):
        with self._db_session_class() as session:
            sql = text("UPDATE repo_metadata SET ai_processing_status = '' WHERE ai_processing_status != ''")
            result = session.execute(sql)
            session.commit()
            if result.rowcount > 0:
                logger.info('Reset %d repo metadata statuses on startup', result.rowcount)
        

    def generate_ai_summary(self, repo_id, batch_size=50):
        if not self.seafile_ai_api or not self.mq or not self.is_summary_enabled(repo_id):
            logger.debug('Skip ai summary repo=%s, ai_server=%s, mq=%s, summary_enabled=%s',
                          repo_id, bool(self.seafile_ai_api), bool(self.mq), self.is_summary_enabled(repo_id))
            return

        logger.info('Generating ai summary for repo %s', repo_id)
        base_sql = (
            f'SELECT `{METADATA_TABLE.columns.obj_id.name}`, '
            f'`{METADATA_TABLE.columns.suffix.name}`, '
            f'`{METADATA_TABLE.columns.file_mtime.name}`, '
            f'`{METADATA_TABLE.columns.ai_summary_mtime.name}` '
            f'FROM `{METADATA_TABLE.name}` '
            f'WHERE `{METADATA_TABLE.columns.is_dir.name}` = False '
            f'AND `{METADATA_TABLE.columns.file_type.name}` = "_document"'
        )

        support_suffixes = set(SUMMARY_SUPPORTED_FILE_EXTENSIONS)
        start = 0

        while True:
            query_sql = f'{base_sql} LIMIT {start}, {self.query_page_size}'
            rows = self.metadata_server_api.query_rows(repo_id, query_sql, []).get('results', [])
            if not rows:
                logger.debug('No more ai summary rows repo=%s, start=%d', repo_id, start)
                break

            logger.debug('Fetched ai summary rows repo=%s, start=%d, row_count=%d',
                                    repo_id, start, len(rows))
            obj_ids = []
            for row in rows:
                obj_id = row.get(METADATA_TABLE.columns.obj_id.name)
                suffix = (row.get(METADATA_TABLE.columns.suffix.name) or '').lower()
                if not obj_id or suffix not in support_suffixes:
                    logger.debug('Skip ai summary candidate repo=%s, obj_id=%s, suffix=%s', repo_id, obj_id, suffix)
                    continue

                file_mtime = parse_iso_datetime(row.get(METADATA_TABLE.columns.file_mtime.name))
                ai_summary_mtime = parse_iso_datetime(row.get(METADATA_TABLE.columns.ai_summary_mtime.name))
                if file_mtime and ai_summary_mtime and ai_summary_mtime >= file_mtime:
                    logger.debug('Skip up-to-date ai summary repo=%s, obj_id=%s, file_mtime=%s, ai_summary_mtime=%s',
                                 repo_id, obj_id, file_mtime, ai_summary_mtime)
                    continue

                obj_ids.append(obj_id)
                if len(obj_ids) >= batch_size:
                    add_ai_summary(repo_id, obj_ids, self.metadata_server_api, self.seafile_ai_api)
                    obj_ids = []

            if obj_ids:
                add_ai_summary(repo_id, obj_ids, self.metadata_server_api, self.seafile_ai_api)

            if len(rows) < self.query_page_size:
                break
            start += self.query_page_size

        logger.info('Finish generating ai summary for repo %s', repo_id)
