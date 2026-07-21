import json
import logging

from datetime import datetime
from sqlalchemy.sql import text

from seafevents.app.config import SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL
from seafevents.db import init_db_session_class
from seafevents.mq import get_mq
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seafevents.repo_metadata.constants import METADATA_TABLE, SUMMARY_SUPPORTED_FILE_EXTENSIONS
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI
from seafevents.repo_metadata.utils import add_ai_summary, is_summary_enabled, parse_iso_datetime
from seaserv import seafile_api

logger = logging.getLogger('ai_summary')


class AISummaryManager(object):

    AI_SUMMARY_TASK_QUEUE = 'ai_summary_task_high'
    AI_SUMMARY_INIT_TASK_QUEUE = 'ai_summary_task_low'
    NOTIFY_COUNT_KEY_PREFIX = 'init_ai_summary_notify_count_'
    NOTIFY_USER_KEY_PREFIX = 'init_ai_summary_notify_user_'
    QUERY_PAGE_SIZE = 1000

    def __init__(self):
        self._db_session_class = init_db_session_class()
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.seafile_ai_api = SeafileAIAPI(SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def init_ai_summary(self, repo_id, username=None, batch_size=50):
        if not self.seafile_ai_api or not self.mq or not is_summary_enabled(repo_id):
            return

        logger.info('Dispatching ai summary tasks for repo %s', repo_id)
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
        task_count = 0
        start = 0

        while True:
            query_sql = f'{base_sql} LIMIT {start}, {self.QUERY_PAGE_SIZE}'
            rows = self.metadata_server_api.query_rows(repo_id, query_sql, []).get('results', [])
            if not rows:
                break

            obj_ids = []
            for row in rows:
                obj_id = row.get(METADATA_TABLE.columns.obj_id.name)
                suffix = (row.get(METADATA_TABLE.columns.suffix.name) or '').lower()
                if not obj_id or suffix not in support_suffixes:
                    continue

                file_mtime = parse_iso_datetime(row.get(METADATA_TABLE.columns.file_mtime.name))
                ai_summary_mtime = parse_iso_datetime(row.get(METADATA_TABLE.columns.ai_summary_mtime.name))
                if file_mtime and ai_summary_mtime and ai_summary_mtime >= file_mtime:
                    continue

                obj_ids.append(obj_id)
                if len(obj_ids) >= batch_size:
                    self.add_ai_summary_task(repo_id, obj_ids, is_init=True)
                    task_count += 1
                    obj_ids = []

            if obj_ids:
                self.add_ai_summary_task(repo_id, obj_ids, is_init=True)
                task_count += 1

            if len(rows) < self.QUERY_PAGE_SIZE:
                break
            start += self.QUERY_PAGE_SIZE

        if username and task_count == 0:
            self.save_ai_summary_message_to_user_notification(repo_id, username)
        elif username:
            self.mark_init_ai_summary_notification(repo_id, username, task_count)
        logger.info('Finish dispatching ai summary tasks for repo %s', repo_id)

    def add_ai_summary_task(self, repo_id, obj_ids, is_init=False):
        if not repo_id or not obj_ids or not self.mq:
            return

        msg = {
            'repo_id': repo_id,
            'obj_ids': list(obj_ids),
            'is_init': is_init,
        }

        queue_name = self.AI_SUMMARY_INIT_TASK_QUEUE if is_init else self.AI_SUMMARY_TASK_QUEUE
        self.mq.lpush(queue_name, json.dumps(msg))

    def mark_init_ai_summary_notification(self, repo_id, username, task_count):
        if not self.mq or not repo_id or not username or task_count <= 0:
            return

        self.mq.set(self.NOTIFY_COUNT_KEY_PREFIX + repo_id, task_count, ex=24 * 60 * 60)
        self.mq.set(self.NOTIFY_USER_KEY_PREFIX + repo_id, username, ex=24 * 60 * 60)

    def finish_init_ai_summary_batch(self, repo_id):
        if not self.mq or not repo_id:
            return

        count_key = self.NOTIFY_COUNT_KEY_PREFIX + repo_id
        user_key = self.NOTIFY_USER_KEY_PREFIX + repo_id
        if not self.mq.exists(count_key):
            return

        remaining = self.mq.decr(count_key)
        if remaining > 0:
            return

        username = self.mq.get(user_key)
        if isinstance(username, bytes):
            username = username.decode('utf-8')
        if username:
            self.save_ai_summary_message_to_user_notification(repo_id, username)

        self.mq.delete(count_key)
        self.mq.delete(user_key)

    def add_ai_summary(self, repo_id, obj_ids):
        logger.info('Start ai summary batch repo %s, obj_count=%d', repo_id, len(obj_ids))
        add_ai_summary(repo_id, obj_ids, self.metadata_server_api, self.seafile_ai_api)

    def save_ai_summary_message_to_user_notification(self, repo_id, username):
        repo = seafile_api.get_repo(repo_id)
        repo_name = repo.repo_name
        detail = {
            'repo_id': repo_id,
            'repo_name': repo_name,
            'op_user': username,
            'op_type': 'init_ai_summary',
        }
        msg_type = 'ai_summary'
        local_datetime_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        detail = json.dumps(detail).replace('\\', '\\\\')

        values = [(username, msg_type, detail, local_datetime_str, 0)]
        with self._db_session_class() as session:
            sql = """INSERT INTO notifications_usernotification (to_user, msg_type, detail, timestamp, seen)
                                             VALUES %s""" % ', '.join(
                ["('%s', '%s', '%s', '%s', %s)" % value for value in values])
            session.execute(text(sql))
            session.commit()
