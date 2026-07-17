import json
import logging

from datetime import datetime
from sqlalchemy.sql import text

from seafevents.app.config import SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL
from seafevents.db import init_db_session_class
from seafevents.repo_metadata.constants import METADATA_TABLE, SUMMARY_SUPPORTED_FILE_EXTENSIONS
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI
from seafevents.repo_metadata.utils import add_ai_summary, is_summary_enabled, parse_iso_datetime, query_metadata_rows
from seaserv import seafile_api

logger = logging.getLogger('ai_summary')


class AISummaryManager(object):

    def __init__(self):
        self._db_session_class = init_db_session_class()
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.seafile_ai_api = SeafileAIAPI(SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)

    def init_ai_summary(self, repo_id, username=None, batch_size=50):
        if not self.seafile_ai_api or not is_summary_enabled(repo_id):
            return

        logger.info('Updating ai summary repo %s', repo_id)

        sql = (
            f'SELECT `{METADATA_TABLE.columns.obj_id.name}`, '
            f'`{METADATA_TABLE.columns.suffix.name}`, '
            f'`{METADATA_TABLE.columns.file_mtime.name}`, '
            f'`{METADATA_TABLE.columns.ai_summary_mtime.name}` '
            f'FROM `{METADATA_TABLE.name}` '
            f'WHERE `{METADATA_TABLE.columns.is_dir.name}` = False '
            f'AND `{METADATA_TABLE.columns.file_type.name}` = "_document"'
        )
        rows = query_metadata_rows(repo_id, self.metadata_server_api, sql)
        support_suffixes = set(SUMMARY_SUPPORTED_FILE_EXTENSIONS)
        obj_ids = []
        for row in rows or []:
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
                self.add_ai_summary(repo_id, obj_ids)
                obj_ids = []

        if obj_ids:
            self.add_ai_summary(repo_id, obj_ids)

        if username:
            self.save_ai_summary_message_to_user_notification(repo_id, username)
        logger.info('Finish ai summary repo %s', repo_id)

    def add_ai_summary(self, repo_id, obj_ids):
        logger.info('Start init ai summary repo %s, obj_count=%d', repo_id, len(obj_ids))
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
