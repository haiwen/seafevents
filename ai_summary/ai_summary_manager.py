import logging

from seafevents.app.config import SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL
from seafevents.mq import get_mq
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seafevents.repo_metadata.constants import METADATA_TABLE, SUMMARY_SUPPORTED_FILE_EXTENSIONS
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI
from seafevents.repo_metadata.utils import add_ai_summary, is_summary_enabled, parse_iso_datetime

logger = logging.getLogger('ai_summary')


class AISummaryManager(object):

    AI_SUMMARY_TASK_QUEUE = 'ai_summary_task_high'
    AI_SUMMARY_INIT_TASK_QUEUE = 'ai_summary_task_low'
    QUERY_PAGE_SIZE = 1000

    def __init__(self):
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.seafile_ai_api = SeafileAIAPI(SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)
        self.mq = get_mq(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def init_ai_summary(self, repo_id, username=None, batch_size=50):
        if not self.seafile_ai_api or not self.mq or not is_summary_enabled(repo_id):
            logger.debug('Skip init ai summary repo=%s, ai_server=%s, mq=%s, summary_enabled=%s',
                         repo_id, bool(self.seafile_ai_api), bool(self.mq), is_summary_enabled(repo_id))
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
                logger.debug('No more init ai summary rows repo=%s, start=%d', repo_id, start)
                break

            logger.debug('Fetched init ai summary rows repo=%s, start=%d, row_count=%d',
                         repo_id, start, len(rows))

            obj_ids = []
            for row in rows:
                obj_id = row.get(METADATA_TABLE.columns.obj_id.name)
                suffix = (row.get(METADATA_TABLE.columns.suffix.name) or '').lower()
                if not obj_id or suffix not in support_suffixes:
                    logger.debug('Skip init ai summary candidate repo=%s, obj_id=%s, suffix=%s', repo_id, obj_id, suffix)
                    continue

                file_mtime = parse_iso_datetime(row.get(METADATA_TABLE.columns.file_mtime.name))
                ai_summary_mtime = parse_iso_datetime(row.get(METADATA_TABLE.columns.ai_summary_mtime.name))
                if file_mtime and ai_summary_mtime and ai_summary_mtime >= file_mtime:
                    logger.debug('Skip up-to-date init ai summary repo=%s, obj_id=%s, file_mtime=%s, ai_summary_mtime=%s',
                                 repo_id, obj_id, file_mtime, ai_summary_mtime)
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
        logger.debug('Enqueued ai summary task repo=%s, queue=%s, is_init=%s, obj_count=%d',
                     repo_id, queue_name, is_init, len(obj_ids))

    def add_ai_summary(self, repo_id, obj_ids):
        logger.info('Start ai summary batch repo %s, obj_count=%d', repo_id, len(obj_ids))
        add_ai_summary(repo_id, obj_ids, self.metadata_server_api, self.seafile_ai_api)
