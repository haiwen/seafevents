import json
import logging
import time
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
from threading import Lock, Thread

from dateutil.relativedelta import relativedelta
from sqlalchemy import text

from seafevents.app.config import AI_PRICES
from seafevents.app.config import SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL
from seafevents.app.event_redis import RedisClient
from seafevents.db import init_db_session_class

logger = logging.getLogger(__name__)


class AIScenario:
    IMAGE_CAPTION = 'image-caption'
    SUMMARY = 'summary'
    FILE_TAGS = 'file-tags'
    OCR = 'ocr'
    TRANSLATE = 'translate'
    WRITING_ASSISTANT = 'writing-assistant'
    CHAT = 'chat'
    UNKNOWN = 'unknown'

    @classmethod
    def is_valid(cls, value):
        return value in {
            cls.IMAGE_CAPTION,
            cls.SUMMARY,
            cls.FILE_TAGS,
            cls.OCR,
            cls.TRANSLATE,
            cls.WRITING_ASSISTANT,
            cls.CHAT,
            cls.UNKNOWN,
        }


class AIStatsWorker:
    def __init__(self):
        self._db_session_class = init_db_session_class()
        self._redis_client = RedisClient()
        self.stats_lock = Lock()
        self.channel = 'log_ai_model_usage'
        self.keep_months = 6
        self.log_none_message_timeout = 60 * 10
        self.stats_interval = 60
        self.reset_stats()

    def reset_stats(self):
        self.ai_usage_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))

    def save_to_memory(self, usage_info):
        if not usage_info.get('model'):
            return

        model = usage_info['model']
        usage = usage_info.get('usage') or {}
        repo_id = usage_info.get('repo_id')
        repo_owner = usage_info.get('repo_owner')
        group_id = usage_info.get('group_id')
        org_id = usage_info.get('org_id')
        scenario = usage_info.get('scenario')

        if not model or model not in AI_PRICES:
            logger.warning('model %s price not defined', model)
            return

        input_tokens = usage.get('prompt_tokens') or usage.get('input_tokens') or 0
        output_tokens = usage.get('completion_tokens') or usage.get('output_tokens') or 0
        if not isinstance(input_tokens, int):
            input_tokens = 0
        if not isinstance(output_tokens, int):
            output_tokens = 0

        if not isinstance(repo_id, str) or not repo_id.strip():
            repo_id = None
        if not isinstance(repo_owner, str) or not repo_owner.strip():
            repo_owner = None
        if not isinstance(group_id, int):
            group_id = None
        if not isinstance(org_id, int) or org_id <= 0:
            org_id = None

        if not isinstance(scenario, str):
            scenario = AIScenario.UNKNOWN
        else:
            scenario = scenario.strip().lower() or AIScenario.UNKNOWN
            if not AIScenario.is_valid(scenario):
                scenario = AIScenario.UNKNOWN

        key = (repo_owner, group_id, org_id, model, scenario)
        stats = self.ai_usage_stats[repo_id][key]
        stats['input_tokens'] += input_tokens
        stats['output_tokens'] += output_tokens

    def receive(self):
        if not self._redis_client.connection:
            logger.warning('Can not start ai stats receiver: redis connection is not initialized')
            return

        logger.info('Starts to receive ai calls...')
        subscriber = self._redis_client.get_subscriber(self.channel)
        last_message_time = datetime.now()

        while True:
            try:
                message = subscriber.get_message()
                if message is not None:
                    last_message_time = datetime.now()
                    try:
                        usage_info = json.loads(message['data'])
                    except Exception:
                        logger.warning('log_ai_model_usage message invalid')
                        continue
                    logger.debug('usage_info %s', usage_info)
                    try:
                        with self.stats_lock:
                            self.save_to_memory(usage_info)
                    except Exception as error:
                        logger.exception('save usage_info %s to memory error %s', usage_info, error)
                else:
                    if (datetime.now() - last_message_time).seconds >= self.log_none_message_timeout:
                        logger.warning('No log_ai_model_usage message received for %s seconds', self.log_none_message_timeout)
                        last_message_time = datetime.now()
                    time.sleep(0.5)
            except Exception as error:
                logger.error('Failed get message from redis: %s', error)
                subscriber = self._redis_client.get_subscriber(self.channel)
                last_message_time = datetime.now()

    def _calculate_cost(self, model, input_tokens, output_tokens):
        input_tokens_price = AI_PRICES[model].get('input_tokens') or 0
        output_tokens_price = AI_PRICES[model].get('output_tokens') or 0
        input_cost = input_tokens_price * (input_tokens / 1e6)
        output_cost = output_tokens_price * (output_tokens / 1e6)
        return input_cost + output_cost

    def stats_worker(self):
        if not self.ai_usage_stats:
            logger.info('There are no stats')
            return

        with self.stats_lock:
            usage_stats = deepcopy(self.ai_usage_stats)
            self.reset_stats()

        logger.info('There are %s repo stats', len(usage_stats))

        today = datetime.today().date()
        now = datetime.now()

        select_sql = '''
        SELECT `id` FROM `ai_usage_statistics`
        WHERE `date`=:date
          AND `repo_id` <=> :repo_id
          AND `repo_owner` <=> :repo_owner
          AND `group_id` <=> :group_id
          AND `org_id` <=> :org_id
          AND `model`=:model
          AND `scenario`=:scenario
        LIMIT 1
        '''
        update_sql = '''
        UPDATE `ai_usage_statistics`
        SET `input_tokens`=`input_tokens`+:input_tokens,
            `output_tokens`=`output_tokens`+:output_tokens,
            `cost`=`cost`+:cost,
            `updated_at`=:updated_at
        WHERE `id`=:id
        '''
        insert_sql = '''
        INSERT INTO `ai_usage_statistics`(`repo_id`, `date`, `repo_owner`, `group_id`, `org_id`, `model`, `scenario`, `input_tokens`, `output_tokens`, `cost`, `created_at`, `updated_at`)
        VALUES (:repo_id, :date, :repo_owner, :group_id, :org_id, :model, :scenario, :input_tokens, :output_tokens, :cost, :created_at, :updated_at)
        '''

        records = []
        for repo_id, stats_dict in usage_stats.items():
            for (repo_owner, group_id, org_id, model, scenario), usage in stats_dict.items():
                input_tokens = usage.get('input_tokens') or 0
                output_tokens = usage.get('output_tokens') or 0
                cost = self._calculate_cost(model, input_tokens, output_tokens)
                logger.info(
                    'repo %s repo_owner %s group_id %s org_id %s model %s scenario %s input_tokens %s output_tokens %s cost %s',
                    repo_id,
                    repo_owner,
                    group_id,
                    org_id,
                    model,
                    scenario,
                    input_tokens,
                    output_tokens,
                    cost,
                )
                records.append({
                    'repo_id': repo_id,
                    'date': today,
                    'repo_owner': repo_owner,
                    'group_id': group_id,
                    'org_id': org_id,
                    'model': model,
                    'scenario': scenario,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'cost': cost,
                    'created_at': now,
                    'updated_at': now,
                })

        session = self._db_session_class()
        try:
            for data in records:
                result = session.execute(text(select_sql), {
                    'date': data['date'],
                    'repo_id': data['repo_id'],
                    'repo_owner': data['repo_owner'],
                    'group_id': data['group_id'],
                    'org_id': data['org_id'],
                    'model': data['model'],
                    'scenario': data['scenario'],
                }).fetchone()

                if result:
                    session.execute(text(update_sql), {
                        'id': result[0],
                        'input_tokens': data['input_tokens'],
                        'output_tokens': data['output_tokens'],
                        'cost': data['cost'],
                        'updated_at': data['updated_at'],
                    })
                else:
                    session.execute(text(insert_sql), data)
            session.commit()
        except Exception as error:
            logger.exception(error)
        finally:
            session.close()

    def stats(self):
        while True:
            time.sleep(self.stats_interval)
            try:
                self.stats_worker()
            except Exception as error:
                logger.exception(error)

    def clean_worker(self):
        session = self._db_session_class()
        sql = 'DELETE FROM `ai_usage_statistics` WHERE `date` < :clean_month'
        clean_month = (datetime.now() - relativedelta(months=self.keep_months)).strftime('%Y-%m-01')
        try:
            session.execute(text(sql), {'clean_month': clean_month})
            session.commit()
        except Exception as error:
            logger.exception(error)
        finally:
            session.close()

    def clean(self):
        while True:
            now = datetime.now()
            next_run = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            time.sleep(max((next_run - now).total_seconds(), 1))
            try:
                self.clean_worker()
            except Exception as error:
                logger.exception(error)

    def start(self):
        Thread(target=self.receive, daemon=True).start()
        Thread(target=self.stats, daemon=True).start()
        Thread(target=self.clean, daemon=True).start()


class AIStatsManager:
    def __init__(self):
        self.worker = AIStatsWorker()

    def start(self):
        if not SEAFILE_AI_SECRET_KEY or not SEAFILE_AI_SERVER_URL:
            logger.warning('Can not start ai stats manager: secret key or server url is not set')
            return
        self.worker.start()
