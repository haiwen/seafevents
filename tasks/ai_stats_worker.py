import json
import logging
import time
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
from threading import Thread, Lock, Event

from sqlalchemy import text

from seafevents.app.config import AI_PRICES
from seafevents.app.event_redis import RedisClient
from seafevents.db import init_db_session_class
from seafevents.app.config import SEAFILE_AI_SECRET_KEY, SEAFILE_AI_SERVER_URL

logger = logging.getLogger(__name__)

AI_STATS_CHANNEL = 'log_ai_model_usage'
ORG_STATS = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))
OWNER_STATS = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))
RESET_AI_CREDIT_DATES = []

class AIStatsReceiver(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_client = RedisClient()
        self.stats_lock = Lock()
    
    def save_to_memory(self, usage_info):
        if not usage_info.get('model'):
            return

        model = usage_info['model']
        usage = usage_info.get('usage', {})
        username = usage_info.get('username')
        org_id = usage_info.get('org_id')
        if model not in AI_PRICES:
            logger.warning('model %s price not defined', model)
            return

        if 'prompt_tokens' in usage:
            usage['input_tokens'] = usage['prompt_tokens']
        if 'completion_tokens' in usage:
            usage['output_tokens'] = usage['completion_tokens']

        if not isinstance(usage.get('input_tokens'), int):
            usage['input_tokens'] = 0
        if not isinstance(usage.get('output_tokens'), int):
            usage['output_tokens'] = 0

        if org_id and org_id != -1:
            ORG_STATS[org_id][model]['input_tokens'] += usage.get('input_tokens') or 0
            ORG_STATS[org_id][model]['output_tokens'] += usage.get('output_tokens') or 0
        else:
            OWNER_STATS[username][model]['input_tokens'] += usage.get('input_tokens') or 0
            OWNER_STATS[username][model]['output_tokens'] += usage.get('output_tokens') or 0
    
    def run(self):
        if not self._redis_client.connection:
            logger.warning('Can not start ai stats receiver: redis connection is not initialized')
            return
        subscriber = self._redis_client.get_subscriber(AI_STATS_CHANNEL)
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    try:
                        usage_info = json.loads(message['data'])
                    except:
                        logger.warning('log_ai_model_usage message invalid')
                        continue
                    logger.debug('usage_info %s', usage_info)
                    try:
                        with self.stats_lock:
                            self.save_to_memory(usage_info)
                    except Exception as e:
                        logger.exception('save usage_info %s to memory error %s', usage_info, e)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber(AI_STATS_CHANNEL)
        

class AIStatsSaver(Thread):
    TEAM_SQL = '''
    INSERT INTO `stats_ai_by_team`(`org_id`, `month`, `model`, `input_tokens`, `output_tokens`, `cost`, `created_at`, `updated_at`) 
    VALUES (:org_id, :month, :model, :input_tokens, :output_tokens, :cost, :created_at, :updated_at)
    ON DUPLICATE KEY UPDATE `input_tokens`=`input_tokens`+VALUES(`input_tokens`),
                            `output_tokens`=`output_tokens`+VALUES(`output_tokens`),
                            `cost`=`cost`+VALUES(`cost`),
                            `updated_at`=VALUES(`updated_at`)
    '''
    
    OWNER_SQL = '''
    INSERT INTO `stats_ai_by_owner`(`username`, `month`, `model`, `input_tokens`, `output_tokens`, `cost`, `created_at`, `updated_at`) 
    VALUES (:username, :month, :model, :input_tokens, :output_tokens, :cost, :created_at, :updated_at)
    ON DUPLICATE KEY UPDATE `input_tokens`=`input_tokens`+VALUES(`input_tokens`),
                            `output_tokens`=`output_tokens`+VALUES(`output_tokens`),
                            `cost`=`cost`+VALUES(`cost`),
                            `updated_at`=VALUES(`updated_at`)
    '''
    
    RESET_OWNER_AI_CREDIT_SQL = 'TRUNCATE TABLE stats_ai_by_owner'
    RESET_TEAM_AI_CREDIT_SQL = 'TRUNCATE TABLE stats_ai_by_team'

    def __init__(self, interval):
        Thread.__init__(self)
        self._interval = interval
        self.finished = Event()
        self.stats_lock = Lock()
        self._db_session_class = init_db_session_class()
    
    def _calculate_token_cost(self, model, input_tokens, output_tokens):
        input_tokens_price = AI_PRICES[model].get('input_tokens_1k') or 0
        output_tokens_price = AI_PRICES[model].get('output_tokens_1k') or 0
        input_cost = input_tokens_price * (input_tokens / 1000)
        output_cost = output_tokens_price * (output_tokens / 1000)
        return input_cost, output_cost
        
    def stats_worker(self):
        if not ORG_STATS and not OWNER_STATS:
            logger.info('There are no stats')
            return
        with self.stats_lock:
            local_org_stats = deepcopy(ORG_STATS)
            local_owner_stats = deepcopy(OWNER_STATS)
            self.reset_stats()

        logger.info('There are %s org stats and %s owner stats', len(local_org_stats), len(local_owner_stats))
        month = datetime.today().replace(day=1).date()

        team_data = []
        for org_id, models_dict in local_org_stats.items():
            for model, usage in models_dict.items():
                input_tokens = usage.get('input_tokens') or 0
                output_tokens = usage.get('output_tokens') or 0

                input_cost, output_cost = self._calculate_token_cost(model, input_tokens, output_tokens)
                logger.info('org %s model %s, input_tokens %s cost %s, output_tokens %s cost %s', org_id, model, input_tokens, input_cost, output_tokens, output_cost)

                params = {
                    'org_id': org_id,
                    'month': month,
                    'model': model,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'cost': input_cost + output_cost,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                team_data.append(params)

        owner_data = []
        for username, models_dict in local_owner_stats.items():
            for model, usage in models_dict.items():
                input_tokens = usage.get('input_tokens') or 0
                output_tokens = usage.get('output_tokens') or 0

                input_cost, output_cost = self._calculate_token_cost(model, input_tokens, output_tokens)
                logger.info('owner %s model %s, input_tokens %s cost %s, output_tokens %s cost %s', username, model, input_tokens, input_cost, output_tokens, output_cost)

                params = {
                    'username': username,
                    'month': month,
                    'model': model,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'cost': input_cost + output_cost,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                owner_data.append(params)

        dt = datetime.utcnow()
        today = dt.date()
        delta = timedelta(days=dt.day - 1)
        first_day = today - delta
        session = self._db_session_class()
        try:
            if team_data:
                session.execute(text(self.TEAM_SQL), team_data)
            if owner_data:
                session.execute(text(self.OWNER_SQL), owner_data)
            if today == first_day and first_day not in RESET_AI_CREDIT_DATES:
                if len(RESET_AI_CREDIT_DATES) > 2:
                    RESET_AI_CREDIT_DATES.pop(0)
                session.execute(text(self.RESET_OWNER_AI_CREDIT_SQL))
                session.execute(text(self.RESET_TEAM_AI_CREDIT_SQL))
                RESET_AI_CREDIT_DATES.append(first_day)
            session.commit()
        except Exception as e:
            logger.exception(e)
        finally:
            session.close()
    
    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    self.stats_worker()
                except Exception as e:
                    logger.exception(e)
    
    def reset_stats(self):
        ORG_STATS.clear()
        OWNER_STATS.clear()
        
        
        
    def cancel(self):
        self.finished.set()
        

class AIStatsManager:
    def __init__(self):
        self.interval = 300

    def start(self):
        if not SEAFILE_AI_SECRET_KEY or not SEAFILE_AI_SERVER_URL:
            logger.warning('Can not start ai stats manager: secret key or server url is not set')
            return
        logger.info('Starts to receive ai calls')
        self._ai_stats_receiver_thread = AIStatsReceiver()
        self._ai_stats_receiver_thread.start()

        logger.info('Starting ai stats saver, interval = %s sec', self.interval)
        self._ai_stats_saver_thread = AIStatsSaver(self.interval)
        self._ai_stats_saver_thread.start()

