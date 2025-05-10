import json
import time
import datetime
import logging
from threading import Thread, Event
from sqlalchemy.exc import NoResultFound
from seafevents.app.event_redis import RedisClient
from seafevents.mq import get_mq
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from seafevents.app.cache_provider import cache

from seafevents.db import init_db_session_class
from .models import OrgQuotaUsage, UserQuotaUsage
from seafevents.utils.seafile_db import SeafileDB

storage_changed_repo_ids = {}

REPO_SIZE_TASK_CHANNEL_NAME = "repo_size_task"
CACHE_TIME_OUT = 24 * 60 * 60

class QuotaUsageCounter(object):
    
    def __init__(self, config, seafile_db=None):
        self._db_session_class = init_db_session_class(config)
        self.seafile_api = seafile_db
    
    def _get_org_id_by_repo_id(self, repo_id):
        
        cache_key = f"{repo_id}_org_id"
        org_id = cache.get(cache_key)
        if not org_id:
            org_id = self.seafile_api.get_org_id_by_repo_id(repo_id)
            cache.set(cache_key, org_id, CACHE_TIME_OUT)
        return int(org_id)
    
    def _get_repo_owner_by_repo_id(self, repo_id):
        cache_key = f"{repo_id}_repo_owner"
        repo_owner = cache.get(cache_key)
        if repo_owner:
            return repo_owner
        
        org_id = self._get_org_id_by_repo_id(repo_id)
        if org_id > 0:
            # org_user
            repo_owner = self.seafile_api.get_org_repo_owner(repo_id)
        else:
            repo_owner = self.seafile_api.get_repo_owner(repo_id)
        
        cache.set(cache_key, repo_owner, CACHE_TIME_OUT)
        return repo_owner
    
    def get_available_user_info_by_repo_id(self, repo_id):
        # user usage, user quota
        org_id = self._get_org_id_by_repo_id(repo_id)
        repo_owner = self._get_repo_owner_by_repo_id(repo_id)
        
        if not repo_owner:
            logging.warning(f'The repo {repo_id} has no repo_owner when counting quota usage.')
            return None, None, None, None
        
        if '@seafile_group' in repo_owner:
            # Ignore the repo infomations in department
            return None, None, None, None
        
        if org_id > 0:
            quota_usage = self.seafile_api.get_org_user_quota_usage(org_id, repo_owner)
            quota_total = self.seafile_api.get_org_user_quota(org_id, repo_owner)
        
        else:
            quota_usage = self.seafile_api.get_user_self_usage(repo_owner)
            quota_total = self.seafile_api.get_user_quota(repo_owner)
        
        return org_id, repo_owner, quota_usage, quota_total
    
    def get_available_org_info_by_repo_id(self, repo_id):
        # org usage, org quota
        org_id = self._get_org_id_by_repo_id(repo_id)
        if org_id > 0:
            org_quota_usage = self.seafile_api.get_org_quota_usage(org_id)
            org_quota_total = self.seafile_api.get_org_quota(org_id)
        else:
            return None, None, None
        
        return org_id, org_quota_usage, org_quota_total
    
    def save_user_quota_usage(self, repo_id):
        org_id, repo_owner, quota_usage, quota_total = self.get_available_user_info_by_repo_id(repo_id)
        if not repo_owner:
            return

        timestamp = datetime.datetime.utcnow()
        with self._db_session_class() as session:
            try:
                user_quota_usage_obj = session.query(UserQuotaUsage).filter_by(username=repo_owner).one()
                user_quota_usage_obj.usage = quota_usage
                user_quota_usage_obj.quota = quota_total
                user_quota_usage_obj.timestamp = timestamp
            except NoResultFound:
                user_quota_usage_obj = UserQuotaUsage(
                    org_id = org_id,
                    username = repo_owner,
                    usage = quota_usage,
                    quota = quota_total,
                    timestamp = timestamp
                )
                session.add(user_quota_usage_obj)
            
            session.commit()
    
    def save_org_quota_usage(self, repo_id):
        org_id, org_quota_usage, org_quota_total = self.get_available_org_info_by_repo_id(repo_id)
        if not org_id:
            return

        timestamp = datetime.datetime.utcnow()
        with self._db_session_class() as session:
            try:
                org_quota_usage_obj = session.query(OrgQuotaUsage).filter_by(org_id=org_id).one()
                org_quota_usage_obj.usage = org_quota_usage
                org_quota_usage_obj.quota = org_quota_total
                org_quota_usage_obj.timestamp = timestamp
            except NoResultFound:
                org_quota_usage_obj= OrgQuotaUsage(
                    org_id = org_id,
                    usage = org_quota_usage,
                    quota = org_quota_total,
                    timestamp = timestamp
                )
                session.add(org_quota_usage_obj)
                
            session.commit()
    
    def start_count(self):
        repos = storage_changed_repo_ids.keys()
        if len(repos) > 0:
            logging.info('Start counting quota usage by repos, current %s repos waiting to count' % len(repos))
        for repo_id in repos:
            try:
                self.save_org_quota_usage(repo_id)
                self.save_user_quota_usage(repo_id)
            except Exception as e:
                logging.exception(f'Counting quota usage error: {e}, repo_id: {repo_id}')
                continue
                


class RepoChangeInfoCollector(Thread):
    """
    Collect repo_id from redis queue and save to local variable
    """
    
    def __init__(self):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_client = RedisClient()
        self.mq_server = REDIS_HOST
        self.mq_port = REDIS_PORT
        self.mq_password = REDIS_PASSWORD
        self.mq = get_mq(self.mq_server, self.mq_port, self.mq_password)
    
    def run(self):
        logging.info('Starting handle redis channel')
        if not self._redis_client.connection:
            logging.warning('Can not start repo change collector: redis connection is not initialized')
            return
        
        while not self._finished.is_set():
            try:
                # Pop the repo info from redis queue
                res = self.mq.brpop(REPO_SIZE_TASK_CHANNEL_NAME, timeout=30)
                if res is not None:
                    key, value = res
                    repo_id = json.loads(value).get('repo_id')
                    storage_changed_repo_ids[repo_id] = 1
            
            except Exception as e:
                logging.error('Failed to collect repo change information: %s' % e)
                time.sleep(0.5)


class QuotaUsageSaveTimer(Thread):
    """
    Save user / org quota usage to databases
    """
    
    def __init__(self, config, interval):
        Thread.__init__(self)
        self._interval = interval
        self.config = config
        self.finished = Event()
        self._redis_client = RedisClient()
    
    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                try:
                    with SeafileDB() as seafile_db:
                        QuotaUsageCounter(self.config, seafile_db).start_count()
                        
                    storage_changed_repo_ids.clear()
                except Exception as e:
                    logging.exception('Save quota usage error: %s', e)
    
    def cancel(self):
        self.finished.set()


class QuotaUsageManager(object):
    def __init__(self, config):
        self._interval = 30
        self.config = config
    
    def start(self):
        logging.info('Starting quota usage saver timer, interval = %s sec', self._interval)
        self._quota_usage_recorder = QuotaUsageSaveTimer(self.config, self._interval)
        self._quota_usage_recorder.start()
        
        logging.info('Starting repo change collector')
        self._repo_change_collector = RepoChangeInfoCollector()
        self._repo_change_collector.start()
