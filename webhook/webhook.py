import json
import logging
from datetime import datetime
from threading import Event, Thread, current_thread
from queue import Queue
import time

from requests.exceptions import ReadTimeout
from sqlalchemy import select, text

from seafevents.app.event_redis import RedisClient
from seafevents.db import init_db_session_class
from seafevents.webhook.models import Webhooks, WebhookJobs, PENDING, FAILURE
from seafevents.webhook.http import post_webhook
from seafevents.app.cache_provider import cache
from seafevents.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

from seaserv import seafile_api

WEBHOOK_ERROR_CACHE_PREFIX = 'webhook_error_'
WEBHOOK_ERROR_TIMES_CACHE_TIMEOUT = 24 * 60 * 60
WEBHOOK_ALLOW_ERROR_TIMES = 5

RESPONSE_TEXT_LIMIT = 65500


class Webhooker(object):
    """
    There are a few steps in this program:
    1. subscribe events.
    2. query webhooks and generate jobs, then put them to queue.
    3. trigger jobs one by one.
    """
    SUBSCRIPTION_IDLE_TIMEOUT = 5 * 60

    def __init__(self):
        self._db_session_class = init_db_session_class()
        self.job_queue = Queue()
        self._redis_client = RedisClient()
        self._finished = Event()

    def start(self):
        if not self._redis_client.connection:
            logging.warning('Redis has not been set up, webhooker will not start.')
            return
        logging.info('Starting handle webhook jobs...')
        tds = [Thread(target=self.add_jobs, name='webhook_events')]
        tds.extend([Thread(target=self.trigger_jobs, name='trigger_%s' % i) for i in range(2)])
        [td.start() for td in tds]

    def _subscribe_repo_updates(self):
        logging.info('Webhook subscribing: channel=repo_update redis_host=%s redis_port=%s thread=%s',
                     REDIS_HOST, REDIS_PORT, current_thread().name)
        try:
            subscriber = self._redis_client.get_subscriber('repo_update')
        except Exception:
            logging.exception('Webhook subscription failed: channel=repo_update thread=%s',
                              current_thread().name)
            raise
        logging.info('Webhook subscribe command sent: channel=repo_update thread=%s',
                     current_thread().name)
        return subscriber

    def add_jobs(self):
        subscriber = self._subscribe_repo_updates()
        last_message_time = time.monotonic()
        while not self._finished.is_set():
            try:
                msg = subscriber.get_message()
                if msg:
                    last_message_time = time.monotonic()
                    try:
                        data = json.loads(msg.get('data'))
                    except Exception as e:
                        logging.error('parse message error: %s' % e)
                        continue
                    session = self._db_session_class()
                    try:
                        repo_id = data.get('repo_id')
                        logging.info('Webhook event received: channel=repo_update repo=%s thread=%s',
                                     repo_id, current_thread().name)
                        stmt = select(Webhooks).where(Webhooks.repo_id == repo_id, Webhooks.is_valid == 1)
                        hooks = session.scalars(stmt).all()
                        logging.info('Webhook matching completed: repo=%s valid_webhooks=%s', repo_id, len(hooks))
                        for hook in hooks:
                            request_body = {"repo_id": repo_id}
                            request_headers = hook.gen_request_headers(request_body)
                            job = {'webhook_id': hook.id, 'created_at': datetime.now(), 'status': PENDING,
                                   'url': hook.url, 'request_headers': request_headers, 'request_body': request_body}
                            self.job_queue.put(job)
                            logging.info('Webhook job queued: repo=%s webhook_id=%s queue_size=%s',
                                         repo_id, hook.id, self.job_queue.qsize())
                    except Exception as e:
                        logging.error('add jobs error: %s' % e)
                    finally:
                        session.close()
                else:
                    if time.monotonic() - last_message_time >= self.SUBSCRIPTION_IDLE_TIMEOUT:
                        logging.info('Webhook subscription idle for %s seconds; recreating subscription: '
                                     'channel=repo_update thread=%s',
                                     self.SUBSCRIPTION_IDLE_TIMEOUT, current_thread().name)
                        subscriber.close()
                        subscriber = self._subscribe_repo_updates()
                        last_message_time = time.monotonic()
                    time.sleep(1)
            except Exception as e:
                logging.error('Failed to msg: %s' % e)
                logging.info('Webhook resubscribing after event handling failure: channel=repo_update thread=%s',
                             current_thread().name)
                subscriber = self._subscribe_repo_updates()
                last_message_time = time.monotonic()

    def invalidate_webhook(self, webhook_id, db_session):
        sql = "UPDATE webhooks SET is_valid=0 WHERE id=:webhook_id"
        try:
            db_session.execute(text(sql), {'webhook_id': webhook_id})
            db_session.commit()
            logging.info('Webhook invalidated: webhook_id=%s', webhook_id)
        except Exception as e:
            logging.error('invalidate webhook: %s error: %s', webhook_id, e)

    def get_webhook_error_times(self, cache_key):
        webhook_error_times = cache.get(cache_key)
        if not webhook_error_times:
            webhook_error_times = 0
        return int(webhook_error_times)

    def save_webhook_job(self, session, job_params):
        webhook_job = WebhookJobs(*job_params)
        session.add(webhook_job)
        session.commit()

    def trigger_jobs(self):
        logging.info('Webhook delivery worker started: thread=%s', current_thread().name)
        while True:
            try:
                job = self.job_queue.get()
                session = self._db_session_class()
                need_invalidate = False
                webhook_error_cache_key = WEBHOOK_ERROR_CACHE_PREFIX + str(job['webhook_id'])
                try:
                    body = job.get('request_body')
                    headers = job.get('request_headers')
                    started = time.monotonic()
                    logging.info('Webhook delivery started: repo=%s webhook_id=%s thread=%s queue_size=%s',
                                 body.get('repo_id'), job['webhook_id'], current_thread().name,
                                 self.job_queue.qsize())
                    response = post_webhook(job['url'], body, headers, timeout=30)
                except ReadTimeout:
                    logging.info('Webhook delivery completed: repo=%s webhook_id=%s thread=%s '
                                 'result=timeout elapsed_seconds=%.3f',
                                 body.get('repo_id'), job['webhook_id'], current_thread().name,
                                 time.monotonic() - started)
                    logging.warning('request webhook url: %s timeout', job['url'])

                    job_params = (job['webhook_id'], job['created_at'], datetime.now(), FAILURE,
                                job['url'], job['request_headers'], job['request_body'], None, None)
                    self.save_webhook_job(session, job_params)

                    webhook_error_times = self.get_webhook_error_times(webhook_error_cache_key) + 1
                    if webhook_error_times >= WEBHOOK_ALLOW_ERROR_TIMES:
                        need_invalidate = True
                    cache.set(webhook_error_cache_key,
                                        webhook_error_times,
                                        timeout=WEBHOOK_ERROR_TIMES_CACHE_TIMEOUT
                                        )
                except Exception as e:
                    logging.info('Webhook delivery completed: repo=%s webhook_id=%s thread=%s '
                                 'result=error error_type=%s elapsed_seconds=%.3f',
                                 body.get('repo_id'), job['webhook_id'], current_thread().name,
                                 type(e).__name__, time.monotonic() - started)
                    logging.warning('request webhook url: %s error: %s', job['url'], e)
                    need_invalidate = True

                    job_params = (job['webhook_id'], job['created_at'], datetime.now(), FAILURE,
                                job['url'], job['request_headers'], job['request_body'], None, None)
                    self.save_webhook_job(session, job_params)
                else:
                    logging.info('Webhook delivery completed: repo=%s webhook_id=%s thread=%s '
                                 'result=%s status_code=%s elapsed_seconds=%.3f',
                                 body.get('repo_id'), job['webhook_id'], current_thread().name,
                                 'success' if 200 <= response.status_code < 300 else 'http_error',
                                 response.status_code, time.monotonic() - started)
                    if 200 <= response.status_code < 300:
                        cache.delete(webhook_error_cache_key)
                        continue
                    else:
                        job_params = (job['webhook_id'], job['created_at'], datetime.now(), FAILURE, job['url'],
                                    job['request_headers'], job['request_body'], response.status_code, response.text[:RESPONSE_TEXT_LIMIT])
                        self.save_webhook_job(session, job_params)

                        webhook_error_times = self.get_webhook_error_times(webhook_error_cache_key) + 1
                        if webhook_error_times >= WEBHOOK_ALLOW_ERROR_TIMES:
                            need_invalidate = True
                        cache.set(webhook_error_cache_key,
                                            webhook_error_times,
                                            timeout=WEBHOOK_ERROR_TIMES_CACHE_TIMEOUT
                                            )
                finally:
                    if need_invalidate:
                        self.invalidate_webhook(job['webhook_id'], session)
                        cache.delete(webhook_error_cache_key)
                    session.close()
            except Exception as e:
                logging.error('trigger job error: %s' % e)
