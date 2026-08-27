import json
import logging
import threading
import time
import uuid

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError, TimeoutError as RedisTimeoutError

from seafevents.app.config import (
    JWT_PRIVATE_KEY, SDOC_REVIEW_WORKERS, SEAFILE_AI_SECRET_KEY,
    SEAFILE_AI_SERVER_URL, SEAHUB_SERVER_URL,
)
from seafevents.app.event_redis import RedisClient
from seafevents.sdoc_review.api import InternalAPIError, SeahubReviewAPI, SeafileAIReviewAPI


logger = logging.getLogger('sdoc_review')

QUEUE_NAME = 'sdoc_review_task'
APPLY_RESULT_QUEUE_NAME = 'sdoc_review_apply_result'
TOTAL_TIMEOUT_SECONDS = 180
MODEL_CALL_TIMEOUT_SECONDS = 30
RECOVERY_INTERVAL_SECONDS = 30


class SdocReviewWorker(object):
    """Consume SDoc review tasks in the existing SeafEvents background process."""

    def __init__(self):
        self.mq = RedisClient().connection
        self.seahub_api = SeahubReviewAPI(SEAHUB_SERVER_URL, JWT_PRIVATE_KEY)
        self.seafile_ai_api = SeafileAIReviewAPI(
            SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY)
        self.should_stop = threading.Event()
        self.worker_list = []

    def start(self):
        if not self.mq:
            logger.error('Cannot start SDoc review worker: Redis is not configured.')
            return
        if not JWT_PRIVATE_KEY or not SEAHUB_SERVER_URL:
            logger.error('Cannot start SDoc review worker: Seahub internal API is not configured.')
            return
        if not SEAFILE_AI_SERVER_URL or not SEAFILE_AI_SECRET_KEY:
            logger.error('Cannot start SDoc review worker: Seafile AI is not configured.')
            return
        for index in range(max(1, SDOC_REVIEW_WORKERS)):
            thread = threading.Thread(
                target=self._worker_handler,
                name='sdoc_review_worker_thread_%s' % index,
                daemon=True,
            )
            thread.start()
            self.worker_list.append(thread)
        apply_thread = threading.Thread(
            target=self._apply_result_handler,
            name='sdoc_review_apply_result_thread',
            daemon=True,
        )
        apply_thread.start()
        self.worker_list.append(apply_thread)

    def _worker_handler(self):
        logger.info('%s starting SDoc review worker', threading.current_thread().name)
        next_recovery_at = 0
        while not self.should_stop.is_set():
            try:
                now = time.monotonic()
                if now >= next_recovery_at:
                    self._recover_queued_tasks()
                    next_recovery_at = now + RECOVERY_INTERVAL_SECONDS
                result = self.mq.brpop(QUEUE_NAME, timeout=5)
                if result is None:
                    continue
                task_id = self._parse_task_id(result[1])
                if task_id:
                    self._process_task(task_id)
            except (ResponseError, RedisConnectionError, RedisTimeoutError) as error:
                logger.error('SDoc review Redis connection failed: %s', error)
                self.should_stop.wait(1)
            except Exception:
                logger.exception('Unexpected SDoc review worker error.')
                self.should_stop.wait(1)

    @staticmethod
    def _parse_task_id(value):
        try:
            data = json.loads(value)
        except (TypeError, ValueError):
            return None
        task_id = data.get('task_id') if isinstance(data, dict) else None
        return str(task_id) if task_id else None

    @staticmethod
    def _parse_apply_attempt_id(value):
        try:
            data = json.loads(value)
        except (TypeError, ValueError):
            return None
        apply_attempt_id = data.get('apply_attempt_id') if isinstance(data, dict) else None
        return str(apply_attempt_id) if apply_attempt_id else None

    def _recover_queued_tasks(self):
        try:
            result = self.seahub_api.pending()
            for task_id in result.get('task_ids') or []:
                self.mq.lpush(QUEUE_NAME, json.dumps({'task_id': str(task_id)}))
        except Exception as error:
            logger.warning('Failed to reconcile queued SDoc review tasks: %s', error)

    def _apply_result_handler(self):
        logger.info('%s starting SDoc apply reconciliation worker', threading.current_thread().name)
        next_recovery_at = 0
        while not self.should_stop.is_set():
            try:
                now = time.monotonic()
                if now >= next_recovery_at:
                    result = self.seahub_api.pending()
                    for apply_attempt_id in result.get('apply_attempt_ids') or []:
                        self.mq.lpush(
                            APPLY_RESULT_QUEUE_NAME,
                            json.dumps({'apply_attempt_id': str(apply_attempt_id)}))
                    next_recovery_at = now + RECOVERY_INTERVAL_SECONDS
                result = self.mq.brpop(APPLY_RESULT_QUEUE_NAME, timeout=5)
                if result is None:
                    continue
                apply_attempt_id = self._parse_apply_attempt_id(result[1])
                if apply_attempt_id:
                    self._process_apply_attempt(apply_attempt_id)
            except (ResponseError, RedisConnectionError, RedisTimeoutError) as error:
                logger.error('SDoc apply reconciliation Redis connection failed: %s', error)
                self.should_stop.wait(1)
            except Exception:
                logger.exception('Unexpected SDoc apply reconciliation error.')
                self.should_stop.wait(1)

    def _process_apply_attempt(self, apply_attempt_id):
        for delay in (0, 1, 3, 10, 30):
            if delay and self.should_stop.wait(delay):
                return
            try:
                result = self.seahub_api.reconcile_apply(apply_attempt_id)
                if result.get('terminal'):
                    return
            except InternalAPIError as error:
                if error.status_code in (404, 409):
                    return
                logger.warning(
                    'Failed to reconcile SDoc apply attempt %s: %s',
                    apply_attempt_id, error)
            except Exception as error:
                logger.warning(
                    'Failed to reconcile SDoc apply attempt %s: %s',
                    apply_attempt_id, error)

    @staticmethod
    def _remaining(deadline):
        return max(0, deadline - time.monotonic())

    @classmethod
    def _model_timeout(cls, deadline):
        remaining = cls._remaining(deadline)
        if remaining <= 1:
            raise TimeoutError('SDoc review total time budget exhausted')
        return max(1, min(MODEL_CALL_TIMEOUT_SECONDS, remaining))

    @classmethod
    def _model_payload(cls, task, document_context, deadline, **extra):
        remaining = cls._remaining(deadline)
        payload = {
            'prompt': task['prompt'],
            'document_context': document_context,
            'username': task['username'],
            'org_id': task.get('org_id'),
            'repo_id': task['repo_id'],
            'scenario': 'chat',
            'request_timeout_seconds': max(1, min(28, int(remaining) - 1)),
        }
        payload.update(extra)
        return payload

    def _process_task(self, task_id):
        attempt_id = uuid.uuid4()
        deadline = time.monotonic() + TOTAL_TIMEOUT_SECONDS
        try:
            claimed = self.seahub_api.claim(task_id, attempt_id)
        except InternalAPIError as error:
            if error.status_code != 409:
                logger.warning('Failed to claim SDoc review task %s: %s', task_id, error)
            return

        task = claimed.get('task') or {}
        document_context = claimed.get('document_context')
        if not task or not isinstance(document_context, dict):
            self._fail(task_id, attempt_id, 'invalid_claim_result')
            return

        try:
            if task.get('route') == 'answer_then_review':
                try:
                    analysis = self.seafile_ai_api.analyze(
                        self._model_payload(task, document_context, deadline),
                        self._model_timeout(deadline),
                    )
                    if analysis:
                        self.seahub_api.event(
                            task_id, attempt_id, 'analysis', content=analysis)
                except InternalAPIError as error:
                    if error.status_code == 409:
                        return
                    logger.exception(
                        'Failed to persist analysis for SDoc review task %s; continuing.', task_id)
                except Exception:
                    logger.exception(
                        'Failed to generate analysis for SDoc review task %s; continuing.', task_id)

            plan = self.seafile_ai_api.plan(
                self._model_payload(task, document_context, deadline),
                self._model_timeout(deadline),
            )
            chunks = plan.get('chunks') if isinstance(plan, dict) else None
            brief = plan.get('brief') if isinstance(plan, dict) else None
            if not isinstance(chunks, list) or not chunks:
                raise ValueError('SDoc review plan contains no chunks')
            total_blocks = sum(
                len(chunk.get('block_ids') or [])
                for chunk in chunks if isinstance(chunk, dict)
            )
            self.seahub_api.event(
                task_id, attempt_id, 'begin',
                document_context=document_context,
                brief=brief,
                total_chunks=len(chunks),
                total_blocks=total_blocks,
            )

            truncated = False
            stop_reason = None
            for chunk in chunks:
                if self._remaining(deadline) <= 1:
                    truncated = True
                    stop_reason = 'time_budget_exhausted'
                    break
                chunk_index = chunk.get('chunk_index') if isinstance(chunk, dict) else None
                if not isinstance(chunk_index, int):
                    truncated = True
                    stop_reason = stop_reason or 'invalid_chunk_plan'
                    continue
                try:
                    suggestions = self.seafile_ai_api.chunk(
                        self._model_payload(
                            task, document_context, deadline,
                            brief=brief, chunk_index=chunk_index,
                        ),
                        self._model_timeout(deadline),
                    )
                    if not isinstance(suggestions, list):
                        raise ValueError('Invalid SDoc review chunk result')
                    result = self.seahub_api.event(
                        task_id, attempt_id, 'chunk',
                        document_context=document_context,
                        chunk_index=chunk_index,
                        block_count=len(chunk.get('block_ids') or []),
                        suggestions=suggestions,
                    )
                    if result.get('limit_reached'):
                        truncated = True
                        stop_reason = 'suggestion_limit_reached'
                        break
                except InternalAPIError as error:
                    if error.status_code == 409:
                        return
                    logger.warning(
                        'SDoc review task %s chunk %s failed: %s',
                        task_id, chunk_index, error,
                    )
                    truncated = True
                    stop_reason = stop_reason or 'chunk_generation_failed'
                except Exception as error:
                    logger.warning(
                        'SDoc review task %s chunk %s failed: %s',
                        task_id, chunk_index, error,
                    )
                    truncated = True
                    stop_reason = stop_reason or 'chunk_generation_failed'

            self.seahub_api.event(
                task_id, attempt_id, 'finish',
                document_context=document_context,
                truncated=truncated,
                stop_reason=stop_reason,
            )
        except InternalAPIError as error:
            if error.status_code == 409:
                return
            logger.exception('SDoc review task %s failed.', task_id)
            self._fail(task_id, attempt_id, 'generation_failed')
        except Exception:
            logger.exception('SDoc review task %s failed.', task_id)
            self._fail(task_id, attempt_id, 'generation_failed')

    def _fail(self, task_id, attempt_id, error_code):
        try:
            self.seahub_api.event(
                task_id, attempt_id, 'failed', error_code=error_code)
        except Exception:
            logger.exception('Failed to persist failure for SDoc review task %s.', task_id)
