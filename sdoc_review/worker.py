import json
import logging
import threading
import time
import uuid

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError, TimeoutError as RedisTimeoutError
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout

from seafevents.app.config import (
    JWT_PRIVATE_KEY, SDOC_REVIEW_WORKERS, SEAFILE_AI_SECRET_KEY,
    SEAFILE_AI_SERVER_URL, SEAHUB_SERVER_URL,
)
from seafevents.app.event_redis import RedisClient
from seafevents.sdoc_review.api import InternalAPIError, SeahubReviewAPI, SeafileAIReviewAPI


logger = logging.getLogger('sdoc_review')

QUEUE_NAME = 'sdoc_review_task'
APPLY_RESULT_QUEUE_NAME = 'sdoc_review_apply_result'
# A Review may span many chunks. Its lifetime is controlled by the renewable
# Seahub lease, while each individual provider request remains bounded.
MODEL_CALL_TIMEOUT_SECONDS = 90
MODEL_REQUEST_TIMEOUT_SECONDS = MODEL_CALL_TIMEOUT_SECONDS - 2
RECOVERY_INTERVAL_SECONDS = 30
CLAIM_RESPONSE_RETRY_DELAYS = (0, 1, 3)
CHUNK_TRANSIENT_RETRY_DELAYS = (0, 1)
REVISION_BRIEF_REQUIRED_STRING_FIELDS = (
    'goal', 'tone', 'length', 'heading_strategy', 'do_not_modify',
)


class ReviewTaskNoLongerRunning(Exception):
    pass


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
    def _model_timeout():
        return MODEL_CALL_TIMEOUT_SECONDS

    @staticmethod
    def _generation_error_code(error):
        if isinstance(error, TimeoutError):
            return 'generation_timeout'
        return 'seafile_ai_error'

    @staticmethod
    def _is_valid_revision_brief(brief):
        if not isinstance(brief, dict):
            return False
        if any(not isinstance(brief.get(field), str) or not brief[field].strip()
               for field in REVISION_BRIEF_REQUIRED_STRING_FIELDS):
            return False
        terminology = brief.get('terminology')
        return isinstance(terminology, list) and all(
            isinstance(term, str) and term.strip() for term in terminology)

    @classmethod
    def _model_payload(cls, task, document_context, **extra):
        payload = {
            'prompt': task['prompt'],
            'document_context': document_context,
            'username': task['username'],
            'org_id': task.get('org_id'),
            'repo_id': task['repo_id'],
            'scenario': 'chat',
            # Let Seafile AI return a typed failure before the worker's own
            # HTTP client reaches its timeout.
            'request_timeout_seconds': MODEL_REQUEST_TIMEOUT_SECONDS,
        }
        payload.update(extra)
        return payload

    def _wait_or_stop(self, delay):
        stop_event = getattr(self, 'should_stop', None)
        if stop_event is not None:
            return stop_event.wait(delay)
        time.sleep(delay)
        return False

    def _claim_task(self, task_id, attempt_id):
        """Retry an ambiguous claim with the same idempotency key.

        Seahub accepts a repeated claim for the same ``attempt_id``. Retrying
        with that value recovers the common case where Seahub claimed the task
        but the HTTP response was lost before this worker received its context.
        """
        for delay in CLAIM_RESPONSE_RETRY_DELAYS:
            if delay and self._wait_or_stop(delay):
                return None
            try:
                return self.seahub_api.claim(task_id, attempt_id)
            except InternalAPIError as error:
                if error.status_code == 409:
                    return None
                logger.warning('Failed to claim SDoc review task %s: %s', task_id, error)
            except Exception as error:
                logger.warning('Failed to claim SDoc review task %s: %s', task_id, error)
        return None

    def _renew_lease(self, task_id, attempt_id):
        """Keep the durable task lease alive before bounded model calls.

        A cancelled or stale task is deliberately not resumed. A transient
        heartbeat delivery error is logged; normal progress callbacks still
        renew the existing lease when delivery recovers.
        """
        try:
            self.seahub_api.heartbeat(task_id, attempt_id)
            return True
        except InternalAPIError as error:
            if error.status_code == 409:
                return False
            logger.warning('Failed to renew SDoc review lease for task %s: %s', task_id, error)
        except Exception as error:
            logger.warning('Failed to renew SDoc review lease for task %s: %s', task_id, error)
        return True

    @staticmethod
    def _is_transient_chunk_error(error):
        if isinstance(error, InternalAPIError):
            try:
                error_code = json.loads(str(error)).get('error_code')
            except (TypeError, ValueError, AttributeError):
                error_code = None
            if error_code in ('invalid_model_response', 'model_output_truncated'):
                return False
            return error.status_code >= 500
        return isinstance(error, (
            TimeoutError, ConnectionError, RequestsTimeout, RequestsConnectionError,
        ))

    def _generate_chunk(self, task_id, attempt_id, task, document_context,
                        brief, chunk_index, plan_token):
        last_error = None
        for attempt, delay in enumerate(CHUNK_TRANSIENT_RETRY_DELAYS):
            if delay and self._wait_or_stop(delay):
                raise TimeoutError('SDoc review worker is stopping')
            if not self._renew_lease(task_id, attempt_id):
                raise ReviewTaskNoLongerRunning()
            try:
                return self.seafile_ai_api.chunk(
                    self._model_payload(
                        task, document_context,
                        brief=brief, chunk_index=chunk_index, plan_token=plan_token,
                    ),
                    self._model_timeout(),
                )
            except Exception as error:
                last_error = error
                if not self._is_transient_chunk_error(error):
                    raise
                if attempt + 1 >= len(CHUNK_TRANSIENT_RETRY_DELAYS):
                    raise
                logger.warning(
                    'SDoc review task %s chunk %s transient generation failure; retrying: %s',
                    task_id, chunk_index, error,
                )
        raise last_error

    def _process_task(self, task_id):
        attempt_id = uuid.uuid4()
        claimed = self._claim_task(task_id, attempt_id)
        if claimed is None:
            return

        task = claimed.get('task') or {}
        document_context = claimed.get('document_context')
        if not task or not isinstance(document_context, dict):
            self._fail(task_id, attempt_id, 'invalid_claim_result')
            return

        try:
            if task.get('route') == 'answer_then_review':
                try:
                    if not self._renew_lease(task_id, attempt_id):
                        return
                    analysis = self.seafile_ai_api.analyze(
                        self._model_payload(task, document_context),
                        self._model_timeout(),
                    )
                except Exception as error:
                    logger.warning(
                        'Failed to generate analysis for SDoc review task %s; continuing: %s',
                        task_id, error)
                else:
                    if analysis:
                        try:
                            self.seahub_api.event(
                                task_id, attempt_id, 'analysis', content=analysis)
                        except InternalAPIError as error:
                            if error.status_code == 409:
                                return
                            logger.warning(
                                'Failed to persist analysis for SDoc review task %s; continuing: %s',
                                task_id, error)
                        except Exception as error:
                            logger.warning(
                                'Failed to persist analysis for SDoc review task %s; continuing: %s',
                                task_id, error)

            try:
                if not self._renew_lease(task_id, attempt_id):
                    return
                plan = self.seafile_ai_api.plan(
                    self._model_payload(task, document_context),
                    self._model_timeout(),
                )
            except Exception as error:
                logger.warning('SDoc review task %s plan generation failed: %s', task_id, error)
                self._fail(task_id, attempt_id, self._generation_error_code(error))
                return
            chunks = plan.get('chunks') if isinstance(plan, dict) else None
            brief = plan.get('brief') if isinstance(plan, dict) else None
            plan_token = plan.get('plan_token') if isinstance(plan, dict) else None
            if not isinstance(chunks, list) or not chunks or not isinstance(plan_token, str) or not plan_token:
                self._fail(task_id, attempt_id, 'invalid_chunk_plan')
                return
            total_blocks = sum(
                len(chunk.get('block_ids') or [])
                for chunk in chunks if isinstance(chunk, dict)
            )
            if len(chunks) > 1 and not self._is_valid_revision_brief(brief):
                self._fail(task_id, attempt_id, 'invalid_revision_brief')
                return
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
                chunk_index = chunk.get('chunk_index') if isinstance(chunk, dict) else None
                if not isinstance(chunk_index, int):
                    self._fail(task_id, attempt_id, 'invalid_chunk_plan')
                    return
                try:
                    suggestions = self._generate_chunk(
                        task_id, attempt_id, task, document_context,
                        brief, chunk_index, plan_token)
                except ReviewTaskNoLongerRunning:
                    return
                except Exception as error:
                    logger.warning(
                        'SDoc review task %s chunk %s generation failed: %s',
                        task_id, chunk_index, error,
                    )
                    self._fail(task_id, attempt_id, self._generation_error_code(error))
                    return

                if not isinstance(suggestions, list):
                    self._fail(task_id, attempt_id, 'invalid_chunk_response')
                    return

                try:
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
                        'SDoc review task %s chunk %s persistence failed: %s',
                        task_id, chunk_index, error,
                    )
                    self._fail(task_id, attempt_id, 'event_delivery_failed')
                    return
                except Exception as error:
                    logger.warning(
                        'SDoc review task %s chunk %s persistence failed: %s',
                        task_id, chunk_index, error,
                    )
                    self._fail(task_id, attempt_id, 'event_delivery_failed')
                    return

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
