import jwt

from seafevents.sdoc_review.api import InternalAPIError, SeahubReviewAPI, SeafileAIReviewAPI
from seafevents.sdoc_review.worker import SdocReviewWorker


class FakeResponse(object):
    ok = True
    status_code = 200
    text = ''

    def __init__(self, data):
        self.data = data

    def json(self):
        return self.data


def test_seahub_review_api_uses_scoped_internal_token(monkeypatch):
    captured = {}

    def fake_post(url, json, headers, timeout):
        captured.update(url=url, json=json, headers=headers, timeout=timeout)
        return FakeResponse({'task_ids': []})

    monkeypatch.setattr('seafevents.sdoc_review.api.requests.post', fake_post)
    private_key = 'internal-secret-at-least-32-bytes-long'
    api = SeahubReviewAPI('http://seahub:8000', private_key)

    assert api.pending() == {'task_ids': []}
    token = captured['headers']['Authorization'].split()[1]
    payload = jwt.decode(token, private_key, algorithms=['HS256'])
    assert payload['is_internal'] is True
    assert payload['purpose'] == 'sdoc_review_worker'
    assert payload['audience'] == 'seahub_sdoc_review'


def test_seafile_ai_review_api_extracts_chunk_items(monkeypatch):
    def fake_post(url, json, headers, timeout):
        return FakeResponse({'items': [{'kind': 'replace_block_text'}]})

    monkeypatch.setattr('seafevents.sdoc_review.api.requests.post', fake_post)
    api = SeafileAIReviewAPI(
        'http://seafile-ai:8888', 'seafile-ai-secret-at-least-32-bytes')

    assert api.chunk({'prompt': 'improve'}, timeout=30) == [
        {'kind': 'replace_block_text'},
    ]


class FakeSeahubAPI(object):
    def __init__(self):
        self.events = []
        self.heartbeats = []

    def claim(self, task_id, attempt_id):
        return {
            'task': {
                'id': task_id,
                'prompt': 'Improve the document',
                'route': 'review',
                'username': 'user@example.com',
                'org_id': None,
                'repo_id': 'repo-id',
            },
            'document_context': {
                'file_uuid': 'file-uuid',
                'blocks': [{'block_id': 'block-1'}],
            },
        }

    def event(self, task_id, attempt_id, event_type, **payload):
        if event_type == 'heartbeat':
            self.heartbeats.append((task_id, attempt_id))
        else:
            self.events.append((event_type, payload))
        return {'accepted': True, 'limit_reached': False}

    def heartbeat(self, task_id, attempt_id):
        return self.event(task_id, attempt_id, 'heartbeat')


class FakeApplySeahubAPI(object):
    def __init__(self):
        self.calls = 0

    def reconcile_apply(self, apply_attempt_id):
        self.calls += 1
        return {'terminal': self.calls >= 2}


class FakeSeafileAIAPI(object):
    def __init__(self):
        self.chunk_calls = 0

    def plan(self, payload, timeout):
        assert payload['request_timeout_seconds'] <= 88
        assert payload['repo_id'] == 'repo-id'
        assert payload['review_task_id'] == '00000000-0000-4000-8000-000000000001'
        assert payload['generation_attempt_id']
        return {
            'brief': {
                'goal': 'Improve clarity', 'tone': 'concise', 'length': 'preserve length',
                'terminology': [], 'heading_strategy': 'preserve headings',
                'do_not_modify': 'facts',
            },
            'plan_token': 'frozen-plan-token',
            'chunks': [
                {'chunk_index': 0, 'block_ids': ['block-1']},
                {'chunk_index': 1, 'block_ids': ['block-2']},
            ],
        }

    def chunk(self, payload, timeout):
        self.chunk_calls += 1
        return [{
            'kind': 'replace_block_text',
            'block_id': 'block-%s' % (payload['chunk_index'] + 1),
        }]


def test_worker_persists_progressive_review_events():
    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = FakeSeahubAPI()
    worker.seafile_ai_api = FakeSeafileAIAPI()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    event_types = [event_type for event_type, _payload in worker.seahub_api.events]
    assert event_types == ['begin', 'chunk', 'chunk', 'finish']
    assert worker.seahub_api.events[0][1]['total_chunks'] == 2
    assert worker.seahub_api.events[-1][1]['truncated'] is False
    assert len(worker.seahub_api.heartbeats) == 3


def test_worker_recovers_when_the_first_claim_response_is_lost():
    class LostClaimResponseSeahubAPI(FakeSeahubAPI):
        def __init__(self):
            super(LostClaimResponseSeahubAPI, self).__init__()
            self.claim_attempt_ids = []

        def claim(self, task_id, attempt_id):
            self.claim_attempt_ids.append(attempt_id)
            if len(self.claim_attempt_ids) == 1:
                raise RuntimeError('claim response lost')
            return super(LostClaimResponseSeahubAPI, self).claim(task_id, attempt_id)

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = LostClaimResponseSeahubAPI()
    worker.seafile_ai_api = FakeSeafileAIAPI()
    worker.should_stop = type('StopEvent', (), {'wait': lambda self, delay: False})()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert len(worker.seahub_api.claim_attempt_ids) == 2
    assert worker.seahub_api.claim_attempt_ids[0] == worker.seahub_api.claim_attempt_ids[1]
    assert [event_type for event_type, _payload in worker.seahub_api.events] == [
        'begin', 'chunk', 'chunk', 'finish',
    ]


def test_worker_passes_the_plan_brief_to_every_chunk():
    brief = {
        'goal': 'Improve clarity', 'tone': 'concise', 'length': 'preserve length',
        'terminology': ['SDoc'], 'heading_strategy': 'preserve headings',
        'do_not_modify': 'facts',
    }

    class BriefSeafileAIAPI(FakeSeafileAIAPI):
        def __init__(self):
            super(BriefSeafileAIAPI, self).__init__()
            self.chunk_briefs = []

        def plan(self, payload, timeout):
            plan = super(BriefSeafileAIAPI, self).plan(payload, timeout)
            plan['brief'] = brief
            return plan

        def chunk(self, payload, timeout):
            self.chunk_briefs.append(payload['brief'])
            assert payload['plan_token'] == 'frozen-plan-token'
            return super(BriefSeafileAIAPI, self).chunk(payload, timeout)

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = FakeSeahubAPI()
    worker.seafile_ai_api = BriefSeafileAIAPI()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert worker.seafile_ai_api.chunk_briefs == [brief, brief]


def test_worker_fails_when_any_multi_chunk_plan_has_an_invalid_brief():
    class InvalidBriefSeafileAIAPI(FakeSeafileAIAPI):
        def plan(self, payload, timeout):
            return {
                'brief': {},
                'plan_token': 'frozen-plan-token',
                'chunks': [
                    {'chunk_index': 0, 'block_ids': ['block-1']},
                    {'chunk_index': 1, 'block_ids': ['block-2']},
                ],
            }

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = FakeSeahubAPI()
    worker.seafile_ai_api = InvalidBriefSeafileAIAPI()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert [event_type for event_type, _payload in worker.seahub_api.events] == ['failed']
    assert worker.seahub_api.events[-1][1]['error_code'] == 'invalid_revision_brief'


def test_worker_reconciles_apply_outside_generation_thread():
    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = FakeApplySeahubAPI()
    worker.should_stop = type('StopEvent', (), {'wait': lambda self, delay: False})()

    worker._process_apply_attempt('00000000-0000-4000-8000-000000000002')

    assert worker.seahub_api.calls == 2


def test_worker_stops_after_seahub_rejects_cancelled_task():
    class CancelledSeahubAPI(FakeSeahubAPI):
        def event(self, task_id, attempt_id, event_type, **payload):
            if event_type == 'heartbeat':
                return super(CancelledSeahubAPI, self).event(
                    task_id, attempt_id, event_type, **payload)
            self.events.append((event_type, payload))
            if event_type == 'chunk':
                raise InternalAPIError(409, 'Review attempt is stale.')
            return {'accepted': True, 'limit_reached': False}

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = CancelledSeahubAPI()
    worker.seafile_ai_api = FakeSeafileAIAPI()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert worker.seafile_ai_api.chunk_calls == 1
    assert [event_type for event_type, _payload in worker.seahub_api.events] == [
        'begin', 'chunk',
    ]


def test_worker_fails_instead_of_finishing_after_seafile_ai_error():
    class FailingSeafileAIAPI(FakeSeafileAIAPI):
        def chunk(self, payload, timeout):
            raise InternalAPIError(503, 'Model unavailable.')

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = FakeSeahubAPI()
    worker.seafile_ai_api = FailingSeafileAIAPI()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert [event_type for event_type, _payload in worker.seahub_api.events] == [
        'begin', 'failed',
    ]
    assert worker.seahub_api.events[-1][1]['error_code'] == 'seafile_ai_error'


def test_worker_retries_one_transient_chunk_failure():
    class TransientFailureSeafileAIAPI(FakeSeafileAIAPI):
        def chunk(self, payload, timeout):
            self.chunk_calls += 1
            if self.chunk_calls == 1:
                raise InternalAPIError(500, 'Temporary model timeout.')
            return [{
                'kind': 'replace_block_text',
                'block_id': 'block-%s' % payload['chunk_index'],
            }]

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = FakeSeahubAPI()
    worker.seafile_ai_api = TransientFailureSeafileAIAPI()
    worker.should_stop = type('StopEvent', (), {'wait': lambda self, delay: False})()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert worker.seafile_ai_api.chunk_calls == 3
    assert [event_type for event_type, _payload in worker.seahub_api.events] == [
        'begin', 'chunk', 'chunk', 'finish',
    ]


def test_worker_does_not_retry_non_transient_chunk_error():
    class InvalidResponseSeafileAIAPI(FakeSeafileAIAPI):
        def chunk(self, payload, timeout):
            self.chunk_calls += 1
            raise InternalAPIError(400, 'Invalid chunk request.')

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = FakeSeahubAPI()
    worker.seafile_ai_api = InvalidResponseSeafileAIAPI()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert worker.seafile_ai_api.chunk_calls == 1
    assert [event_type for event_type, _payload in worker.seahub_api.events] == [
        'begin', 'failed',
    ]


def test_worker_does_not_retry_invalid_model_response():
    class InvalidResponseSeafileAIAPI(FakeSeafileAIAPI):
        def chunk(self, payload, timeout):
            self.chunk_calls += 1
            raise InternalAPIError(
                502, '{"error_code":"invalid_model_response"}')

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = FakeSeahubAPI()
    worker.seafile_ai_api = InvalidResponseSeafileAIAPI()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert worker.seafile_ai_api.chunk_calls == 1
    assert [event_type for event_type, _payload in worker.seahub_api.events] == [
        'begin', 'failed',
    ]


def test_worker_fails_after_seafile_ai_conflict_instead_of_treating_it_as_stale():
    class FailingSeafileAIAPI(FakeSeafileAIAPI):
        def chunk(self, payload, timeout):
            raise InternalAPIError(409, 'Model request was rejected.')

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = FakeSeahubAPI()
    worker.seafile_ai_api = FailingSeafileAIAPI()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert [event_type for event_type, _payload in worker.seahub_api.events] == [
        'begin', 'failed',
    ]
    assert worker.seahub_api.events[-1][1]['error_code'] == 'seafile_ai_error'


def test_worker_stops_without_generating_when_lease_is_rejected():
    class CancelledSeahubAPI(FakeSeahubAPI):
        def heartbeat(self, task_id, attempt_id):
            raise InternalAPIError(409, 'Review task is no longer running.')

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = CancelledSeahubAPI()
    worker.seafile_ai_api = FakeSeafileAIAPI()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert worker.seafile_ai_api.chunk_calls == 0
    assert worker.seahub_api.events == []


def test_answer_then_review_fails_when_analysis_generation_fails():
    class AnalysisTaskSeahubAPI(FakeSeahubAPI):
        def claim(self, task_id, attempt_id):
            result = super(AnalysisTaskSeahubAPI, self).claim(task_id, attempt_id)
            result['task']['route'] = 'answer_then_review'
            return result

    class FailingAnalysisSeafileAIAPI(FakeSeafileAIAPI):
        def analyze(self, payload, timeout):
            raise InternalAPIError(503, 'Analysis unavailable.')

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = AnalysisTaskSeahubAPI()
    worker.seafile_ai_api = FailingAnalysisSeafileAIAPI()

    worker._process_task('00000000-0000-4000-8000-000000000001')

    assert [event_type for event_type, _payload in worker.seahub_api.events] == ['failed']
    assert worker.seahub_api.events[0][1]['error_code'] == 'analysis_failed'


def test_worker_retries_failure_event_delivery():
    class TransientFailureSeahubAPI(FakeSeahubAPI):
        def __init__(self):
            super(TransientFailureSeahubAPI, self).__init__()
            self.failure_calls = 0

        def event(self, task_id, attempt_id, event_type, **payload):
            if event_type == 'failed':
                self.failure_calls += 1
                if self.failure_calls == 1:
                    raise InternalAPIError(503, 'Seahub unavailable.')
            return super(TransientFailureSeahubAPI, self).event(
                task_id, attempt_id, event_type, **payload)

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = TransientFailureSeahubAPI()
    worker.should_stop = type('StopEvent', (), {'wait': lambda self, delay: False})()

    worker._fail('task-id', 'attempt-id', 'generation_failed')

    assert worker.seahub_api.failure_calls == 2
    assert worker.seahub_api.events == [
        ('failed', {'error_code': 'generation_failed'}),
    ]


def test_worker_stops_failure_delivery_after_stale_attempt_response():
    class StaleSeahubAPI(FakeSeahubAPI):
        def __init__(self):
            super(StaleSeahubAPI, self).__init__()
            self.failure_calls = 0

        def event(self, task_id, attempt_id, event_type, **payload):
            self.failure_calls += 1
            raise InternalAPIError(409, 'Review attempt is stale.')

    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = StaleSeahubAPI()

    worker._fail('task-id', 'attempt-id', 'generation_failed')

    assert worker.seahub_api.failure_calls == 1
