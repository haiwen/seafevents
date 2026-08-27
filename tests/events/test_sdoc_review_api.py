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
        self.events.append((event_type, payload))
        return {'accepted': True, 'limit_reached': False}


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
        assert payload['request_timeout_seconds'] <= 28
        assert payload['repo_id'] == 'repo-id'
        return {
            'brief': None,
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


def test_worker_reconciles_apply_outside_generation_thread():
    worker = SdocReviewWorker.__new__(SdocReviewWorker)
    worker.seahub_api = FakeApplySeahubAPI()
    worker.should_stop = type('StopEvent', (), {'wait': lambda self, delay: False})()

    worker._process_apply_attempt('00000000-0000-4000-8000-000000000002')

    assert worker.seahub_api.calls == 2


def test_worker_stops_after_seahub_rejects_cancelled_task():
    class CancelledSeahubAPI(FakeSeahubAPI):
        def event(self, task_id, attempt_id, event_type, **payload):
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
