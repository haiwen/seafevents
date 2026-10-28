import jwt

from seafevents.sdoc_review.api import SeahubReviewAPI, SeafileAIReviewAPI


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

