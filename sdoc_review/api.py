import time
from urllib.parse import urljoin

import jwt
import requests


class InternalAPIError(Exception):
    def __init__(self, status_code, message):
        super().__init__(message)
        self.status_code = status_code


class _JSONAPI(object):
    def __init__(self, base_url):
        self.base_url = (base_url or '').rstrip('/') + '/'

    def _post(self, path, payload, headers, timeout=30):
        response = requests.post(
            urljoin(self.base_url, path.lstrip('/')),
            json=payload,
            headers=headers,
            timeout=timeout,
        )
        if not response.ok:
            raise InternalAPIError(response.status_code, response.text)
        try:
            return response.json()
        except ValueError:
            raise InternalAPIError(response.status_code, 'Invalid JSON response')


class SeahubReviewAPI(_JSONAPI):
    def __init__(self, base_url, private_key):
        super().__init__(base_url)
        self.private_key = private_key

    def _headers(self):
        payload = {
            'exp': int(time.time()) + 300,
            'is_internal': True,
            'purpose': 'sdoc_review_worker',
            'audience': 'seahub_sdoc_review',
        }
        token = jwt.encode(payload, self.private_key, algorithm='HS256')
        return {'Authorization': 'Token %s' % token}

    def pending(self):
        return self._post(
            '/api/v2.1/ai/internal/sdoc-reviews/pending/', {}, self._headers(), timeout=15)

    def claim(self, task_id, attempt_id):
        return self._post(
            '/api/v2.1/ai/internal/sdoc-reviews/%s/claim/' % task_id,
            {'attempt_id': str(attempt_id)}, self._headers(), timeout=30)

    def event(self, task_id, attempt_id, event_type, **payload):
        body = {'attempt_id': str(attempt_id), 'event_type': event_type}
        body.update(payload)
        return self._post(
            '/api/v2.1/ai/internal/sdoc-reviews/%s/events/' % task_id,
            body, self._headers(), timeout=30)

    def reconcile_apply(self, apply_attempt_id):
        return self._post(
            '/api/v2.1/ai/internal/sdoc-review-applies/%s/reconcile/' % apply_attempt_id,
            {}, self._headers(), timeout=10)


class SeafileAIReviewAPI(_JSONAPI):
    def __init__(self, base_url, secret_key):
        super().__init__(base_url)
        self.secret_key = secret_key

    def _headers(self):
        token = jwt.encode(
            {'exp': int(time.time()) + 300}, self.secret_key, algorithm='HS256')
        return {'Authorization': 'Token %s' % token}

    def analyze(self, payload, timeout):
        result = self._post(
            '/api/v1/sdoc-analyze/', payload, self._headers(), timeout=timeout)
        return result.get('analysis')

    def plan(self, payload, timeout):
        result = self._post(
            '/api/v1/sdoc-review-plan/', payload, self._headers(), timeout=timeout)
        return result.get('plan')

    def chunk(self, payload, timeout):
        result = self._post(
            '/api/v1/sdoc-review-chunk/', payload, self._headers(), timeout=timeout)
        return result.get('items')
