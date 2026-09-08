import logging
import os
import time

import jwt
import requests


logger = logging.getLogger('ai_summary')


def parse_response(response):
    if response.status_code >= 400 or response.status_code < 200:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            return response.json()
        except:
            pass


class SeafileAIAPI:
    def __init__(self, server_url, secret_key, timeout=90):
        self.timeout = timeout
        self.secret_key = secret_key
        self.server_url = server_url

    def gen_headers(self):
        payload = {'exp': int(time.time()) + 300, }
        token = jwt.encode(payload, self.secret_key, algorithm='HS256')
        return {"Authorization": "Token %s" % token}

    def face_embeddings(self, path, download_token, need_face=False):
        headers = self.gen_headers()
        url = f'{self.server_url}/api/v1/face-embeddings'
        data = {
            'path': path,
            'download_token': download_token,
            'need_face': need_face
        }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)
    
    def face_cluster(self, repo_id):
        headers = self.gen_headers()
        url = f'{self.server_url}/api/v1/face-cluster'
        data = {
            'repo_id': repo_id,
        }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)
    
    def face_batch_embeddings(self, repo_id, obj_ids, need_classify=False):
        headers = self.gen_headers()
        url = f'{self.server_url}/api/v1/face-batch-embeddings'
        data = {
            'repo_id': repo_id,
            'obj_ids': obj_ids,
            'need_classify': need_classify
        }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)
    
    def update_people_cover_photo(self, repo_id, people_id, obj_id):
        headers = self.gen_headers()
        url = f'{self.server_url}/api/v1/update-people-cover-photo'
        data = {
            'repo_id': repo_id,
            'people_id': people_id,
            'obj_id': obj_id,
        }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)
    
    def recognize_faces(self, repo_id, obj_ids):
        headers = self.gen_headers()
        url = f'{self.server_url}/api/v1/recognize-faces'
        data = {
            'repo_id': repo_id,
            'obj_ids': obj_ids,
        }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def generate_ai_summary(self, repo_id, obj_id, path, username='system', org_id=None):
        headers = self.gen_headers()
        url = f'{self.server_url}/api/v1/generate-summary'
        data = {
            'repo_id': repo_id,
            'obj_id': obj_id,
            'path': path,
            'username': username,
            'org_id': org_id,
        }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response).get('summary', '')

    def batch_generate_embeddings(self, contents, username='system', org_id=None):
        headers = self.gen_headers()
        url = f'{self.server_url}/api/v1/embeddings/batch'
        data = {
            'contents': contents,
            'username': username,
            'org_id': org_id,
            'scenario': 'summary_index',
        }
        proxy_variables = ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY')
        logger.info(
            'Sending embedding batch request: url=%s, content_count=%d, total_characters=%d, '
            'environment_proxy_configured=%s, no_proxy_configured=%s',
            url, len(contents), sum(len(content) for content in contents),
            any(os.environ.get(name) for name in proxy_variables), bool(os.environ.get('NO_PROXY')),
        )
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        logger.info(
            'Received embedding batch response: url=%s, status_code=%d, server=%s, via=%s',
            url, response.status_code, response.headers.get('Server', ''), response.headers.get('Via', ''),
        )
        result = parse_response(response)
        embeddings = result.get('embeddings')
        if not isinstance(embeddings, list) or len(embeddings) != len(contents):
            raise ValueError('Embedding response count mismatch')
        return result.get('model'), embeddings
