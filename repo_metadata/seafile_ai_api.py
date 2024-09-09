import requests, jwt, time

from seafevents.app.config import SEAFILE_AI_SERVER_URL, SEAFILE_AI_SECRET_KEY


def parse_response(response):
    if response.status_code >= 400 or response.status_code < 200:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            return response.json()
        except:
            pass


class SeafileAIAPI:
    def __init__(self, timeout=30):
        self.timeout = timeout
        self.secret_key = SEAFILE_AI_SECRET_KEY
        self.server_url = SEAFILE_AI_SERVER_URL

    def gen_headers(self):
        payload = {'exp': int(time.time()) + 300, }
        token = jwt.encode(payload, self.secret_key, algorithm='HS256')
        return {"Authorization": "Token %s" % token}

    def images_embedding(self, repo_id, obj_ids):
        headers = self.gen_headers()
        url = f'{self.server_url}/api/v1/images-embedding/'
        data = {
            'repo_id': repo_id,
            'obj_ids': obj_ids,
        }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)
