import logging
import requests
import jwt
import time
import json


logger = logging.getLogger(__name__)


def parse_response(response):
    if response.status_code >= 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass


class SeaEmbeddingAPI(object):

    def __init__(self, sea_embedding_url, sea_embedding_key, time_out=180):
        self.sea_embedding_url = sea_embedding_url.rstrip('/')
        self.time_out = time_out
        self.sea_embedding_key = sea_embedding_key

    def gen_headers(self):
        payload = {'exp': int(time.time()) + 300, }
        token = jwt.encode(payload, self.sea_embedding_key, algorithm='HS256')
        return {"Authorization": "Token %s" % token}

    def embeddings(self, input):
        url = self.sea_embedding_url + '/api/v1/embeddings/'
        params = {
            'input': input,
        }
        headers = self.gen_headers()

        response = requests.post(url, headers=headers, json=params, timeout=self.time_out)
        data = parse_response(response)
        return data
