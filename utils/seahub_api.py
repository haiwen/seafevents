import jwt
import json
import requests
import logging
from seafevents.app.config import JWT_PRIVATE_KEY, SEAHUB_SERVER_URL

logger = logging.getLogger(__name__)


def parse_response(response):
    if response.status_code == 400:
        logger.warning('seahub error: %s', response.text)
    if response.status_code > 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            return json.loads(response.text)
        except:
            pass


class SeahubAPI(object):

    def __init__(self, timeout=180):
        self.server = SEAHUB_SERVER_URL
        self.timeout = timeout
        self.gen_header()

    def gen_header(self):
        payload = {
            'is_internal': True
        }
        jwt_token = jwt.encode(payload, JWT_PRIVATE_KEY, algorithm='HS256')
        self.headers = {
            'Authorization': f'token {jwt_token}'
        }

    def get_download_rate_limit_info(self, traffic_info_list):
        url = self.server + '/api/v2.1/internal/download-limit/'
        response = requests.post(url, headers=self.headers, json=traffic_info_list, timeout=self.timeout)

        if response.status_code == 400:
            raise Exception('Failed get download rate limit, error: %s' % response.text)
        elif response.status_code > 400:
            raise ConnectionError(response.status_code, response.text)
        data = parse_response(response)
        return data
