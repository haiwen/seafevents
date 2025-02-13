import json
import logging
import requests

logger = logging.getLogger(__name__)


def parse_response(response):
    if response.status_code == 400:
        logger.warning('seasearch error: %s', response.text)
    if response.status_code > 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            return json.loads(response.text)
        except:
            pass


class Encoder(json.JSONEncoder):
    def encode(self, obj, *args, **kwargs):
        lines = []
        for each in obj:
            line = super(Encoder, self).encode(each, *args, **kwargs)
            lines.append(line)
        return '\n'.join(lines)


def ndjson_dumps(*args, **kwargs):
    """
    dumps data to ndjson format as seasearch parameter
    """
    kwargs.setdefault('cls', Encoder)
    return json.dumps(*args, **kwargs)


class SeaSearchAPI(object):

    def __init__(self, server, token, timeout=180):
        self.token = token
        self.server = server
        self.timeout = timeout
        self.gen_header()

    # 生成 API 请求的头部。
    def gen_header(self):
        self.headers = {
            'Authorization': 'Basic ' + self.token
        }

    # 创建一个新的索引。
    def create_index(self, index_name, data):
        url = self.server + '/api/index/' + index_name
        response = requests.put(url, headers=self.headers, json=data, timeout=self.timeout)
        if response.status_code == 400:
            raise Exception('create index: %s, error: %s' % (index_name, response.text))
        data = parse_response(response)

        return data

    # 创建一个新的文档，并指定其 ID。
    def create_document_by_id(self, index_name, doc_id, data):
        url = self.server + '/api/' + index_name + '/_doc/' + doc_id
        response = requests.put(url, headers=self.headers, json=data, timeout=self.timeout)
        if response.status_code == 400:
            raise Exception('index: %s, add document: %s, error: %s' % (index_name, doc_id, response.text))
        data = parse_response(response)

        return data

    # 对索引执行批量操作。
    def bulk(self, index_name, data):
        """
        this option includes add, update and delete index or document
        """
        url = self.server + '/es/' + index_name + '/_bulk'
        data = ndjson_dumps(data)
        response = requests.post(url, headers=self.headers, data=data, timeout=self.timeout)
        data = parse_response(response)
        error = data.get('error')
        if error:
            raise Exception(error)
        return data

    # 在索引中执行向量搜索。
    def vector_search(self, index_name, data):
        url = self.server + '/api/' + index_name + '/_search/vector'
        response = requests.post(url, headers=self.headers, json=data, timeout=self.timeout)

        return parse_response(response)

    # 在索引中执行普通搜索。
    def normal_search(self, index_name, data):
        url = self.server + '/es/' + index_name + '/_search'
        response = requests.post(url, headers=self.headers, json=data, timeout=self.timeout)

        return parse_response(response)

    # 在索引中执行多搜索。
    def m_search(self, data, unify_score=True):
        url = self.server + '/es/_msearch'
        if unify_score:
            url += '?unify_score=true'
        data = ndjson_dumps(data)
        response = requests.post(url, headers=self.headers, data=data, timeout=self.timeout)
        return parse_response(response)

    # 联合搜索
    def unified_search(self, data):
        url = self.server + '/api/unified_search'
        response = requests.post(url, headers=self.headers, data=data, timeout=self.timeout)
        return parse_response(response)

    # 检查索引是否存在。
    def check_index_mapping(self, index_name):
        url = self.server + '/es/' + index_name + '/_mapping'
        response = requests.get(url, headers=self.headers, timeout=self.timeout)
        if response.status_code == 400:
            return {'is_exist': False}
        elif response.status_code > 400:
            raise ConnectionError(response.status_code, response.text)

        return {'is_exist': True}

    # 检查文档是否存在。
    def check_document_by_id(self, index_name, doc_id):
        url = self.server + '/api/' + index_name + '/_doc/' + doc_id
        response = requests.get(url, headers=self.headers, timeout=self.timeout)
        if response.status_code == 400:
            return {'is_exist': False}
        elif response.status_code > 400:
            raise ConnectionError(response.status_code, response.text)

        return {'is_exist': True}

    # 根据 ID 检索文档。
    def get_document_by_id(self, index_name, doc_id):
        url = self.server + '/api/' + index_name + '/_doc/' + doc_id
        response = requests.get(url, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def delete_document_by_id(self, index_name, doc_id):
        url = self.server + '/api/' + index_name + '/_doc/' + doc_id
        response = requests.delete(url, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        error = data.get('error')
        if error:
            logger.warning('delete_document_by_id error: %s', error)
        return data

    # 删除索引。
    def delete_index_by_name(self, index_name):
        url = self.server + '/api/index/' + index_name
        response = requests.delete(url, headers=self.headers, timeout=self.timeout)
        if response.status_code == 400:
            logger.warning('index: %s not exist error: %s' % (index_name, response.text))
        elif response.status_code > 400:
            raise ConnectionError(response.status_code, response.text)
        return json.loads(response.text)

    # 更新文档。
    def update_document_by_id(self, index_name, doc_id, data):
        url = self.server + '/api/' + index_name + '/_doc/' + doc_id
        response = requests.put(url, headers=self.headers, json=data, timeout=self.timeout)
        data = parse_response(response)
        error = data.get('error')
        if error:
            raise Exception(error)
        return data
