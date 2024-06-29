import requests, jwt, time

from seafevents.app.config import METADATA_SERVER_SECRET_KEY, METADATA_SERVER_URL


class StructureTable(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name


class StructureColumn(object):
    def __init__(self, key, name, type):
        self.key = key
        self.name = name
        self.type = type

    def to_build_column_dict(self):
        return {
            'key': self.key,
            'name': self.name,
            'type': self.type
        }

#metadata base
METADATA_TABLE = StructureTable('0001', 'Table1')
METADATA_COLUMN_ID = StructureColumn('_id', '_id', 'text')
METADATA_COLUMN_CREATOR = StructureColumn('_file_creator', '_file_creator', 'text')
METADATA_COLUMN_CREATED_TIME = StructureColumn('_file_ctime', '_file_ctime', 'date')
METADATA_COLUMN_MODIFIER = StructureColumn('_file_modifier', '_file_modifier', 'text')
METADATA_COLUMN_MODIFIED_TIME = StructureColumn('_file_mtime', '_file_mtime', 'date')
METADATA_COLUMN_PARENT_DIR = StructureColumn('_parent_dir', '_parent_dir', 'text')
METADATA_COLUMN_NAME = StructureColumn('_name', '_name', 'text')
METADATA_COLUMN_IS_DIR = StructureColumn('_is_dir', '_is_dir', 'text')


def parse_response(response):
    if response.status_code >= 400 or response.status_code < 200:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            return response.json()
        except:
            pass


class MetadataServerAPI:
    def __init__(self, user, timeout=30):
        self.user = user
        self.timeout = timeout
        self.secret_key = METADATA_SERVER_SECRET_KEY
        self.server_url = METADATA_SERVER_URL

    def gen_headers(self, base_id):
        payload = {
            'exp': int(time.time()) + 3600,
            'base_id': base_id,
            'user': self.user
        }
        token = jwt.encode(payload, self.secret_key, algorithm='HS256')
        return {"Authorization": "Bearer %s" % token}

    def create_base(self, base_id):
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/base/{base_id}'
        response = requests.post(url, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def delete_base(self, base_id):
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/base/{base_id}'
        response = requests.delete(url, headers=headers, timeout=self.timeout)

        if response.status_code == 404:
            return {'success': True}
        return parse_response(response)

    def add_column(self, base_id, table_id, column):
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/base/{base_id}/columns'
        data = {
            'table_id': table_id,
            'column': column
        }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)
    
    def insert_rows(self, base_id, table_id, rows):
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/base/{base_id}/rows'
        data = {
                'table_id': table_id,
                'rows': rows
            }
        response = requests.post(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)
    
    def update_rows(self, base_id, table_id, rows):
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/base/{base_id}/rows'
        data = {
                'table_id': table_id,
                'rows': rows
            }
        response = requests.put(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def delete_rows(self, base_id, table_id, row_ids):
        headers = self.gen_headers(base_id)
        url = f'{self.server_url}/api/v1/base/{base_id}/rows'
        data = {
                'table_id': table_id,
                'row_ids': row_ids
            }
        response = requests.delete(url, json=data, headers=headers, timeout=self.timeout)
        return parse_response(response)

    def query_rows(self, base_id, sql, params=[]):
        headers = self.gen_headers(base_id)
        post_data = {
            'sql': sql
        }

        if params:
            post_data['params'] = params
        url = f'{self.server_url}/api/v1/base/{base_id}/query'
        response = requests.post(url, json=post_data, headers=headers, timeout=self.timeout)
        return parse_response(response)
