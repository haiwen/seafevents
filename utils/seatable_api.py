import requests


# 定义列类型（从表格部分抄过来）
class ColumnTypes:
    COLLABORATOR = 'collaborator'
    NUMBER = 'number'
    DATE = 'date'
    GEOLOCATION = 'geolocation'
    CREATOR = 'creator'
    LAST_MODIFIER = 'last-modifier'
    TEXT = 'text'
    IMAGE = 'image'
    LONG_TEXT = 'long-text'
    CHECKBOX = 'checkbox'
    SINGLE_SELECT = 'single-select'
    MULTIPLE_SELECT = 'multiple-select'
    URL = 'url'
    DURATION = 'duration'
    FILE = 'file'
    EMAIL = 'email'
    RATE = 'rate'
    FORMULA = 'formula'
    LINK_FORMULA = 'link-formula'
    AUTO_NUMBER = 'auto-number'
    LINK = 'link'
    CTIME = 'ctime'
    MTIME = 'mtime'
    BUTTON = 'button'
    DIGITAL_SIGN = 'digital-sign'


# 转换响应(>=400都是连接错误），200 304 response.json（）返回查询后的信息
def parse_response(response):
    if response.status_code >= 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            return response.json()
        except:
            pass


class SeaTableAPI:

    # SeaTable API 的一个封装（三个 URL，分别是 server_url, dtable_server_url, dtab;e_db_url）
    def __init__(self, api_token, server_url):
        self.api_token = api_token
        self.server_url = server_url
        self.dtable_uuid = None
        self.access_token = None
        self.dtable_server_url = None
        self.dtable_db_url = None
        self.headers = None
        # 调用 auth 方法进行身份验证
        self.auth()

    # 使用提供的 API 令牌进行身份验证，并获取访问令牌、dtable UUID 和服务器 URL。
    def auth(self):
        # 拼接 URL 从 dtable-server 获取 AccessToken
        url = f"{self.server_url.strip('/')}/api/v2.1/dtable/app-access-token/?from=dtable_web"
        resp = requests.get(url, headers={'Authorization': f'Token {self.api_token}'})
        # 下面 resp.json() 可以优化一次，然后获取不同服务器的 URL
        self.dtable_uuid = resp.json()['dtable_uuid']
        self.access_token = resp.json()['access_token']
        self.dtable_server_url = resp.json()['dtable_server']
        self.dtable_db_url = resp.json()['dtable_db']
        self.headers = {'Authorization': f'Token {self.access_token}'}

    # 获取 dtable-server 的元数据
    def get_metadata(self):
        url = f"{self.dtable_server_url.strip('/')}/api/v1/dtables/{self.dtable_uuid}/metadata/?from=dtable_web"
        resp = requests.get(url, headers=self.headers)
        return parse_response(resp)['metadata']

    # 在 dtable-db 上执行 SQL 查询并返回结果。
    def query(self, sql, convert=None, server_only=None):
        # 1. url 是 dtable-db 的 API  URL，用于执行 SQL 查询
        url = f"{self.dtable_db_url.strip('/')}/api/v1/query/{self.dtable_uuid}/?from=dtable_web"
        # 2. data 是一个字典，包含了 SQL 语句和可选的 convert 和 server_only 两个参数
        data = {'sql': sql}
        # 3. convert 是一个可选参数，用于将查询结果转换为特定的数据类型
        if convert is not None:
            data['convert_keys'] = convert
        # 4. server_only 是一个可选参数，用于指定是否只是在服务器端执行 SQL 语句
        if server_only is not None:
            data['server_only'] = server_only
        resp = requests.post(url, json=data, headers=self.headers)
        return parse_response(resp)

    # dtable_server_url 创建一个新表格，指定名称，列存在就增加列。
    def add_table(self, table_name, columns=None):
        url = f"{self.dtable_server_url.strip('/')}/api/v1/dtables/{self.dtable_uuid}/tables/?from=dtable_web"
        data = {'table_name': table_name}
        if columns:
            data['columns'] = columns
        resp = requests.post(url, headers=self.headers, json=data)
        return parse_response(resp)

    # dtable_server_url 添加一个新列
    def insert_column(self, table_name, column):
        url = f"{self.dtable_server_url.strip('/')}/api/v1/dtables/{self.dtable_uuid}/columns/?from=dtable_web"
        data = {'table_name': table_name}
        data.update(column)
        resp = requests.post(url, headers=self.headers, json=data)
        return parse_response(resp)

    # 向 dtable 添加一行
    def append_row(self, table_name, row):
        url = f"{self.dtable_server_url.strip('/')}/api/v1/dtables/{self.dtable_uuid}/rows/?from=dtable_web"
        data = {
            'table_name': table_name,
            'row': row
        }
        resp = requests.post(url, headers=self.headers, json=data)
        return parse_response(resp)

    # 更新 dtable 中的现有表格中的某一行数据
    def update_row(self, table_name, row_id, row):
        url = f"{self.dtable_server_url.strip('/')}/api/v1/dtables/{self.dtable_uuid}/rows/?from=dtable_web"
        data = {
            'table_name': table_name,
            'row': row,
            "row_id": row_id
        }
        resp = requests.put(url, headers=self.headers, json=data)
        return parse_response(resp)

    # 根据名称获取 dtable 中某个表格
    def get_table_by_name(self, table_name):
        metadata = self.get_metadata()
        for table in metadata['tables']:
            if table['name'] == table_name:
                return table
        return None
