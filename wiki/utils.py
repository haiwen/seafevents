import json
import requests
import posixpath
import jwt
import time

from urllib.parse import quote

from seaserv import seafile_api
from seafevents.app.config import FILE_CONVERTER_SERVER_URL, SEADOC_PRIVATE_KEY, ENABLE_INNER_FILESERVER, \
    INNER_FILE_SERVER_ROOT, FILE_SERVER_ROOT
from seafevents.utils.constants import WIKI_CONFIG_PATH, WIKI_CONFIG_FILE_NAME


def gen_file_get_url(token, filename):
    """
    Generate fileserver file url.
    Format: http://<domain:port>/files/<token>/<filename>
    """
    return '%s/files/%s/%s' % (FILE_SERVER_ROOT, token, quote(filename))

def gen_file_upload_url(token, op, replace=False):
    url = '%s/%s/%s' % (FILE_SERVER_ROOT, op, token)
    if replace is True:
        url += '?replace=1'
    return url


def get_wiki_config(repo_id, username):
    config_path = posixpath.join(WIKI_CONFIG_PATH, WIKI_CONFIG_FILE_NAME)
    file_id = seafile_api.get_file_id_by_path(repo_id, config_path)
    if not file_id:
        return {}
    token = seafile_api.get_fileserver_access_token(repo_id, file_id, 'download', username, use_onetime=True)
    url = gen_inner_file_get_url(token, WIKI_CONFIG_FILE_NAME)
    resp = requests.get(url)
    wiki_config = json.loads(resp.content)
    return wiki_config
    
    
def get_all_wiki_ids(navigation):
    id_set = set()

    def recurse_item(item):
        id_set.add(item.get('id'))
        children = item.get('children')
        if children:
            for child in children:
                recurse_item(child)

    for nav in navigation:
        recurse_item(nav)
    return id_set


def convert_file_gen_headers():
    payload = {'exp': int(time.time()) + 300, }
    token = jwt.encode(payload, SEADOC_PRIVATE_KEY, algorithm='HS256')
    return {"Authorization": "Token %s" % token}


def convert_confluence_to_wiki(filename, download_url, upload_url, username, seafile_server_url):
    headers = convert_file_gen_headers()
    params = {
        'filename': filename,
        'download_url': download_url,
        'upload_url': upload_url,
        'username': username,
        'seafile_server_url': seafile_server_url
    }
    url = FILE_CONVERTER_SERVER_URL.rstrip('/') + '/api/v1/confluence-to-wiki/'
    resp = requests.post(url, json=params, headers=headers, timeout=30)
    return resp.content


def gen_inner_file_get_url(token, filename):
    """Generate inner fileserver file url.

    If ``ENABLE_INNER_FILESERVER`` set to False(defaults to True), will
    returns outer fileserver file url.

    Arguments:
    - `token`:
    - `filename`:

    Returns:
    	e.g., http://127.0.0.1:<port>/files/<token>/<filename>
    """
    if ENABLE_INNER_FILESERVER:
        return '%s/files/%s/%s' % (INNER_FILE_SERVER_ROOT, token,
                                   quote(filename))
    else:
        return gen_file_get_url(token, filename)
