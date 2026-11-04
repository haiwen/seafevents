import os
import json
import requests
import posixpath
import jwt
import time

from urllib.parse import quote

from seaserv import seafile_api
from seafevents.app.config import FILE_CONVERTER_SERVER_URL, SEADOC_PRIVATE_KEY,  \
    INNER_FILE_SERVER_ROOT, FILE_SERVER_ROOT
from seafevents.utils.constants import WIKI_CONFIG_PATH, WIKI_CONFIG_FILE_NAME


def gen_file_get_url(token, filename):
    return '%s/files/%s/%s' % (FILE_SERVER_ROOT, token, quote(filename))

def gen_inner_file_get_url(token, filename):
    return '%s/files/%s/%s' % (INNER_FILE_SERVER_ROOT, token,
                                quote(filename))

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
    url = gen_file_get_url(token, WIKI_CONFIG_FILE_NAME)
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


def convert_file_token(repo_id, file_id, filename, username, doc_uuid='', output_path=None):
    payload = {
        'aud': 'seadoc-converter-file', 'exp': int(time.time()) + 300,
        'repo_id': repo_id, 'file_id': file_id, 'filename': filename,
        'username': username, 'doc_uuid': str(doc_uuid),
    }
    if output_path:
        payload['output_path'] = output_path
    return jwt.encode(payload, SEADOC_PRIVATE_KEY, algorithm='HS256')


def convert_confluence_to_wiki(filename, file_token, username, seafile_server_url):
    params = {
        'filename': filename,
        'file_token': file_token,
        'username': username,
        'seafile_server_url': seafile_server_url
    }
    url = FILE_CONVERTER_SERVER_URL.rstrip('/') + '/api/v1/confluence-to-wiki/'
    headers = convert_file_gen_headers()
    resp = requests.post(url, json=params, headers=headers, timeout=30)
    return resp.content


def convert_file(path, username, doc_uuid, file_token, src_type, dst_type):
    params = {
        'path': path,
        'username': username,
        'doc_uuid': doc_uuid,
        'file_token': file_token,
        'src_type': src_type,
        'dst_type': dst_type,
    }
    url = FILE_CONVERTER_SERVER_URL.rstrip('/') + '/api/v1/file-convert/'
    headers = convert_file_gen_headers()
    resp = requests.post(url, json=params, headers=headers, timeout=30)
    return resp


def gen_new_page_nav_by_id(navigation, page_id, current_id, insert_position, is_find):
    new_nav = {
        'id': page_id,
        'type': 'page',
    }
    if current_id:
        if insert_position == 'inner':
            for nav in navigation:
                if nav.get('type') == 'page' and nav.get('id') == current_id:
                    sub_nav = nav.get('children', [])
                    sub_nav.append(new_nav)
                    nav['children'] = sub_nav
                    is_find[0] = True
                    return True
                else:
                    gen_new_page_nav_by_id(nav.get('children', []), page_id, current_id, insert_position, is_find)
        elif insert_position == 'above':
            for index, nav in enumerate(navigation):
                if nav.get('type') == 'page' and nav.get('id') == current_id:
                    navigation.insert(index, new_nav)
                    is_find[0] = True
                    return True
                else:
                    gen_new_page_nav_by_id(nav.get('children', []), page_id, current_id, insert_position, is_find)
        elif insert_position == 'below':
            for index, nav in enumerate(navigation):
                if nav.get('type') == 'page' and nav.get('id') == current_id:
                    navigation.insert(index+1, new_nav)
                    is_find[0] = True
                    return True
                else:
                    gen_new_page_nav_by_id(nav.get('children', []), page_id, current_id, insert_position, is_find)
    else:
        navigation.append(new_nav)
        is_find[0] = True
        return True


def delete_wiki_page_dir(repo_id, parent_dir, username):
    del_parent_dir = os.path.dirname(parent_dir)
    del_file_name = os.path.basename(parent_dir)
    seafile_api.del_file(repo_id, del_parent_dir,
                json.dumps([del_file_name]), username)
