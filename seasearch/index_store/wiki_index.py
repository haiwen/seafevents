import logging
import posixpath
import json

from seafevents.seasearch.utils import get_library_diff_files, is_in_wiki_dirs, extract_sdoc_text, md5
from seafevents.seasearch.utils.constants import ZERO_OBJ_ID, WIKI_INDEX_PREFIX
from seafobj import fs_mgr, commit_mgr
from seaserv import seafile_api


logger = logging.getLogger(__name__)


SEASEARCH_WIKI_BULK_OPETATE_LIMIT = 25
WIKI_PAGES_DIR = '/wiki-pages'
WIKI_CONFIG_PATH = '_Internal/Wiki'
WIKI_CONFIG_FILE_NAME = 'index.json'


class WikiIndex(object):
    mapping = {
        'properties': {
            'wiki_id': {
                'type': 'keyword',
            },
            'doc_uuid':{
                'type': 'keyword',
            },
            'content': {
                'type': 'text',
                'highlightable': True,
                'fields': {
                    'ngram': {
                        'type': 'text',
                        'index': True,
                        'analyzer': 'seafile_wiki_ngram_analyzer',
                    },
                },
            },
        }
    }

    index_settings = {
        'analysis': {
            'analyzer': {
                'seafile_wiki_ngram_analyzer': {
                    'type': 'custom',
                    'tokenizer': 'seafile_wiki_ngram_tokenizer',
                    'filter': [
                        'lowercase',
                    ],
                }
            },
            'tokenizer': {
                'seafile_wiki_ngram_tokenizer': {
                    'type': 'ngram',
                    'min_gram': 4,
                    'max_gram': 4,
                    'token_chars': [
                        'letter',
                        'digit'
                    ]
                }
            }
        }
    }

    def __init__(self, seasearch_api, repo_data, shard_num):
        self.seasearch_api = seasearch_api
        self.repo_data = repo_data
        self.shard_num = shard_num

    def _make_query_searches(self, keyword):
        match_query_kwargs = {'minimum_should_match': '-25%'}

        def _make_match_query(field, key_word, **kw):
            q = {'query': key_word}
            q.update(kw)
            return {'match': {field: q}}

        searches = []
        searches.append(_make_match_query('content', keyword, **match_query_kwargs))
        searches.append({
            'match': {
                'content.ngram': {
                    'query': keyword,
                    'minimum_should_match': '80%',
                }
            }
        })
        return searches

    def create_index_if_missing(self, index_name):
        if not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            data = {
                'shard_num': self.shard_num,
                'mappings': self.mapping,
                'settings': self.index_settings
            }
            self.seasearch_api.create_index(index_name, data)

    def check_index(self, index_name):
        return self.seasearch_api.check_index_mapping(index_name).get('is_exist')

    def get_wiki_content(self, wiki_id, obj_id):
        if obj_id == ZERO_OBJ_ID:
            return None
        f = fs_mgr.load_seafile(wiki_id, 1, obj_id)
        b_content = f.get_content()
        if not b_content.strip():
            return None
        content = extract_sdoc_text(b_content)

        return content.strip()

    def get_wiki_conf(self, wiki_id, commit_id=None):
        # Get wiki config dict
        conf_path = posixpath.join(WIKI_CONFIG_PATH, WIKI_CONFIG_FILE_NAME)
        if commit_id is not None:
            file_id = seafile_api.get_file_id_by_commit_and_path(wiki_id, commit_id, conf_path)
        else:
            file_id = seafile_api.get_file_id_by_path(wiki_id, conf_path)

        f = fs_mgr.load_seafile(wiki_id, 1, file_id)
        return json.loads(f.get_content().decode())

    def extract_doc_uuids(self, config):
        """Determine the UUID for extracting unremoved or deleted wiki pages
        """
        def extract_ids_from_navigation(navigation_items, navigation_ids):
            for item in navigation_items:
                navigation_ids.add(item['id'])
                if 'children' in item and item['children']:
                    extract_ids_from_navigation(item['children'], navigation_ids)

        navigation_ids = set()
        extract_ids_from_navigation(config['navigation'], navigation_ids)

        doc_uuids, deleted_doc_uuids = [], []
        for page in config['pages']:
            if page['id'] in navigation_ids:
                doc_uuids.append(page['docUuid'])
            else:
                deleted_doc_uuids.append(page['docUuid'])

        return doc_uuids, deleted_doc_uuids

    def add_files(self, index_name, wiki_id, files, doc_uuids):
        bulk_add_params = []

        for path, obj_id, mtime, size in files:
            if not is_in_wiki_dirs(path):
                continue

            doc_uuid = path.split('/')[2]
            if doc_uuid not in doc_uuids:
                continue
            doc_info = {
                'wiki_id': wiki_id,
                'doc_uuid': doc_uuid,
            }

            if content := self.get_wiki_content(wiki_id, obj_id):
                doc_info.update(content=content)
            else:
                continue

            index_info = {'index': {'_index': index_name, '_id': md5(doc_uuid)}}
            bulk_add_params.append(index_info)
            bulk_add_params.append(doc_info)

            if len(bulk_add_params) >= SEASEARCH_WIKI_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, bulk_add_params)
                bulk_add_params = []
        if bulk_add_params:
            self.seasearch_api.bulk(index_name, bulk_add_params)

    def delete_files(self, index_name, files, deleted_doc_uuids):
        delete_params = []

        def bulk_delete():
            if delete_params:
                self.seasearch_api.bulk(index_name, delete_params)
                delete_params.clear()

        for file in files:
            path = file[0]
            if not is_in_wiki_dirs(path):
                continue
            doc_uuid = path.split('/')[2]
            delete_params.append({'delete': {'_id': md5(doc_uuid), '_index': index_name}})
            if len(delete_params) >= SEASEARCH_WIKI_BULK_OPETATE_LIMIT:
                bulk_delete()

        bulk_delete()

        for doc_uuid in deleted_doc_uuids:
            delete_params.append({'delete': {'_id': md5(doc_uuid), '_index': index_name}})
            if len(delete_params) >= SEASEARCH_WIKI_BULK_OPETATE_LIMIT:
                bulk_delete()

        bulk_delete()

    def update(self, index_name, wiki_id, old_commit_id, new_commit_id):
        added_files, deleted_files, modified_files, _ , _ = \
            get_library_diff_files(wiki_id, old_commit_id, new_commit_id)

        if not (added_files or deleted_files or modified_files):
            return
        conf = self.get_wiki_conf(wiki_id, new_commit_id)
        doc_uuids, deleted_doc_uuids = self.extract_doc_uuids(conf)

        need_deleted_files = deleted_files
        self.delete_files(index_name, need_deleted_files, deleted_doc_uuids)

        need_added_files = added_files + modified_files

        self.add_files(index_name, wiki_id, need_added_files, doc_uuids)

    def search_wiki(self, wiki, keyword, start=0, size=10):
        bulk_search_params = []

        title_info = []

        query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
        searches = self._make_query_searches(keyword)
        query_map['bool']['should'] = searches

        data = {
            'query': query_map,
            'from': start,
            'size': size,
            '_source': ['wiki_id', 'doc_uuid'],
            'sort': ['_score'],
            "highlight": {
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
                "fields": {"content": {}},
            },    
        }
        index_name = WIKI_INDEX_PREFIX + wiki
        index_info = {"index": index_name}
        bulk_search_params.append(index_info)
        bulk_search_params.append(data)

        # Get wiki title
        conf = self.get_wiki_conf(wiki)
        doc_uuids, _ = self.extract_doc_uuids(conf) 
        for page in conf['pages']:
            page_uuid = page['path'].split('/')[2]
            if page_uuid in doc_uuids:
                title_info.append((page_uuid, page["name"], wiki))

        results = self.seasearch_api.m_search(bulk_search_params)
        wiki_content = []
        for result in results.get('responses'):
            hits = result.get('hits', {}).get('hits', [])

            if not hits:
                continue

            for hit in hits:
                source = hit.get('_source')
                score = hit.get('_score')
                _id = hit.get('_id')

                r = {
                    'doc_uuid': source['doc_uuid'],
                    'wiki_id': source['wiki_id'],
                    'score': score,
                    '_id': _id,
                    'hit_type': 'wiki_content'
                }
                if highlight_content := hit.get('highlight', {}).get('content', [None])[0]:
                    r.update(content=highlight_content)
                wiki_content.append(r)

        # Search in wiki title
        title_match = []
        for doc_uuid, title, wiki_id in title_info:
            if keyword in title:
                r_t = {
                    'doc_uuid': doc_uuid,
                    'wiki_id': wiki_id,
                    'title': title,
                    'hit_type': 'wiki_title'
                }
                title_match.append(r_t)

        return title_match + wiki_content

    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)
