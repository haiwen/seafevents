import logging
import posixpath
import json

from seafevents.seasearch.utils import get_library_diff_files, is_in_wiki_dirs, extract_sdoc_text, md5
from seafevents.seasearch.utils.constants import ZERO_OBJ_ID, WIKI_INDEX_PREFIX
from seafobj import fs_mgr, commit_mgr
from seaserv import seafile_api


logger = logging.getLogger(__name__)


SEASEARCH_WIKI_BULK_ADD_LIMIT = 10
SEASEARCH_WIKI_BULK_DELETE_LIMIT = 50
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

    def __init__(self, seasearch_api, repo_data, shard_num, **kwargs):
        self.seasearch_api = seasearch_api
        self.repo_data = repo_data
        self.shard_num = shard_num
        if size_cap := kwargs.get('wiki_file_size_limit'):
            self.size_cap = size_cap 

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
        if commit_id == ZERO_OBJ_ID:
            return None

        if commit_id is not None:
            file_id = seafile_api.get_file_id_by_commit_and_path(wiki_id, commit_id, conf_path)
        else:
            file_id = seafile_api.get_file_id_by_path(wiki_id, conf_path)
        if file_id is None:
            return None
        f = fs_mgr.load_seafile(wiki_id, 1, file_id)
        return json.loads(f.get_content().decode())

    def get_uuid_path_mapping(self, config):
        """Determine the UUID-PATH mapping for extracting unremoved or deleted wiki pages
        """
        def extract_ids_from_navigation(navigation_items, navigation_ids):
            for item in navigation_items:
                navigation_ids.add(item['id'])
                if 'children' in item and item['children']:
                    extract_ids_from_navigation(item['children'], navigation_ids)
        if config is None:
            return {}, {}
        navigation_ids = set()
        extract_ids_from_navigation(config['navigation'], navigation_ids)

        uuid_to_path, rm_uuid_to_path = {}, {}
        for page in config['pages']:
            if page['id'] in navigation_ids:
                uuid_to_path[page['docUuid']] = page['path']
            else:
                rm_uuid_to_path[page['docUuid']] = page['path']

        return uuid_to_path, rm_uuid_to_path

    def add_files(self, index_name, wiki_id, files, uuid_path, commit_id):
        bulk_add_params = []

        def bulk_add():
            if bulk_add_params:
                self.seasearch_api.bulk(index_name, bulk_add_params)
                bulk_add_params.clear()

        def process_file(doc_uuid, content):
            index_info = {'index': {'_index': index_name, '_id': doc_uuid}}
            doc_info = {
                'wiki_id': wiki_id,
                'doc_uuid': doc_uuid,
                'content': content
            }
            bulk_add_params.extend([index_info, doc_info])
            if len(bulk_add_params) >= SEASEARCH_WIKI_BULK_ADD_LIMIT:
                bulk_add()

        for path, obj_id, mtime, size in files:
            if not is_in_wiki_dirs(path):
                continue

            if self.size_cap is not None and int(size) >= int(self.size_cap):
                continue
            doc_uuid = path.split('/')[2]
            if content := self.get_wiki_content(wiki_id, obj_id):
                process_file(doc_uuid, content)

        # Recovered files
        for doc_uuid, path in uuid_path.items():
            file_id = seafile_api.get_file_id_by_commit_and_path(wiki_id, commit_id, path)
            if content := self.get_wiki_content(wiki_id, file_id):
                process_file(doc_uuid, content)
        bulk_add()

    def delete_files(self, index_name, dirs, doc_uuids):
        delete_params = []

        def bulk_delete():
            if delete_params:
                self.seasearch_api.bulk(index_name, delete_params)
                delete_params.clear()

        for path in dirs:
            if not is_in_wiki_dirs(path):
                continue
            doc_uuid = path.split('/')[2]
            delete_params.append({'delete': {'_id': doc_uuid, '_index': index_name}})
            if len(delete_params) >= SEASEARCH_WIKI_BULK_DELETE_LIMIT:
                bulk_delete()

        for doc_uuid in doc_uuids:
            delete_params.append({'delete': {'_id': doc_uuid, '_index': index_name}})
            if len(delete_params) >= SEASEARCH_WIKI_BULK_DELETE_LIMIT:
                bulk_delete()
        bulk_delete()

    def update(self, index_name, wiki_id, old_commit_id, new_commit_id):
        # Clean trash equivalent to remove corresponding dirs
        added_files, _, modified_files, _, deleted_dirs = \
            get_library_diff_files(wiki_id, old_commit_id, new_commit_id)

        # When the file is placed in the recycle bin, the index_json file is modified
        if not (added_files or deleted_dirs or modified_files):
            return
        old_cfg = self.get_wiki_conf(wiki_id, old_commit_id)
        new_cfg = self.get_wiki_conf(wiki_id, new_commit_id)
        prev_uuid_paths, prev_recycled_uuid_paths = self.get_uuid_path_mapping(old_cfg)
        curr_uuid_paths, curr_recycled_uuid_paths = self.get_uuid_path_mapping(new_cfg)

        recently_trashed_uuids = (
            prev_uuid_paths.keys() & curr_recycled_uuid_paths.keys()
        )
        self.delete_files(index_name, deleted_dirs, list(recently_trashed_uuids))

        need_added_files = added_files + modified_files

        recently_restore_uuid_to_path = {
            uuid: path
            for uuid, path in curr_uuid_paths.items()
            if uuid in prev_recycled_uuid_paths
        }
        self.add_files(
            index_name, wiki_id, need_added_files, recently_restore_uuid_to_path, new_commit_id
        )

    def search_wiki(self, wiki, keyword, start=0, size=10):
        bulk_search_params = []

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
                }
                if highlight_content := hit.get('highlight', {}).get('content', [None])[0]:
                    r.update(content=highlight_content)
                wiki_content.append(r)

        return wiki_content

    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)