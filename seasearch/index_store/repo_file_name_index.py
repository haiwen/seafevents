import json
import os
import logging

from seafevents.seasearch.utils import get_library_diff_files, md5, is_sys_dir_or_file
from seafevents.seasearch.utils.constants import REPO_FILENAME_INDEX_PREFIX

logger = logging.getLogger(__name__)

SEASEARCH_BULK_OPETATE_LIMIT = 2000


class RepoFileNameIndex(object):
    mapping = {
        'properties': {
            'repo_id': {
                'type': 'keyword',
            },
            'path': {
                'type': 'keyword'
            },
            'filename': {
                'type': 'text',
                'fields': {
                    'ngram': {
                        'type': 'text',
                        'index': True,
                        'analyzer': 'seafile_file_name_ngram_analyzer',
                    },
                },
            },
            'suffix': {
                'type': 'keyword'
            },
            'is_dir': {
                'type': 'boolean',
            }
        }
    }

    index_settings = {
        'analysis': {
            'analyzer': {
                'seafile_file_name_ngram_analyzer': {
                    'type': 'custom',
                    'tokenizer': 'seafile_file_name_ngram_tokenizer',
                    'filter': [
                        'lowercase',
                    ],
                }
            },
            'tokenizer': {
                'seafile_file_name_ngram_tokenizer': {
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

    def _make_query_searches(self, keyword):
        match_query_kwargs = {'minimum_should_match': '-25%'}

        def _make_match_query(field, key_word, **kw):
            q = {'query': key_word}
            q.update(kw)
            return {'match': {field: q}}

        searches = []
        searches.append(_make_match_query('filename', keyword, **match_query_kwargs))
        searches.append({
            'match': {
                'filename.ngram': {
                    'query': keyword,
                    'minimum_should_match': '80%',
                }
            }
        })
        return searches

    def _add_path_filter(self, query_map, search_path):
        if search_path is None:
            return query_map

        if query_map['bool'].get('filter'):
            query_map['bool']['filter'].append({'prefix': {'path': search_path}})
        else:
            query_map['bool']['filter'] = [{'prefix': {'path': search_path}}]
        return query_map

    def _add_suffix_filter(self, query_map, suffixes):
        if suffixes:
            if not query_map['bool'].get('filter'):
                query_map['bool']['filter'] = []
            if isinstance(suffixes, list):
                suffixes = [x.lower() for x in suffixes]
                query_map['bool']['filter'].append({'terms': {'suffix': suffixes}})
            else:
                query_map['bool']['filter'].append({'term': {'suffix': suffixes.lower()}})
        return query_map

    def search_files(self, repos, keyword, start=0, size=10, suffixes=None):
        bulk_search_params = []
        for repo in repos:
            repo_id = repo[0]
            origin_repo_id = repo[1]
            origin_path = repo[2]
            query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
            searches = self._make_query_searches(keyword)
            query_map['bool']['should'] = searches

            if origin_repo_id:
                repo_id = origin_repo_id
                query_map = self._add_path_filter(query_map, origin_path)
            query_map = self._add_suffix_filter(query_map, suffixes)

            data = {
                'query': query_map,
                'from': start,
                'size': size,
                '_source': ['path', 'repo_id', 'filename', 'is_dir'],
                'sort': ['_score']
            }
            index_name = REPO_FILENAME_INDEX_PREFIX + repo_id
            index_info = {"index": index_name}
            bulk_search_params.append(index_info)
            bulk_search_params.append(data)

        logger.debug('search in repo_filename_index params: %s', json.dumps(bulk_search_params))

        results = self.seasearch_api.m_search(bulk_search_params)
        files = []

        for result in results.get('responses'):
            hits = result.get('hits', {}).get('hits', [])

            if not hits:
                continue

            for hit in hits:
                source = hit.get('_source')
                score = hit.get('_score')
                _id = hit.get('_id')
                r = {
                    'repo_id': source['repo_id'],
                    'fullpath': source['path'],
                    'name': source['filename'],
                    'is_dir': source['is_dir'],
                    'score': score,
                    '_id': _id,
                }
                files.append(r)
        files = sorted(files, key=lambda row: row['score'], reverse=True)[:size]

        return files

    @staticmethod
    def get_file_suffix(path):
        try:
            name = os.path.basename(path)
            suffix = os.path.splitext(name)[1][1:]
            if suffix:
                return suffix.lower()
            return None
        except:
            return None

    def add_files(self, index_name, repo_id, files):
        bulk_add_params = []
        for file_info in files:
            path = file_info[0]
            obj_id = file_info[1]
            mtime = file_info[2]
            size = file_info[3]

            if is_sys_dir_or_file(path):
                continue

            suffix = self.get_file_suffix(path)
            filename = os.path.basename(path)
            if suffix:
                filename = filename[:-len(suffix)-1]

            index_info = {'index': {'_index': index_name, '_id': md5(path)}}
            doc_info = {
                'repo_id': repo_id,
                'path': path,
                'suffix': suffix,
                'filename': filename,
                'is_dir': False,
            }

            bulk_add_params.append(index_info)
            bulk_add_params.append(doc_info)

            # bulk add every 2000 params
            if len(bulk_add_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, bulk_add_params)
                bulk_add_params = []
        if bulk_add_params:
            self.seasearch_api.bulk(index_name, bulk_add_params)

    def add_dirs(self, index_name, repo_id, dirs):
        bulk_add_params = []
        for dir in dirs:
            path = dir[0]
            obj_id = dir[1]
            mtime = dir[2]
            size = dir[3]

            if is_sys_dir_or_file(path):
                continue

            if path == '/':
                repo = self.repo_data.get_repo_name_mtime_size(repo_id)
                if not repo:
                    return

                filename = repo[0]['name']
            else:
                filename = os.path.basename(path)

            path = path + '/' if path != '/' else path
            index_info = {'index': {'_index': index_name, '_id': md5(path)}}
            doc_info = {
                'repo_id': repo_id,
                'path': path,
                'suffix': None,
                'filename': filename,
                'is_dir': True,
            }
            bulk_add_params.append(index_info)
            bulk_add_params.append(doc_info)

            # bulk add every 2000 params
            if len(bulk_add_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, bulk_add_params)
                bulk_add_params = []
        if bulk_add_params:
            self.seasearch_api.bulk(index_name, bulk_add_params)

    def delete_files(self, index_name, files):
        delete_params = []
        for file in files:
            path = file[0]
            if is_sys_dir_or_file(path):
                continue
            delete_params.append({'delete': {'_id': md5(path), '_index': index_name}})
            # bulk add every 2000 params
            if len(delete_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, delete_params)
                delete_params = []
        if delete_params:
            self.seasearch_api.bulk(index_name, delete_params)

    def delete_dirs(self, index_name, dirs):
        delete_params = []
        for dir in dirs:
            path = dir

            if is_sys_dir_or_file(path):
                continue
            path = path + '/' if path != '/' else path
            delete_params.append({'delete': {'_id': md5(path), '_index': index_name}})
            # bulk add every 2000 params
            if len(delete_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, delete_params)
                delete_params = []
        if delete_params:
            self.seasearch_api.bulk(index_name, delete_params)

    def query_data_by_dir(self, index_name, directory, start, size):
        dsl = {
            "query": {
                "bool": {
                    "must": [
                        {"prefix": {"path": directory}}
                    ]
                }
            },
            "from": start,
            "size": size,
            "_source": False,
            "sort": ["-@timestamp"],  # sort is for getting data ordered
        }

        hits, total = self.normal_search(index_name, dsl)
        return hits, total

    def normal_search(self, index_name, dsl):
        doc_item = self.seasearch_api.normal_search(index_name, dsl)
        total = doc_item['hits']['total']['value']

        return doc_item['hits']['hits'], total

    def delete_files_by_deleted_dirs(self, index_name, dirs):
        for directory in dirs:
            if is_sys_dir_or_file(directory):
                continue
            per_size = SEASEARCH_BULK_OPETATE_LIMIT
            start = 0
            delete_params = []
            while True:
                hits, total = self.query_data_by_dir(index_name, directory, start, per_size)
                for hit in hits:
                    _id = hit['_id']
                    delete_params.append({'delete': {'_id': _id, '_index': index_name}})

                if delete_params:
                    self.seasearch_api.bulk(index_name, delete_params)
                if len(hits) < per_size:
                    break

    def update(self, index_name, repo_id, old_commit_id, new_commit_id):
        added_files, deleted_files, modified_files, added_dirs, deleted_dirs = \
            get_library_diff_files(repo_id, old_commit_id, new_commit_id)

        need_deleted_files = deleted_files
        self.delete_files(index_name, need_deleted_files)

        self.delete_dirs(index_name, deleted_dirs)

        self.delete_files_by_deleted_dirs(index_name, deleted_dirs)

        need_added_files = added_files + modified_files
        self.add_files(index_name, repo_id, need_added_files)

        self.add_dirs(index_name, repo_id, added_dirs)

    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)
