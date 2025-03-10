import json
import os
import logging

from seafevents.seasearch.utils import get_library_diff_files, md5, is_sys_dir_or_file
from seafevents.seasearch.utils.constants import REPO_FILENAME_INDEX_PREFIX
from seafevents.repo_metadata.constants import METADATA_TABLE
from seafevents.repo_metadata.utils import get_metadata_by_obj_ids

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
            'description': {
                'type': 'text',
                'analyzer': 'standard'
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
        searches.append(_make_match_query('description', keyword, **match_query_kwargs))
        return searches

    def _ensure_filter_exists(self, query_map):
        if 'filter' not in query_map['bool']:
            query_map['bool']['filter'] = []
        return query_map

    def _add_path_filter(self, query_map, search_path):
        if search_path is None:
            return query_map

        query_map = self._ensure_filter_exists(query_map)
        query_map['bool']['filter'].append({'prefix': {'path': search_path}})
        return query_map

    def _add_suffix_filter(self, query_map, suffixes):
        if suffixes is None:
            return query_map

        query_map = self._ensure_filter_exists(query_map)

        if isinstance(suffixes, list):
            suffixes = [x.lower() for x in suffixes]
            query_map['bool']['filter'].append({'terms': {'suffix': suffixes}})
        else:
            query_map['bool']['filter'].append({'term': {'suffix': suffixes.lower()}})
        return query_map

    def _add_obj_type_filter(self, query_map, obj_type):
        if obj_type is None:
            return query_map

        query_map = self._ensure_filter_exists(query_map)

        query_map['bool']['filter'].append({'term': {'is_dir': obj_type == 'dir'}})
        return query_map

    def search_files(self, repos, keyword, start=0, size=10, suffixes=None, search_path=None, obj_type=None):
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
                if search_path:
                    search_path = os.path.join(origin_path, search_path.strip('/'))
                else:
                    search_path = origin_path

            query_map = self._add_suffix_filter(query_map, suffixes)
            query_map = self._add_path_filter(query_map, search_path)
            query_map = self._add_obj_type_filter(query_map, obj_type)
            data = {
                'query': query_map,
                'from': start,
                'size': size,
                '_source': ['path', 'repo_id', 'filename', 'is_dir'],
                'sort': ['_score']
            }
            index_name = REPO_FILENAME_INDEX_PREFIX + repo_id
            repo_query_info = {
                'index': index_name,
                'query': data
            }
            bulk_search_params.append(repo_query_info)

            search_path = None
        query_body = json.dumps({
            'index_queries': bulk_search_params
        })
        results = self.seasearch_api.unified_search(query_body)
        files = []

        hits = results.get('hits', []).get('hits', [])
        total = results.get('hits', {}).get('total', {}).get('value', 0)

        if not hits:
            return files

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

        logger.debug('search keyword: %s, search path: %s, in repos: %s , \nsearch result: %s', keyword, search_path,
                    repos, files)

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

    def add_files(self, index_name, repo_id, files, path_to_metadata_row):
        bulk_add_params = []
        for file_info in files:
            path = file_info[0]

            if is_sys_dir_or_file(path):
                continue

            suffix = self.get_file_suffix(path)
            filename = os.path.basename(path)
            if suffix:
                filename = filename[:-len(suffix)-1]

            index_info = {'index': {'_index': index_name, '_id': md5(path)}}
            metadata_row = path_to_metadata_row.get(path, {})
            doc_info = {
                'repo_id': repo_id,
                'path': path,
                'suffix': suffix,
                'filename': filename,
                'description': metadata_row.get('_description', ''),
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
            "_source": ["path"],
            "sort": ["-@timestamp"],  # sort is for getting data ordered
        }

        hits, total = self.normal_search(index_name, dsl)
        return hits, total

    def query_data_by_paths(self, index_name, paths, start, size):
        dsl = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "terms": {
                                "path": paths
                            }
                        }
                    ]
                }
            },
            "from": start,
            "size": size,
            "_source": ["path"],
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

    def filter_exist_paths(self, index_name, paths):
        exist_paths = []
        per_size = SEASEARCH_BULK_OPETATE_LIMIT
        start = 0
        for i in range(0, len(paths), per_size):
            hits, total = self.query_data_by_paths(index_name, paths[i: i + per_size], start, per_size)
            for hit in hits:
                source = hit.get('_source')
                exist_paths.append(source['path'])

        return exist_paths

    def update(self, index_name, repo_id, old_commit_id, new_commit_id, metadata_rows, metadata_server_api, need_index_metadata):
        added_files, deleted_files, modified_files, added_dirs, deleted_dirs = \
            get_library_diff_files(repo_id, old_commit_id, new_commit_id)

        need_deleted_files = deleted_files
        self.delete_files(index_name, need_deleted_files)

        self.delete_dirs(index_name, deleted_dirs)

        self.delete_files_by_deleted_dirs(index_name, deleted_dirs)

        need_added_files = added_files + modified_files

        need_update_metadata_files, path_to_metadata_row = [], {}
        if need_index_metadata:
            need_update_metadata_files, path_to_metadata_row = self.cal_metadata_files(index_name, repo_id, metadata_rows, need_added_files, metadata_server_api)

        self.add_files(index_name, repo_id, need_added_files + need_update_metadata_files, path_to_metadata_row)
        self.add_dirs(index_name, repo_id, added_dirs)

    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)

    def cal_metadata_files(self, index_name, repo_id, metadata_rows, need_added_files, metadata_server_api):
        metadata_files = []
        path_to_metadata_row = {}
        metadate_file_obj_ids = []
        need_added_paths = {item[0] for item in need_added_files}
        for row in metadata_rows:
            path = os.path.join(row[METADATA_TABLE.columns.parent_dir.name], row[METADATA_TABLE.columns.file_name.name])
            path_to_metadata_row[path] = row
            metadate_file_obj_ids.append(row['_obj_id'])
            if path not in need_added_paths:
                metadata_files.append([path, row['_obj_id']])

        added_files_lacked_obj_ids = [file_info[1] for file_info in need_added_files if file_info[1] not in metadate_file_obj_ids]
        added_files_lacked_rows = get_metadata_by_obj_ids(repo_id, added_files_lacked_obj_ids,
                                                          metadata_server_api) if added_files_lacked_obj_ids else []
        if not added_files_lacked_rows:
            added_files_lacked_rows = []

        for row in added_files_lacked_rows:
            path = os.path.join(row[METADATA_TABLE.columns.parent_dir.name], row[METADATA_TABLE.columns.file_name.name])
            if path not in need_added_paths:
                continue
            path_to_metadata_row[path] = row

        paths = self.filter_exist_paths(index_name, [item[0] for item in metadata_files])
        need_update_metadata_files = [item for item in metadata_files if item[0] in paths]
        return need_update_metadata_files, path_to_metadata_row
