import json
import os
import logging

from seafevents.seasearch.utils import get_library_diff_files, md5, is_sys_dir_or_file
from seafevents.seasearch.utils.constants import REPO_FILENAME_INDEX_PREFIX
from seafevents.repo_metadata.constants import METADATA_TABLE
from seafevents.repo_metadata.utils import get_metadata_by_obj_ids

logger = logging.getLogger(__name__)

SEASEARCH_BULK_OPETATE_LIMIT = 2000


# 管理仓库文件索引和搜索的 RepoFileNameIndex 对象
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

    # 初始化对象，需要 seasearch_api、repo_data 和 shard_num。
    def __init__(self, seasearch_api, repo_data, shard_num):
        self.seasearch_api = seasearch_api
        self.repo_data = repo_data
        self.shard_num = shard_num

    # 如果索引不存在，则创建一个新的索引。
    def create_index_if_missing(self, index_name):
        if not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            data = {
                'shard_num': self.shard_num,
                'mappings': self.mapping,
                'settings': self.index_settings
            }
            self.seasearch_api.create_index(index_name, data)

    # 检查指定名称的索引是否存在。
    def check_index(self, index_name):
        return self.seasearch_api.check_index_mapping(index_name).get('is_exist')

    # 为给定的关键字创建一个搜索查询列表。
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

    # 为搜索查询添加过滤器的辅助方法。
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

    # 核心函数：根据关键字、路径、后缀和对象类型搜索仓库中的文件。
    # 这个代码片段定义了一个名为 `search_files` 的方法，用于根据给定的关键词、路径、后缀、对象类型、起始索引和大小在仓库中搜索文件。
    def search_files(self, repos, keyword, start=0, size=10, suffixes=None, search_path=None, obj_type=None):
        # 该方法接受一个仓库列表 (`repos`）、一个要搜索的关键词以及可选的起始索引、大小、后缀、搜索路径和对象类型参数。
        # 它创建一个名为 `bulk_search_params` 的列表来存储每个仓库的搜索参数。
        bulk_search_params = []
        for repo in repos:
            # 对于每个仓库，它提取仓库 ID、原始仓库 ID 和原始路径。
            # 然后，它创建一个带有布尔查询的查询映射，该布尔查询包含一个由 `_make_query_searches` 方法生成的搜索查询列表。
            repo_id = repo[0]
            origin_repo_id = repo[1]
            origin_path = repo[2]
            query_map = {'bool': {'should': [], 'minimum_should_match': 1}}
            searches = self._make_query_searches(keyword)
            query_map['bool']['should'] = searches

            # 如果仓库有原始仓库 ID，它会更新仓库 ID 和路径。然后，它使用帮助方法向查询映射中添加后缀、路径和对象类型过滤器。
            if origin_repo_id:
                repo_id = origin_repo_id
                if search_path:
                    search_path = os.path.join(origin_path, search_path.strip('/'))
                else:
                    search_path = origin_path

            # 它创建一个包含查询映射、起始索引、大小和要返回的源字段的数据字典。
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

        # 它根据仓库 ID 构造索引名称，并将索引和数据字典添加到 `bulk_search_params` 列表中。
        # 在迭代所有仓库之后，它使用 `bulk_search_params` 列表调用 `seasearch_api` 对象的 `m_search` 方法来执行搜索。
        query_body = json.dumps({
            'index_queries': bulk_search_params
        })
        # 新版采用联合搜索
        results = self.seasearch_api.unified_search(query_body)
        files = []

        # 它处理搜索结果，并创建一个包含文件的仓库 ID、全路径、名称、是否为目录、分数和 ID 的列表。
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

        # 它按分数降序对文件进行排序，并返回前 `size` 个文件。
        # 最后，它记录搜索关键词、搜索路径和仓库，以及搜索结果。
        # 总的来说，这个代码片段实现了基于各种搜索参数在仓库中搜索文件的功能。
        logger.debug('search keyword: %s, search path: %s, in repos: %s , \nsearch result: %s', keyword, search_path,
                    repos, files)

        return files

    # 返回给定路径的文件后缀。
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

    # 将文件和目录添加到索引中：它批量将文件添加到索引中。
    def add_files(self, index_name, repo_id, files, path_to_metadata_row):
        bulk_add_params = []
        # 它遍历文件列表
        for file_info in files:
            path = file_info[0]

            # 跳过系统目录和文件
            if is_sys_dir_or_file(path):
                continue

            suffix = self.get_file_suffix(path)
            filename = os.path.basename(path)
            if suffix:
                filename = filename[:-len(suffix)-1]

            # 对于每个文件，它提取文件后缀、文件名和元数据（如果可用）。
            index_info = {'index': {'_index': index_name, '_id': md5(path)}}
            metadata_row = path_to_metadata_row.get(path, {})

            # 它创建一个文档信息字典，包含文件的元数据，并将其添加到批量添加参数列表中。
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


            # 当列表达到一定限制（2000）时，它使用 seasearch_api 批量将文件添加到索引中
            # bulk add every 2000 params
            if len(bulk_add_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, bulk_add_params)
                bulk_add_params = []
        # 最后，它将剩余的文件添加到索引中。
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
                continue
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

    # 从索引中删除文件和目录。
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

    # 根据目录或路径查询数据。
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

    # 在索引上执行正常搜索。
    def normal_search(self, index_name, dsl):
        doc_item = self.seasearch_api.normal_search(index_name, dsl)
        total = doc_item['hits']['total']['value']

        return doc_item['hits']['hits'], total

    # 删除已删除目录中的文件。
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

    # 从路径列表中过滤出已存在的路径。
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

    # 更新索引中的数据。
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

    # 更新资料库名称
    def update_repo_name(self, index_name, repo_id):
        repo = self.repo_data.get_repo_name_mtime_size(repo_id)
        if not repo:
            return
        path = '/'
        bulk_add_params = []
        index_info = {'index': {'_index': index_name, '_id': md5(path)}}
        doc_info = {
            'repo_id': repo_id,
            'path': path,
            'suffix': None,
            'filename': repo[0]['name'],
            'is_dir': True,
        }
        bulk_add_params.append(index_info)
        bulk_add_params.append(doc_info)

        self.seasearch_api.bulk(index_name, bulk_add_params)

    # 根据名称删除索引。
    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)

    # 计算仓库和元数据行的元数据文件。
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
