import logging
import os

from seaserv import seafile_api

from seafevents.seasearch.utils.constants import SEASEARCH_BULK_OPETATE_LIMIT, THRESHOLD, DIMENSION, VECTOR_M, SHARD_NUM
from seafevents.repo_metadata.seafile_ai_api import SeafileAIAPI

from seafevents.utils import timestamp_to_isoformat_timestr

logger = logging.getLogger(__name__)


class RepoImageIndex(object):

    def __init__(self, seasearch_api):
        self.seasearch_api = seasearch_api
        self.seafile_ai_api = SeafileAIAPI()

    def create_index_if_missing(self, index_name):
        if not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            mapping = {
                "properties": {
                    "vec": {
                        "type": "vector",
                        "dims": DIMENSION,
                        "vec_index_type": "flat",
                        "m": VECTOR_M
                    },
                    "path": {
                        "type": "keyword"
                    }
                }
            }
            data = {
                'shard_num': SHARD_NUM,
                'mappings': mapping,
                'settings': {}
            }
            self.seasearch_api.create_index(index_name, data)

    def check_index(self, index_name):
        return self.seasearch_api.check_index_mapping(index_name).get('is_exist')

    def add_images(self, index_name, images):
        bulk_add_params = []
        for image in images:
            path = image['path']
            embedding = image['embedding']

            if not embedding:
                continue

            index_info = {'index': {'_index': index_name}}
            doc_info = {
                'path': path,
                'vec': embedding,
            }

            bulk_add_params.append(index_info)
            bulk_add_params.append(doc_info)

            # bulk add every 2000 params
            if len(bulk_add_params) >= SEASEARCH_BULK_OPETATE_LIMIT:
                self.seasearch_api.bulk(index_name, bulk_add_params)
                bulk_add_params = []
        if bulk_add_params:
            self.seasearch_api.bulk(index_name, bulk_add_params)

    def delete_images(self, index_name, paths):
        if not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            return
        per_size = SEASEARCH_BULK_OPETATE_LIMIT
        start = 0
        delete_params = []
        while True:
            hits, total = self.query_data_by_paths(index_name, paths, start, per_size)
            for hit in hits:
                _id = hit['_id']
                delete_params.append({'delete': {'_id': _id, '_index': index_name}})

            if delete_params:
                self.seasearch_api.bulk(index_name, delete_params)
            if len(hits) < per_size:
                break
            start += per_size

    def query_data_by_paths(self, index_name, path_list, start, size):
        dsl = {
            "query": {
                "terms": {
                    "path": path_list
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

    def delete_index_by_index_name(self, index_name):
        self.seasearch_api.delete_index_by_name(index_name)

    def image_search(self, index_name, repo_id, obj_id, file_path, k):
        embeddings = self.seafile_ai_api.images_embedding(repo_id, [obj_id])
        data = {
            "query_field": "vec",
            "k": k,
            "return_fields": ["path"],
            "_source": False,
            "vector": embeddings['data'][0]['embedding']
        }
        result = self.seasearch_api.vector_search(index_name, data)
        if result.get('error'):
            logger.warning('search in vector_search error: %s .', result.get('error'))
            return []

        hits = result['hits']['hits']
        if not hits:
            return []
        searched_result = []
        for hit in hits:
            score = hit['_score']
            _id = hit['_id']
            path = hit['fields']['path'][0]
            if path == file_path:
                continue

            if score < THRESHOLD:
                continue

            file_obj = seafile_api.get_dirent_by_path(repo_id, file_path)
            result_dict = {
                '_id': _id,
                'path': path,
                'file_name': os.path.basename(path),
                'parent_dir': os.path.dirname(path),
                'size': file_obj.size if file_obj else None,
                'mtime': timestamp_to_isoformat_timestr(file_obj.mtime) if file_obj else None
            }

            searched_result.append(result_dict)

        return searched_result
