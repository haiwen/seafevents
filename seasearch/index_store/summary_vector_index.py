import os

from seafevents.seasearch.utils.constants import SUMMARY_VECTOR_INDEX_PREFIX


class SummaryVectorIndex:
    mapping = {
        'properties': {
            'repo_id': {'type': 'keyword'},
            'path': {'type': 'keyword'},
            'filename': {'type': 'text'},
            'ai_summary': {'type': 'text'},
            'ai_summary_mtime': {'type': 'date'},
            'mtime': {'type': 'date', 'format': 'epoch_millis'},
            'embedding_model': {'type': 'keyword'},
            'vec': {
                'type': 'vector',
                'dims': 1536,
                'm': 64,
                'nbits': 8,
                'vec_index_type': 'flat',
            },
        }
    }

    def __init__(self, seasearch_api, shard_num=1):
        self.seasearch_api = seasearch_api
        self.shard_num = shard_num

    @staticmethod
    def get_index_name(repo_id):
        return SUMMARY_VECTOR_INDEX_PREFIX + repo_id

    def index_exists(self, index_name):
        return bool(self.seasearch_api.check_index_mapping(index_name).get('is_exist'))

    def create_index_if_missing(self, index_name):
        if not self.index_exists(index_name):
            self.seasearch_api.create_index(index_name, {
                'shard_num': self.shard_num,
                'mappings': self.mapping,
            })

    def index_documents(self, index_name, repo_id, rows, embeddings, embedding_model):
        if len(rows) != len(embeddings):
            raise ValueError('Embedding response count mismatch')

        self.delete_paths(index_name, [row['path'] for row in rows])

        bulk_params = []
        for row, embedding in zip(rows, embeddings):
            path = row['path']
            bulk_params.append({'index': {'_index': index_name}})
            bulk_params.append({
                'repo_id': repo_id,
                'path': path,
                'filename': os.path.basename(path),
                'ai_summary': row['ai_summary'],
                'ai_summary_mtime': row.get('ai_summary_mtime'),
                'mtime': row.get('mtime'),
                'embedding_model': embedding_model,
                'vec': embedding,
            })
        if bulk_params:
            self._bulk(index_name, bulk_params)

    def delete_paths(self, index_name, paths):
        if not paths or not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            return
        while True:
            response = self.seasearch_api.normal_search(index_name, {
                'query': {'terms': {'path': paths}},
                '_source': False,
                'size': 200,
                'from': 0,
            })
            hits = response.get('hits', {}).get('hits', [])
            if not hits:
                break
            self._bulk(index_name, [
                {'delete': {'_id': hit['_id'], '_index': index_name}}
                for hit in hits
            ])
            if len(hits) < 200:
                break

    def delete_directories(self, index_name, directories):
        if not directories or not self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            return
        for directory in directories:
            prefix = directory.rstrip('/') + '/'
            while True:
                response = self.seasearch_api.normal_search(index_name, {
                    'query': {'prefix': {'path': prefix}},
                    '_source': False,
                    'size': 200,
                    'from': 0,
                })
                hits = response.get('hits', {}).get('hits', [])
                if not hits:
                    break
                self._bulk(index_name, [
                    {'delete': {'_id': hit['_id'], '_index': index_name}}
                    for hit in hits
                ])
                if len(hits) < 200:
                    break

    def delete_index(self, index_name):
        if self.seasearch_api.check_index_mapping(index_name).get('is_exist'):
            self.seasearch_api.delete_index_by_name(index_name)

    def _bulk(self, index_name, params):
        response = self.seasearch_api.bulk(index_name, params) or {}
        if response.get('error') or response.get('errors'):
            raise RuntimeError('SeaSearch bulk operation failed')
        for item in response.get('items', []):
            result = next(iter(item.values()), {})
            if result.get('error'):
                raise RuntimeError('SeaSearch bulk item failed: %s' % result['error'])
