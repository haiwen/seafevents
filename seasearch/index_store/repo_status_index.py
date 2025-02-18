class RepoStatus(object):
    def __init__(self, repo_id, from_commit, to_commit, metadata_updated_time):
        self.repo_id = repo_id
        self.from_commit = from_commit
        self.to_commit = to_commit
        self.metadata_updated_time = metadata_updated_time

    def need_recovery(self):
        return self.to_commit is not None


class RepoStatusIndex(object):
    """The repo-head index is used to store the status for each repo.
    用于管理资料库状态索引
    具体来说，它负责存储和管理每个资料库的状态，包括：
        资料库 ID (repo_id)
        提交 ID (commit_id)
        更新目标 (updatingto)
        元数据更新时间 (metadata_updated_time)

    For each repo:
    (1) before update: commit = <previously indexed commit>, updatingto = None
    (2) during updating: commit = <previously indexed commit>, updatingto = <current latest commit>
    (3) after updating: commit = <newly indexed commit>, updatingto = None

    When error occured during updating, the status is left in case (2). So the
    next time we update that repo, we can recover the failed process again.

    The elasticsearch document id for each repo in repo_head index is its repo
    id.
    
    对于每个资料库：
    （1）更新前：commit = <之前索引的提交>，updatingto=None`
    （2）更新过程中：commit = <之前索引的提交>，updatingto=<当前最新的提交>`
    （3）更新后：commit = <新索引的提交>，updatingto=None`
    当更新过程中出现错误时，状态会停留在（2）中。这样下次更新该资料库时，可以恢复之前失败的更新过程。
    在 repo_head 索引中，每个资料库的 Elasticsearch 文档 ID 是其资料库 ID。
    """

    mapping = {
        'properties': {
            'repo_id': {
                'type': 'keyword'
            },
            'commit_id': {
                'type': 'keyword'
            },
            'updatingto': {
                'type': 'keyword'
            },
            'metadata_updated_time': {
                'type': 'keyword'
            },
        },
    }

    def __init__(self, seasearch_api, index_name):
        self.index_name = index_name
        self.seasearch_api = seasearch_api
        self.create_index_if_missing()

    # 创建索引（如果不存在）
    def create_index_if_missing(self):
        if not self.seasearch_api.check_index_mapping(self.index_name).get('is_exist'):
            data = {
                'mappings': self.mapping,
            }
            self.seasearch_api.create_index(self.index_name, data)

    def check_repo_status(self, repo_id):
        return self.seasearch_api.check_document_by_id(self.index_name, repo_id).get('is_exist')

    # 添加资料库状态
    def add_repo_status(self, repo_id, commit_id, updatingto,  metadata_updated_time):
        data = {
            'repo_id': repo_id,
            'commit_id': commit_id,
            'updatingto': updatingto,
            'metadata_updated_time': metadata_updated_time,
        }

        doc_id = repo_id
        self.seasearch_api.create_document_by_id(self.index_name, doc_id, data)

    def begin_update_repo(self, repo_id, old_commit_id, new_commit_id, metadata_updated_time):
        self.add_repo_status(repo_id, old_commit_id, new_commit_id, metadata_updated_time)

    def finish_update_repo(self, repo_id, commit_id, metadata_updated_time):
        self.add_repo_status(repo_id, commit_id, None, metadata_updated_time)

    def delete_documents_by_repo(self, repo_id):
        return self.seasearch_api.delete_document_by_id(self.index_name, repo_id)

    # 根据资料库 ID 获取资料库状态
    def get_repo_status_by_id(self, repo_id):
        doc = self.seasearch_api.get_document_by_id(self.index_name, repo_id)
        if doc.get('error'):
            return RepoStatus(repo_id, None, None, None)

        commit_id = doc['_source']['commit_id']
        updatingto = doc['_source']['updatingto']
        metadata_updated_time = doc['_source']['metadata_updated_time']
        repo_id = doc['_source']['repo_id']

        return RepoStatus(repo_id, commit_id, updatingto, metadata_updated_time)

    def update_repo_status_by_id(self, doc_id, data):
        self.seasearch_api.update_document_by_id(self.index_name, doc_id, data)

    def get_repo_status_by_time(self, check_time):
        per_size = 2000
        start = 0
        repo_head_list = []
        while True:
            query_params = {
                "query": {
                    "bool": {
                        "must": [
                            {"range":
                                {"@timestamp":
                                    {
                                        "lt": check_time
                                    }
                                }
                            }
                        ]
                    }
                },
                "_source": ["commit_id", "updatingto", "metadata_updated_time"],
                "from": start,
                "size": per_size,
                "sort": ["-@timestamp"],
            }

            repo_heads, total = self._repo_head_search(query_params)
            repo_head_list.extend(repo_heads)
            start += per_size
            if len(repo_heads) < per_size or start == total:
                return repo_head_list

    def get_all_repos_from_index(self):
        start = 0
        per_size = 2000
        repo_head_list = []
        while True:
            repo_heads, total = self.get_repos_from_index_by_size(start, per_size)
            repo_head_list.extend(repo_heads)

            start += per_size
            if len(repo_heads) < per_size or start == total:
                return repo_head_list

    def get_repos_from_index_by_size(self, start, per_size):
        query_params = {
            'from': start,
            'size': per_size,
        }

        repo_heads, total = self._repo_head_search(query_params)
        return repo_heads, total

    def _repo_head_search(self, query_params):
        result = self.seasearch_api.normal_search(self.index_name, query_params)
        total = result['hits']['total']['value']
        hits = result['hits']['hits']
        repo_heads = []

        for hit in hits:
            repo_id = hit['_id']
            commit_id = hit.get('_source').get('commit_id')
            updatingto = hit.get('_source').get('updatingto')
            metadata_updated_time = hit.get('_source').get('metadata_updated_time')
            repo_head = {
                'repo_id': repo_id,
                'commit_id': commit_id,
                'updatingto': updatingto,
                'metadata_updated_time': metadata_updated_time
            }
            repo_heads.append(repo_head)
        return repo_heads, total

    def delete_index_by_index_name(self):
        self.seasearch_api.delete_index_by_name(self.index_name)
