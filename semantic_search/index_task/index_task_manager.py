import logging

from seafevents.semantic_search.index_store.index_manager import IndexManager
from seafevents.semantic_search.index_store.repo_file_name_index import RepoFileNameIndex
from seafevents.semantic_search.utils.seasearch_api import SeaSearchAPI
from seafevents.repo_data import repo_data


logger = logging.getLogger(__name__)


class IndexTaskManager:

    def init(self, config):
        self.seasearch_api = SeaSearchAPI(
            config['SEMANTIC_SEARCH']['seasearch_url'],
            config['SEMANTIC_SEARCH']['seasearch_token'],
        )
        self._repo_data = repo_data
        self.index_manager = IndexManager(config)
        self._repo_filename_index = RepoFileNameIndex(
            self.seasearch_api,
            self._repo_data,
            int(config['SEMANTIC_SEARCH']['seasearch_shard_num']),
        )

    def keyword_search(self, query, repos, count, suffixes):
        return self.index_manager.keyword_search(
            query, repos, self._repo_filename_index, count, suffixes
        )


index_task_manager = IndexTaskManager()
