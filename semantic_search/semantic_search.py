import logging

from seafevents.semantic_search.index_task.index_task_manager import index_task_manager
from seafevents.semantic_search.index_task.filename_index_updater import repo_filename_index_updater
from seafevents.semantic_search.index_store.index_manager import IndexManager
from seafevents.semantic_search.index_store.repo_status_index import RepoStatusIndex
from seafevents.semantic_search.index_store.repo_file_index import RepoFileIndex
from seafevents.semantic_search.index_store.repo_file_name_index import RepoFileNameIndex
from seafevents.semantic_search.utils.seasearch_api import SeaSearchAPI
from seafevents.semantic_search.utils.sea_embedding_api import SeaEmbeddingAPI
from seafevents.semantic_search.utils.constants import REPO_STATUS_FILE_INDEX_NAME, REPO_STATUS_FILENAME_INDEX_NAME
from seafevents.repo_data import repo_data
from seafevents.semantic_search import config

logger = logging.getLogger(__name__)

class SemanticSearch():
    def __init__(self):
        self.index_manager = None
        self.seasearch_api = None
        self.repo_data = None
        self.embedding_api = None

        # for semantic search
        self.repo_status_index = None
        self.repo_file_index = None

        # for keyword search
        self.repo_status_filename_index = None
        self.repo_filename_index = None
        self.index_task_manager = None
        self.repo_filename_index_updater = None

    def init(self):
        self.index_manager = IndexManager()
        self.seasearch_api = SeaSearchAPI(config.SEASEARCH_SERVER, config.SEASEARCH_TOKEN)
        self.repo_data = repo_data
        self.embedding_api = SeaEmbeddingAPI(config.APP_NAME, config.SEA_EMBEDDING_SERVER)

        # for semantic search
        self.repo_status_index = RepoStatusIndex(self.seasearch_api, REPO_STATUS_FILE_INDEX_NAME)
        self.repo_file_index = RepoFileIndex(self.seasearch_api)

        # for keyword search
        self.repo_status_filename_index = RepoStatusIndex(self.seasearch_api, REPO_STATUS_FILENAME_INDEX_NAME)
        self.repo_filename_index = RepoFileNameIndex(self.seasearch_api, self.repo_data)
        self.index_task_manager = index_task_manager
        self.repo_filename_index_updater = repo_filename_index_updater

sem_app = SemanticSearch()
