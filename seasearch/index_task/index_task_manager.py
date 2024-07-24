import logging

from seafevents.seasearch.index_store.index_manager import IndexManager
from seafevents.seasearch.index_store.repo_file_name_index import RepoFileNameIndex
from seafevents.seasearch.utils.seasearch_api import SeaSearchAPI
from seafevents.seasearch.config import SHARD_NUM
from seafevents.repo_data import repo_data
from seafevents.utils import parse_bool, get_opt_from_conf_or_env


logger = logging.getLogger(__name__)


class IndexTaskManager:
    def __init__(self):
        self.enabled = False

        self.seasearch_api = None
        self._repo_data = None
        self.index_manager = None
        self._repo_filename_index = None

    def init(self, config):
        self._parse_config(config)

    def _parse_config(self, config):
        """Parse fimename index update related parts of events.conf"""
        section_name = 'SEASEARCH'
        key_enabled = 'enabled'
        es_section_name = 'INDEX FILES'

        if config.has_section(es_section_name):
            es_enabled = get_opt_from_conf_or_env(config, es_section_name, key_enabled, default=False)
            if es_enabled := parse_bool(es_enabled):
                return
        if not config.has_section(section_name):
            return

        # [ enabled ]
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)
        if not enabled:
            return
        self.enabled = True

        seasearch_url = get_opt_from_conf_or_env(
            config, section_name, 'seasearch_url'
        )
        seasearch_token = get_opt_from_conf_or_env(
            config, section_name, 'seasearch_token'
        )
    
        self.seasearch_api = SeaSearchAPI(
            seasearch_url,
            seasearch_token,
        )
        self._repo_data = repo_data
        self.index_manager = IndexManager()
        self._repo_filename_index = RepoFileNameIndex(
            self.seasearch_api,
            self._repo_data,
            int(SHARD_NUM),
        )

    def keyword_search(self, query, repos, count, suffixes):
        return self.index_manager.keyword_search(
            query, repos, self._repo_filename_index, count, suffixes
        )


index_task_manager = IndexTaskManager()
