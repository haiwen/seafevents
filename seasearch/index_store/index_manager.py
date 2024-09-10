import json
import logging
import os
import time

from seafevents.seasearch.utils import need_index_description
from seafevents.db import init_db_session_class
from seafevents.seasearch.utils.constants import ZERO_OBJ_ID, REPO_FILENAME_INDEX_PREFIX
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.utils import METADATA_TABLE
from seafevents.utils import get_opt_from_conf_or_env

logger = logging.getLogger(__name__)


class IndexManager(object):

    def __init__(self, config):
        self.session = init_db_session_class(config)
        self.metadata_server_api = MetadataServerAPI('seafevents')
        self.description_index_info_dir = get_opt_from_conf_or_env(
            config, 'SEASEARCH', 'description_index_info_dir'
        )
        if not os.path.exists(self.description_index_info_dir):
            os.makedirs(self.description_index_info_dir)

    def update_library_filename_index(self, repo_id, commit_id, repo_filename_index, repo_status_filename_index):
        try:
            new_commit_id = commit_id
            index_name = REPO_FILENAME_INDEX_PREFIX + repo_id

            repo_filename_index.create_index_if_missing(index_name)

            repo_status = repo_status_filename_index.get_repo_status_by_id(repo_id)
            from_commit = repo_status.from_commit
            to_commit = repo_status.to_commit

            if not from_commit:
                commit_id = ZERO_OBJ_ID
            else:
                commit_id = from_commit

            rows = []
            description_index_info = []
            description_index_info_path = os.path.join(self.description_index_info_dir, repo_id + '.json')
            if need_index_description(repo_id, self.session, self.metadata_server_api):
                sql = f'SELECT `_id`, `_mtime`, `_description`, `_parent_dir`, `_name` FROM `{METADATA_TABLE.name}`'
                rows = self.metadata_server_api.query_rows(repo_id, sql, []).get('results', [])
                if os.path.exists(description_index_info_path):
                    with open(description_index_info_path, 'r') as fp:
                        description_index_info = json.load(fp)
            if repo_status.need_recovery():
                logger.warning('%s: repo filename index inrecovery', repo_id)
                description_index_info = repo_filename_index.update(index_name, repo_id, commit_id, to_commit, rows, description_index_info)
                commit_id = to_commit
                time.sleep(1)

            repo_status_filename_index.begin_update_repo(repo_id, commit_id, new_commit_id)
            description_index_info = repo_filename_index.update(index_name, repo_id, commit_id, new_commit_id, rows, description_index_info)
            with open(description_index_info_path, 'w') as f:
                f.write(json.dumps(description_index_info))
            repo_status_filename_index.finish_update_repo(repo_id, new_commit_id)

            logger.info('repo: %s, update repo filename index success', repo_id)

        except Exception as e:
            logger.exception('repo_id: %s, update repo filename index error: %s.', repo_id, e)

    def delete_repo_filename_index(self, repo_id, repo_filename_index, repo_status_filename_index):
        # first delete repo_file_index
        repo_filename_index_name = REPO_FILENAME_INDEX_PREFIX + repo_id
        repo_filename_index.delete_index_by_index_name(repo_filename_index_name)
        repo_status_filename_index.delete_documents_by_repo(repo_id)

    def keyword_search(self, query, repos, repo_filename_index, count, suffixes=None, search_path=None):
        return repo_filename_index.search_files(repos, query, self.session, self.metadata_server_api, 0, count, suffixes, search_path)
