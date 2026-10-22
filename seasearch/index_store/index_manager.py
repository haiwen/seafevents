import logging
import time

from requests.exceptions import RequestException

from seafevents.seasearch.utils import need_index_metadata_info
from seafevents.db import init_db_session_class
from seafevents.seasearch.utils.constants import ZERO_OBJ_ID, REPO_FILE_INDEX_PREFIX, \
    WIKI_INDEX_PREFIX
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.constants import METADATA_TABLE
from seafevents.utils import timestamp_to_isoformat_timestr

from seafevents.repo_metadata.utils import query_metadata_rows, get_metadata_by_row_ids

logger = logging.getLogger('seasearch')


class IndexManager(object):

    def __init__(self):
        self.session = init_db_session_class()
        self.metadata_server_api = MetadataServerAPI('seafevents')

    def update_library_file_index(self, repo_id, commit_id, repo_file_index, repo_status_file_index, metadata_query_time, update_metadata=True):
        try:
            new_commit_id = commit_id
            index_name = REPO_FILE_INDEX_PREFIX + repo_id

            index_exists = repo_file_index.check_index(index_name)
            repo_file_index.create_index_if_missing(index_name)

            repo_status = repo_status_file_index.get_repo_status_by_id(repo_id)
            from_commit = repo_status.from_commit
            to_commit = repo_status.to_commit
            metadata_last_updated_time = repo_status.metadata_updated_time
            if not index_exists:
                from_commit, to_commit = None, None
                metadata_last_updated_time = None

            if not from_commit:
                commit_id = ZERO_OBJ_ID
            else:
                commit_id = from_commit

            need_index_metadata = need_index_metadata_info(repo_id, self.session)
            recovering = index_exists and repo_status.need_recovery()
            if recovering:
                logger.warning('%s: repo file index inrecovery', repo_id)
                repo_file_index.update(index_name, repo_id, commit_id, to_commit, [], self.metadata_server_api, need_index_metadata)
                commit_id = to_commit

            if recovering or new_commit_id != commit_id:
                logger.info('SeaSearch file index updating: repo=%s from_commit=%s to_commit=%s recovery=%s rebuild=%s',
                            repo_id, commit_id, new_commit_id, recovering, not index_exists)
                repo_file_index.update_repo_name(index_name, repo_id)
                repo_status_file_index.begin_update_repo(repo_id, commit_id, new_commit_id, metadata_last_updated_time)
                repo_file_index.update(index_name, repo_id, commit_id, new_commit_id, [], self.metadata_server_api, need_index_metadata)
                repo_status_file_index.finish_update_repo(repo_id, new_commit_id, metadata_last_updated_time)
            else:
                logger.info('SeaSearch file content unchanged: repo=%s commit=%s', repo_id, new_commit_id)

            # Preserve the manual indexing API, but never rebuild content from metadata rows.
            if update_metadata:
                self.compensate_library_descriptions(repo_id, repo_file_index, repo_status_file_index)

            logger.info('repo: %s, update repo file index success', repo_id)
            return True

        except RequestException:
            raise
        except Exception as e:
            logger.exception('repo_id: %s, update repo file index error: %s.', repo_id, e)
            return False

    def update_library_file_descriptions(self, repo_id, row_ids, repo_file_index):
        if not need_index_metadata_info(repo_id, self.session):
            return
        index_name = REPO_FILE_INDEX_PREFIX + repo_id
        if not repo_file_index.check_index(index_name):
            return
        row_ids = list(row_ids)
        found = 0
        for start in range(0, len(row_ids), 100):
            rows = get_metadata_by_row_ids(repo_id, row_ids[start:start + 100], self.metadata_server_api)
            found += len(rows)
            repo_file_index.update_descriptions(index_name, rows)
        logger.info('SeaSearch description rows queried: repo=%s requested=%s found=%s', repo_id, len(row_ids), found)

    def compensate_library_descriptions(self, repo_id, repo_file_index, repo_status_file_index):
        if not need_index_metadata_info(repo_id, self.session):
            return
        query_time = time.time()
        status = repo_status_file_index.get_repo_status_by_id(repo_id)
        # _mtime has second precision. Overlap the boundary to avoid missing same-second edits.
        since = max(0, float(status.metadata_updated_time or 0) - 1)
        last_update = timestamp_to_isoformat_timestr(since)
        sql = (f"SELECT `_id`, `_description`, `_parent_dir`, `_name`, `_is_dir` "
               f"FROM `{METADATA_TABLE.name}` WHERE `_is_dir` = False "
               f"AND `_mtime` >= '{last_update}' ORDER BY `_id`")
        rows = query_metadata_rows(repo_id, self.metadata_server_api, sql)
        logger.info('SeaSearch description compensation queried: repo=%s since=%s rows=%s', repo_id, since, len(rows))
        if not repo_file_index.update_descriptions(REPO_FILE_INDEX_PREFIX + repo_id, rows):
            raise RuntimeError('Description documents not yet indexed')
        repo_status_file_index.add_repo_status(
            repo_id, status.from_commit, status.to_commit, query_time)
        logger.info('SeaSearch description watermark advanced: repo=%s from_time=%s to_time=%s',
                    repo_id, status.metadata_updated_time, query_time)

    def has_description_changes(self, repo_id, metadata_updated_time):
        """Read-only probe; only the writing worker may advance the watermark."""
        if not need_index_metadata_info(repo_id, self.session):
            return False
        since = max(0, float(metadata_updated_time or 0) - 1)
        last_update = timestamp_to_isoformat_timestr(since)
        sql = (f"SELECT `_id` FROM `{METADATA_TABLE.name}` WHERE `_is_dir` = False "
               f"AND `_mtime` >= '{last_update}' LIMIT 1")
        return bool(self.metadata_server_api.query_rows(repo_id, sql, []).get('results'))

    def delete_repo_file_index(self, repo_id, repo_file_index, repo_status_file_index):
        # first delete repo_file_index
        repo_file_index_name = REPO_FILE_INDEX_PREFIX + repo_id
        repo_file_index.delete_index_by_index_name(repo_file_index_name)
        repo_status_file_index.delete_documents_by_repo(repo_id)

    def file_search(self, query, repos, repo_file_index, count, suffixes, search_path, obj_type, time_range, size_range, search_filename_only):
        return repo_file_index.search_files(repos, query, 0, count, suffixes, search_path, obj_type, time_range, size_range, search_filename_only)

    def delete_wiki_index(self, wiki_id, wiki_index, wiki_status_index):
        # first delete wiki_index
        wiki_index_name = WIKI_INDEX_PREFIX + wiki_id
        wiki_index.delete_index_by_index_name(wiki_index_name)
        wiki_status_index.delete_documents_by_repo(wiki_id)

    def search_wikis(self, query, wiki_ids, wiki_index, count):
        return wiki_index.search_wikis(wiki_ids, query, 0, count)

    def update_wiki_index(self, wiki_id, commit_id, wiki_index, wiki_status_index):
        try:
            new_commit_id = commit_id
            index_name = WIKI_INDEX_PREFIX + wiki_id

            index_exists = wiki_index.check_index(index_name)
            wiki_index.create_index_if_missing(index_name)

            wiki_status = wiki_status_index.get_repo_status_by_id(wiki_id)
            from_commit = wiki_status.from_commit
            to_commit = wiki_status.to_commit
            if not index_exists:
                from_commit, to_commit = None, None

            recovering = index_exists and wiki_status.need_recovery()
            if new_commit_id == from_commit and not recovering:
                logger.info('SeaSearch wiki content unchanged: repo=%s commit=%s', wiki_id, new_commit_id)
                return True

            if not from_commit:
                commit_id = ZERO_OBJ_ID
            else:
                commit_id = from_commit

            if recovering:
                logger.warning('%s: wiki index inrecovery', wiki_id)
                wiki_index.update(index_name, wiki_id, commit_id, to_commit)
                commit_id = to_commit
            wiki_status_index.begin_update_repo(wiki_id, commit_id, new_commit_id)
            logger.info('SeaSearch wiki index updating: repo=%s from_commit=%s to_commit=%s recovery=%s rebuild=%s',
                        wiki_id, commit_id, new_commit_id, recovering, not index_exists)
            wiki_index.update(index_name, wiki_id, commit_id, new_commit_id)
            wiki_status_index.finish_update_repo(wiki_id, new_commit_id)

            logger.info('wiki: %s, update wiki index success', wiki_id)
            return True

        except RequestException:
            raise
        except Exception as e:
            logger.exception('wiki_id: %s, update wiki index error: %s.', wiki_id, e)
            return False
