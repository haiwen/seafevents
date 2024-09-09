import logging
import time

from seafevents.seasearch.utils.constants import ZERO_OBJ_ID, REPO_FILENAME_INDEX_PREFIX, REPO_IMAGE_INDEX_PREFIX

logger = logging.getLogger(__name__)


class IndexManager(object):

    def update_library_filename_index(self, repo_id, commit_id, repo_filename_index, repo_status_filename_index):
        try:
            new_commit_id = commit_id
            index_name = REPO_FILENAME_INDEX_PREFIX + repo_id

            repo_filename_index.create_index_if_missing(index_name)

            repo_status = repo_status_filename_index.get_repo_status_by_id(repo_id)
            from_commit = repo_status.from_commit
            to_commit = repo_status.to_commit

            if new_commit_id == from_commit:
                return

            if not from_commit:
                commit_id = ZERO_OBJ_ID
            else:
                commit_id = from_commit

            if repo_status.need_recovery():
                logger.warning('%s: repo filename index inrecovery', repo_id)
                repo_filename_index.update(index_name, repo_id, commit_id, to_commit)
                commit_id = to_commit
                time.sleep(1)

            repo_status_filename_index.begin_update_repo(repo_id, commit_id, new_commit_id)
            repo_filename_index.update(index_name, repo_id, commit_id, new_commit_id)
            repo_status_filename_index.finish_update_repo(repo_id, new_commit_id)

            logger.info('repo: %s, update repo filename index success', repo_id)

        except Exception as e:
            logger.exception('repo_id: %s, update repo filename index error: %s.', repo_id, e)

    def delete_repo_filename_index(self, repo_id, repo_filename_index, repo_status_filename_index):
        # first delete repo_file_index
        repo_filename_index_name = REPO_FILENAME_INDEX_PREFIX + repo_id
        repo_filename_index.delete_index_by_index_name(repo_filename_index_name)
        repo_status_filename_index.delete_documents_by_repo(repo_id)

    def delete_repo_image_index(self, repo_id, repo_image_index):
        repo_image_index_name = REPO_IMAGE_INDEX_PREFIX + repo_id
        repo_image_index.delete_index_by_index_name(repo_image_index_name)

    def keyword_search(self, query, repos, repo_filename_index, count, suffixes=None, search_path=None):
        return repo_filename_index.search_files(repos, query, 0, count, suffixes, search_path)

    def image_search(self, index_name, repo_id, obj_id, path, repo_image_index, count):
        return repo_image_index.image_search(index_name, repo_id, obj_id, path, count)
