import logging
import time
from datetime import datetime

from sqlalchemy.sql import text

from seafevents.semantic_search.utils.constants import ZERO_OBJ_ID, REPO_FILENAME_INDEX_PREFIX
from seafevents.db import init_db_session_class
from seafevents.semantic_search.index_store.models import IndexRepo

logger = logging.getLogger(__name__)


class IndexManager(object):
    def __init__(self, config):
        self._db_session_class = init_db_session_class(config)

    def create_index_repo_db(self, repo_id):
        with self._db_session_class() as db_session:
            index_repo = IndexRepo(repo_id, datetime.now(), datetime.now())
            db_session.add(index_repo)
            db_session.commit()

    def delete_index_repo_db(self, repo_id):
        with self._db_session_class() as db_session:
            db_session.query(IndexRepo).filter(IndexRepo.repo_id == repo_id).delete()
            db_session.commit()

    def update_index_repo_db(self, repo_id):
        with self._db_session_class() as db_session:
            index_repo = db_session.query(IndexRepo). \
                filter(IndexRepo.repo_id == repo_id)
            index_repo.update({"updated": datetime.now()})
            db_session.commit()

    def get_index_repo_by_repo_id(self, repo_id):
        with self._db_session_class() as db_session:
            return db_session.query(IndexRepo).filter(IndexRepo.repo_id == repo_id).first()

    def list_index_repos(self):
        with self._db_session_class() as db_session:
            sql = """
                    SELECT `repo_id` FROM index_repo
                    """

            index_repos = db_session.execute(text(sql))
            return index_repos

    def get_index_repos_by_size(self, start, size):
        with self._db_session_class() as db_session:
            sql = """
                    SELECT `repo_id`
                    FROM index_repo LIMIT :start, :size
                    """

            index_repos = db_session.execute(text(sql), {
                'start': start,
                'size': size,
            })
            return index_repos

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

    def keyword_search(self, query, repos, repo_filename_index, count, suffixes=None):
        return repo_filename_index.search_files(repos, query, 0, count, suffixes)
