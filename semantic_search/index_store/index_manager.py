import logging
import time
import os
from datetime import datetime

from sqlalchemy.sql import text

from seafevents.app.config import get_config
from seafevents.db import init_db_session_class
from seafevents.semantic_search.utils.constants import ZERO_OBJ_ID, REPO_FILENAME_INDEX_PREFIX
from seafevents.semantic_search.index_store.models import IndexRepo
from seafevents.semantic_search.index_store.utils import rank_fusion, filter_hybrid_searched_files

logger = logging.getLogger(__name__)


class IndexManager():
    def __init__(self, retrieval_num):
        self.evtconf = os.environ['EVENTS_CONFIG_FILE']
        self._db_session_class = init_db_session_class(get_config(self.evtconf))
        self.retrieval_num = retrieval_num

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

    def create_library_sdoc_index(self, repo_id, embedding_api, repo_file_index, repo_status_index, commit_id):
        repo_status_index.begin_update_repo(repo_id, ZERO_OBJ_ID, commit_id)
        repo_file_index.create_index(repo_id)
        repo_file_index.add(repo_id, ZERO_OBJ_ID, commit_id, embedding_api)
        repo_status_index.finish_update_repo(repo_id, commit_id)

        logger.info('library: %s, save library file to SeaSearch success', repo_id)

    def search_children_in_library(self, query, repo, embedding_api, repo_file_index, count=20):
        return repo_file_index.search_files(repo, self.retrieval_num, embedding_api, query)[:count]

    def update_library_sdoc_index(self, repo_id, embedding_api, repo_file_index, repo_status_index, new_commit_id):
        try:
            repo_status = repo_status_index.get_repo_status_by_id(repo_id)

            from_commit = repo_status.from_commit
            to_commit = repo_status.to_commit

            if new_commit_id == from_commit:
                return

            commit_id = from_commit
            if repo_status.need_recovery():
                logger.warning('%s: repo file index inrecovery', repo_id)

                is_exist = repo_file_index.check_index(repo_id)
                if not is_exist:
                    repo_file_index.create_index(repo_id)

                repo_file_index.update(repo_id, from_commit, to_commit, embedding_api)

                # time sleep for SeaSearch save data
                time.sleep(1)

                commit_id = to_commit
            repo_status_index.begin_update_repo(repo_id, commit_id, new_commit_id)
            repo_file_index.update(repo_id, commit_id, new_commit_id, embedding_api)
            repo_status_index.finish_update_repo(repo_id, new_commit_id)

            self.update_index_repo_db(repo_id)

            logger.info('repo: %s, update repo file index success', repo_id)

        except Exception as e:
            logger.exception('repo_id: %s, update repo file index error: %s.', repo_id, e)

    def delete_library_sdoc_index_by_repo_id(self, repo_id, repo_file_index, repo_status_index):
        # first delete repo_file_index
        repo_file_index.delete_index_by_index_name(repo_id)
        repo_status_index.delete_documents_by_repo(repo_id)
        self.delete_index_repo_db(repo_id)

    def keyword_search(self, query, repos, repo_filename_index, count, suffixes=None):
        return repo_filename_index.search_files(repos, query, 0, count, suffixes)

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

    def hybrid_search(self, query, repo, repo_filename_index, embedding_api, repo_file_index, count):
        keyword_files = self.keyword_search(query, [repo], repo_filename_index, count)
        similar_files = self.search_children_in_library(query, repo, embedding_api, repo_file_index, count)
        fused_files = rank_fusion([keyword_files, similar_files])

        return filter_hybrid_searched_files(fused_files)[:count]
