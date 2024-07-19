import logging
from threading import Thread

from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.gevent import GeventScheduler
from seafevents.semantic_search.index_store.index_manager import IndexManager
from seafevents.semantic_search.index_store.repo_file_name_index import RepoFileNameIndex
from seafevents.semantic_search.index_store.repo_status_index import RepoStatusIndex
from seafevents.semantic_search.utils.constants import REPO_STATUS_FILENAME_INDEX_NAME
from seafevents.semantic_search.utils.seasearch_api import SeaSearchAPI
from seafevents.repo_data import repo_data


logger = logging.getLogger(__name__)


class RepoFilenameIndexUpdater(object):
    def __init__(self, config):
        self.seasearch_api = SeaSearchAPI(
            config['SEMANTIC_SEARCH']['seasearch_url'],
            config['SEMANTIC_SEARCH']['seasearch_token'],
        )
        self._repo_data = repo_data
        self._repo_status_filename_index = RepoStatusIndex(
            self.seasearch_api, REPO_STATUS_FILENAME_INDEX_NAME
        )
        self._repo_filename_index = RepoFileNameIndex(
            self.seasearch_api,
            self._repo_data,
            int(config['SEMANTIC_SEARCH']['seasearch_shard_num']),
        )
        self._index_manager = IndexManager(config)

    def start(self):
        RepoFilenameIndexUpdaterTimer(
            self._repo_status_filename_index,
            self._repo_filename_index,
            self._index_manager,
            self._repo_data,
        ).start()


def clear_deleted_repo(repo_status_filename_index, repo_filename_index, index_manager, repos):
    logger.info("start to clear filename index deleted repo")

    repo_list = repo_status_filename_index.get_all_repos_from_index()
    repo_all = [e.get('repo_id') for e in repo_list]

    repo_deleted = set(repo_all) - set(repos)

    logger.info("filename index %d repos need to be deleted." % len(repo_deleted))
    for repo_id in repo_deleted:
        index_manager.delete_repo_filename_index(repo_id, repo_filename_index, repo_status_filename_index)
        logger.info('Repo %s has been deleted from filename index.' % repo_id)
    logger.info("filename index deleted repo has been cleared")


def update_repo_file_name_indexes(repo_status_filename_index, repo_filename_index, index_manager, repo_data):
    start, count = 0, 1000
    all_repos = []
    while True:
        try:
            repo_commits = repo_data.get_repo_id_commit_id(start, count)
        except Exception as e:
            logger.error("Error: %s" % e)
            return
        start += 1000

        if len(repo_commits) == 0:
            break

        for repo_id, commit_id in repo_commits:
            all_repos.append(repo_id)

            index_manager.update_library_filename_index(repo_id, commit_id, repo_filename_index, repo_status_filename_index)

    logger.info("Finish update filename index")

    clear_deleted_repo(repo_status_filename_index, repo_filename_index, index_manager, all_repos)


class RepoFilenameIndexUpdaterTimer(Thread):
    def __init__(self, repo_status_filename_index, repo_filename_index, index_manager, repo_data):
        super(RepoFilenameIndexUpdaterTimer, self).__init__()
        self.repo_status_filename_index = repo_status_filename_index
        self.repo_filename_index = repo_filename_index
        self.index_manager = index_manager
        self.repo_data = repo_data

    def run(self):
        sched = GeventScheduler()
        logging.info('Start to update filename index...')
        try:
            sched.add_job(update_repo_file_name_indexes, CronTrigger(minute='*/5'),
                          args=(self.repo_status_filename_index, self.repo_filename_index, self.index_manager, self.repo_data))
        except Exception as e:
            logging.exception('periodical update filename index error: %s', e)

        sched.start()
