import logging
import time
from threading import Thread, Event
from seafevents.seasearch.index_store.index_manager import IndexManager
from seafevents.seasearch.index_store.repo_file_name_index import RepoFileNameIndex
from seafevents.seasearch.index_store.repo_status_index import RepoStatusIndex
from seafevents.seasearch.utils.constants import REPO_STATUS_FILENAME_INDEX_NAME, SHARD_NUM, REPO_TYPE_WIKI
from seafevents.seasearch.utils.seasearch_api import SeaSearchAPI
from seafevents.repo_data import repo_data
from seafevents.utils import parse_bool, get_opt_from_conf_or_env, parse_interval


logger = logging.getLogger(__name__)


class RepoFilenameIndexUpdater(object):
    def __init__(self, config):
        self._enabled = False

        self.seasearch_api = None
        self._repo_data = None
        self._repo_status_filename_index = None
        self._repo_filename_index = None
        self._index_manager = None
        self._parse_config(config)

    def _parse_config(self, config):
        """Parse filename index update related parts of events.conf"""
        section_name = 'SEASEARCH'
        key_enabled = 'enabled'
        key_seasearch_url = 'seasearch_url'
        key_seasearch_token = 'seasearch_token'
        key_index_interval = 'interval'

        default_index_interval = 30 * 60 # 30 min

        if not config.has_section(section_name):
            return

        # [ enabled ]
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)
        if not enabled:
            return
        self._enabled = True

        seasearch_url = get_opt_from_conf_or_env(
            config, section_name, key_seasearch_url
        )
        seasearch_token = get_opt_from_conf_or_env(
            config, section_name, key_seasearch_token
        )
        interval = get_opt_from_conf_or_env(config, section_name, key_index_interval,
                                            default=default_index_interval)
        interval = parse_interval(interval, default_index_interval)

        self.seasearch_api = SeaSearchAPI(
            seasearch_url,
            seasearch_token,
        )
        self._repo_data = repo_data
        self._interval = interval
        self._repo_status_filename_index = RepoStatusIndex(
            self.seasearch_api, REPO_STATUS_FILENAME_INDEX_NAME
        )
        self._repo_filename_index = RepoFileNameIndex(
            self.seasearch_api,
            self._repo_data,
            int(SHARD_NUM),
        )
        self._index_manager = IndexManager(config)

    def is_enabled(self):
        return self._enabled

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start filename index updater: it is not enabled!')
            return

        logging.info('Start to update filename index, interval = %s sec', self._interval)
        RepoFilenameIndexUpdaterTimer(
            self._repo_status_filename_index,
            self._repo_filename_index,
            self._index_manager,
            self._repo_data,
            self._interval
        ).start()


def clear_deleted_repo(repo_status_filename_index, repo_filename_index, index_manager, repos):
    logger.info("start to clear filename index deleted repo")

    repo_list = repo_status_filename_index.get_all_repos_from_index()
    repo_all = [e.get('repo_id') for e in repo_list]

    repo_deleted = set(repo_all) - set(repos)

    logger.info("%d repos of filename index need to be deleted." % len(repo_deleted))
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

        metadata_query_time = time.time()
        repo_ids = [repo[0] for repo in repo_commits if repo[2] != REPO_TYPE_WIKI]
        virtual_repos = repo_data.get_virtual_repo_in_repos(repo_ids)
        virtual_repo_set = {repo[0] for repo in virtual_repos}

        for repo_id, commit_id, repo_type in repo_commits:
            if repo_id in virtual_repo_set or repo_type == REPO_TYPE_WIKI:
                continue
            all_repos.append(repo_id)

            index_manager.update_library_filename_index(repo_id, commit_id, repo_filename_index, repo_status_filename_index, metadata_query_time)

    logger.info("Finish updating filename index")

    clear_deleted_repo(repo_status_filename_index, repo_filename_index, index_manager, all_repos)


class RepoFilenameIndexUpdaterTimer(Thread):
    def __init__(self, repo_status_filename_index, repo_filename_index, index_manager, repo_data, interval):
        super(RepoFilenameIndexUpdaterTimer, self).__init__()
        self.repo_status_filename_index = repo_status_filename_index
        self.repo_filename_index = repo_filename_index
        self.index_manager = index_manager
        self.repo_data = repo_data
        self.interval = interval
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            if not self.finished.is_set():
                logging.info('starts to update filename index...')
                try:
                    update_repo_file_name_indexes(self.repo_status_filename_index, self.repo_filename_index, self.index_manager, self.repo_data)
                except Exception as e:
                    logging.exception('periodical update filename index error: %s', e)

    def cancel(self):
        self.finished.set()
