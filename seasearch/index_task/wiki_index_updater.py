import logging
from threading import Thread

from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.gevent import GeventScheduler
from seafevents.seasearch.index_store.index_manager import IndexManager
from seafevents.seasearch.index_store.wiki_index import WikiIndex
from seafevents.seasearch.index_store.repo_status_index import RepoStatusIndex
from seafevents.seasearch.utils.constants import WIKI_STATUS_INDEX_NAME, WIKI_INDEX_PREFIX, SHARD_NUM
from seafevents.seasearch.utils.seasearch_api import SeaSearchAPI
from seafevents.repo_data import repo_data
from seafevents.utils import parse_bool, get_opt_from_conf_or_env


logger = logging.getLogger(__name__)


class WikiIndexUpdater(object):
    def __init__(self, config):
        self._enabled = False

        self.seasearch_api = None
        self._repo_data = None
        self._wiki_status_index = None
        self._wiki_index = None
        self._index_manager = None
        self._parse_config(config)

    def _parse_config(self, config):
        """Parse wiki index update related parts of events.conf"""
        section_name = 'SEASEARCH'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            return

        # [ enabled ]
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)
        if not enabled:
            return
        self._enabled = True

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
        self._wiki_status_index = RepoStatusIndex(
            self.seasearch_api, WIKI_STATUS_INDEX_NAME
        )
        self._wiki_index = WikiIndex(
            self.seasearch_api,
            self._repo_data,
            int(SHARD_NUM),
        )
        self._index_manager = IndexManager()

    def is_enabled(self):
        return self._enabled

    def start(self):
        if not self.is_enabled():
            return

        WikiIndexUpdaterTimer(
            self._wiki_status_index,
            self._wiki_index,
            self._index_manager,
            self._repo_data,
        ).start()


def clear_deleted_wiki(wiki_status_index, wiki_index, index_manager, wikis):
    logger.info("start to clear deleted wiki index")

    wiki_list = wiki_status_index.get_all_repos_from_index()
    wiki_all = [e.get('repo_id') for e in wiki_list]

    wiki_deleted = set(wiki_all) - set(wikis)

    logger.info("wiki index %d need to be deleted." % len(wiki_deleted))
    for wiki_id in wiki_deleted:
        index_manager.delete_wiki_index(wiki_id, wiki_index, wiki_status_index)
        logger.info('Wiki %s has been deleted from wiki index.' % wiki_id)
    logger.info("wiki index deleted wiki has been cleared")


def update_wiki_indexes(wiki_status_index, wiki_index, index_manager, repo_data):
    start, count = 0, 50
    all_wikis = []
    while True:
        try:
            wiki_commits = repo_data.get_wiki_id_commit_id(start, count)
        except Exception as e:
            logger.error("Error: %s" % e)
            return
        start += 1000
        if len(wiki_commits) == 0:
            break

        for wiki_id, commit_id in wiki_commits:
            all_wikis.append(wiki_id)

            index_manager.update_wiki_index(wiki_id, commit_id, wiki_index, wiki_status_index)

    logger.info("Finish update wiki index")

    clear_deleted_wiki(wiki_status_index, wiki_index, index_manager, all_wikis)


class WikiIndexUpdaterTimer(Thread):
    def __init__(self, wiki_status_index, wiki_index, index_manager, repo_data):
        super(WikiIndexUpdaterTimer, self).__init__()
        self.wiki_status_index = wiki_status_index
        self.wiki_index = wiki_index
        self.index_manager = index_manager
        self.repo_data = repo_data

    def run(self):
        sched = GeventScheduler()
        logging.info('Start to update wiki index...')
        try:
            sched.add_job(update_wiki_indexes, CronTrigger(minute='*/15'),
                          args=(self.wiki_status_index, self.wiki_index, self.index_manager, self.repo_data))
        except Exception as e:
            logging.exception('periodical update wiki index error: %s', e)

        sched.start()
