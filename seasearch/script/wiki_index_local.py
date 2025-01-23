import os
import sys
import time
import queue
import logging
import argparse
import threading

from seafobj import commit_mgr, fs_mgr, block_mgr
from seafevents.utils import get_opt_from_conf_or_env
from seafevents.app.config import get_config
from seafevents.seasearch.utils import init_logging
from seafevents.repo_data import repo_data
from seafevents.seasearch.index_store.index_manager import IndexManager
from seafevents.seasearch.utils.seasearch_api import SeaSearchAPI
from seafevents.seasearch.index_store.wiki_status_index import WikiStatusIndex
from seafevents.seasearch.utils.constants import WIKI_INDEX_PREFIX, WIKI_STATUS_INDEX_NAME
from seafevents.seasearch.index_store.wiki_index import WikiIndex

logger = logging.getLogger('seasearch')

UPDATE_FILE_LOCK = os.path.join(os.path.dirname(__file__), 'update.lock')
lockfile = None
NO_TASKS = False


class WikiIndexLocal(object):
    """ Independent update wiki page index.
    """
    def __init__(self, index_manager, wiki_status_index, wiki_index, repo_data, workers=3):
        self.index_manager = index_manager
        self.wiki_status_index = wiki_status_index
        self.wiki_index = wiki_index
        self.repo_data = repo_data
        self.error_counter = 0
        self.worker_list = []
        self.workers = workers

    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All worker threads has stopped.")

    def run(self):
        time_start = time.time()
        wikis_queue = queue.Queue(0)
        for i in range(self.workers):
            thread_name = "worker" + str(i)
            logger.info("starting %s worker threads for wiki indexing"
                        % thread_name)
            t = threading.Thread(target=self.thread_task, args=(wikis_queue, ), name=thread_name)
            t.start()
            self.worker_list.append(t)

        start, per_size = 0, 1000
        wikis = {}
        while True:
            global NO_TASKS
            try:
                repo_commits = self.repo_data.get_wiki_repo_id_commit_id(start, per_size)
            except Exception as e:
                logger.error("Error: %s" % e)
                NO_TASKS = True
                self.clear_worker()
                return
            else:
                if len(repo_commits) == 0:
                    NO_TASKS = True
                    break
                for repo_id, commit_id, repo_type in repo_commits:
                    wikis_queue.put((repo_id, commit_id))
                    wikis[repo_id] = commit_id
                start += per_size

        self.clear_worker()
        logger.info("wiki index updated, total time %s seconds" % str(time.time() - time_start))
        try:
            self.clear_deleted_wiki(list(wikis.keys()))
        except Exception as e:
            logger.exception('Delete Wiki Error: %s' % e)
            self.incr_error()

    def thread_task(self, wikis_queue):
        while True:
            try:
                queue_data = wikis_queue.get(False)
            except queue.Empty:
                if NO_TASKS:
                    logger.debug(
                        "Queue is empty, %s worker threads stop"
                        % (threading.current_thread().name)
                    )
                    break
                else:
                    time.sleep(2)
            else:
                wiki_id = queue_data[0]
                commit_id = queue_data[1]

                try:
                    self.index_manager.update_wiki_index(wiki_id, commit_id, self.wiki_index, self.wiki_status_index)
                except Exception as e:
                    logger.exception('Wiki index error: %s, wiki_id: %s' % (e, wiki_id), exc_info=True)
                    self.incr_error()

        logger.info(
            "%s worker updated at %s time"
            % (threading.current_thread().name,
               time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time())))
        )
        logger.info(
            "%s worker get %s error"
            % (threading.current_thread().name,
                str(self.error_counter))
        )

    def clear_deleted_wiki(self, wikis):
        logger.info("start to clear deleted wiki")
        wiki_all = [e.get('repo_id') for e in self.wiki_status_index.get_all_repos_from_index()]

        wiki_deleted = set(wiki_all) - set(wikis)
        logger.info("wiki index %d need to be deleted." % len(wiki_deleted))
        for wiki_id in wiki_deleted:
            self.delete_wiki_index(wiki_id)
            logger.info('Wiki %s has been deleted from index.' % wiki_id)
        logger.info("deleted wiki has been cleared")

    def incr_error(self):
        self.error_counter += 1

    def delete_wiki_index(self, wiki_id):
        if len(wiki_id) != 36:
            return
        self.index_manager.delete_wiki_index(wiki_id, self.wiki_index, self.wiki_status_index)


def start_index_local():
    if not check_concurrent_update():
        return
    section_name = 'SEASEARCH'
    seafevents_conf = os.environ.get('EVENTS_CONFIG_FILE')
    config = get_config(seafevents_conf)
    seasearch_url = get_opt_from_conf_or_env(
        config, section_name, 'seasearch_url'
    )
    seasearch_token = get_opt_from_conf_or_env(
        config, section_name, 'seasearch_token'
    )
    wiki_size_limit = get_opt_from_conf_or_env(
        config, section_name, 'wiki_file_size_limit', default=int(5)
    )
    index_manager = IndexManager(config)
    seasearch_api = SeaSearchAPI(seasearch_url, seasearch_token)
    wiki_status_index = WikiStatusIndex(seasearch_api, WIKI_STATUS_INDEX_NAME)
    wiki_index = WikiIndex(
        seasearch_api,
        repo_data,
        shard_num=1,
        wiki_file_size_limit=int(wiki_size_limit) * 1024 * 1024,
    )

    try:
        index_local = WikiIndexLocal(index_manager, wiki_status_index, wiki_index, repo_data)
    except Exception as e:
        logger.error("Index wiki process init error: %s." % e)
        return

    logger.info("Index wiki process initialized.")
    index_local.run()

    logger.info('\n\nWiki index updated, statistic report:\n')
    logger.info('[commit read] %s', commit_mgr.read_count())
    logger.info('[dir read]    %s', fs_mgr.dir_read_count())
    logger.info('[file read]   %s', fs_mgr.file_read_count())
    logger.info('[block read]  %s', block_mgr.read_count())


def delete_indices():
    section_name = 'SEASEARCH'
    seafevents_conf = os.environ.get('EVENTS_CONFIG_FILE')
    config = get_config(seafevents_conf)
    seasearch_url = get_opt_from_conf_or_env(
        config, section_name, 'seasearch_url'
    )
    seasearch_token = get_opt_from_conf_or_env(
        config, section_name, 'seasearch_token'
    )

    seasearch_api = SeaSearchAPI(seasearch_url, seasearch_token)
    wiki_status_index = WikiStatusIndex(seasearch_api, WIKI_STATUS_INDEX_NAME)
    wiki_index = WikiIndex(seasearch_api, repo_data, shard_num=1)

    start, count = 0, 1000
    while True:
        try:
            repo_commits = repo_data.get_wiki_repo_id_commit_id(start, count)
        except Exception as e:
            logger.error("Error: %s" % e)
            return
        start += 1000

        if len(repo_commits) == 0:
            break

        for repo_id, commit_id, repo_type in repo_commits:
            wiki_index_name = WIKI_INDEX_PREFIX + repo_id
            wiki_index.delete_index_by_index_name(wiki_index_name)

    wiki_status_index.delete_index_by_index_name()


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands', description='')

    parser.add_argument(
        '--logfile',
        default=sys.stdout,
        type=argparse.FileType('a'),
        help='log file')

    parser.add_argument(
        '--loglevel',
        default='info',
        help='log level')

    # update index
    parser_update = subparsers.add_parser('update', help='update seafile wiki index')
    parser_update.set_defaults(func=start_index_local)

    # clear
    parser_clear = subparsers.add_parser('clear', help='clear all wiki index')
    parser_clear.set_defaults(func=delete_indices)

    if len(sys.argv) == 1:
        print(parser.format_help())
        return

    args = parser.parse_args()
    init_logging(args)

    logger.info('storage: using ' + commit_mgr.get_backend_name())

    args.func()


def do_lock(fn):
    if os.name == 'nt':
        return do_lock_win32(fn)
    else:
        return do_lock_linux(fn)


def do_lock_win32(fn):
    import ctypes

    CreateFileW = ctypes.windll.kernel32.CreateFileW
    GENERIC_WRITE = 0x40000000
    OPEN_ALWAYS = 4

    def lock_file(path):
        lock_file_handle = CreateFileW(path, GENERIC_WRITE, 0, None, OPEN_ALWAYS, 0, None)

        return lock_file_handle

    global lockfile

    lockfile = lock_file(fn)

    return lockfile != -1


def do_lock_linux(fn):
    from seafevents.seasearch.script import portalocker
    global lockfile
    lockfile = open(fn, 'w')
    try:
        portalocker.lock(lockfile, portalocker.LOCK_NB | portalocker.LOCK_EX)
        return True
    except portalocker.LockException:
        return False


def check_concurrent_update():
    """Use a lock file to ensure only one task can be running"""
    if not do_lock(UPDATE_FILE_LOCK):
        logger.error('another index task is running, quit now')
        return False

    return True


if __name__ == "__main__":
    main()
