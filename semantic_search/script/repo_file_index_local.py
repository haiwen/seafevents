import os
import sys
import time
import queue
import logging
import argparse
import threading

from seafobj import commit_mgr, fs_mgr, block_mgr

import config
from seafevents.semantic_search.utils import init_logging
from seafevents.repo_data import repo_data
from seafevents.semantic_search.index_store.index_manager import IndexManager
from seafevents.semantic_search.utils.seasearch_api import SeaSearchAPI
from seafevents.semantic_search.index_store.repo_status_index import RepoStatusIndex
from seafevents.semantic_search.index_store.repo_file_index import RepoFileIndex
from seafevents.semantic_search.utils.constants import REPO_STATUS_FILE_INDEX_NAME
from seafevents.semantic_search.utils.sea_embedding_api import SeaEmbeddingAPI


MAX_ERRORS_ALLOWED = 1000
logger = logging.getLogger('semantic_search')

UPDATE_FILE_LOCK = os.path.join(os.path.dirname(__file__), 'update.lock')
lockfile = None
NO_TASKS = False


class RepoFileIndexLocal(object):
    """ Independent update repo file index.
    """
    def __init__(self, index_manager, repo_status_index, repo_file_index, embedding_api, repo_data, workers=3):
        self.index_manager = index_manager
        self.repo_status_index = repo_status_index
        self.repo_file_index = repo_file_index
        self.embedding_api = embedding_api
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
        repos_queue = queue.Queue(0)
        for i in range(self.workers):
            thread_name = "worker" + str(i)
            logger.info("starting %s worker threads for repo file indexing"
                        % thread_name)
            t = threading.Thread(target=self.thread_task, args=(repos_queue, ), name=thread_name)
            t.start()
            self.worker_list.append(t)

        start, per_size = 0, 1000
        need_deleted_index_repos = []
        while True:
            global NO_TASKS
            try:
                index_repos = list(self.index_manager.get_index_repos_by_size(start, per_size))
            except Exception as e:
                logger.error("Error: %s" % e)
                NO_TASKS = True
                self.clear_worker()
                break
            else:
                if len(index_repos) == 0:
                    NO_TASKS = True
                    break

                for index_repo in index_repos:
                    repo_id = index_repo[0]
                    commit_id = self.repo_data.get_repo_head_commit(repo_id)
                    if not commit_id:
                        # repo has deleted, delete repo index
                        need_deleted_index_repos.append(repo_id)
                        continue
                    repos_queue.put((repo_id, commit_id))

                start += per_size

        self.clear_worker()
        logger.info("repo file index updated, total time %s seconds" % str(time.time() - time_start))
        try:
            self.clear_deleted_repo(need_deleted_index_repos)
        except Exception as e:
            logger.exception('Delete Repo Error: %s' % e)
            self.incr_error()

    def thread_task(self, repos_queue):
        while True:
            try:
                queue_data = repos_queue.get(False)
            except queue.Empty:
                if NO_TASKS:
                    logger.debug(
                        "Queue is empty, %s worker threads stop"
                        % (threading.currentThread().getName())
                    )
                    break
                else:
                    time.sleep(2)
            else:
                repo_id = queue_data[0]
                commit_id = queue_data[1]
                try:
                    self.index_manager.create_library_sdoc_index(repo_id, self.embedding_api, self.repo_file_index, self.repo_status_index, commit_id)
                except Exception as e:
                    logger.exception('Repo file index error: %s, repo_id: %s' % (e, repo_id), exc_info=True)
                    self.incr_error()

        logger.info(
            "%s worker updated at %s time" 
            % (threading.currentThread().getName(),
               time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time())))
        )
        logger.info(
            "%s worker get %s error"
            % (threading.currentThread().getName(),
                str(self.error_counter))
        )

    def clear_deleted_repo(self, repos):
        logger.info("start to clear deleted repo")
        logger.info("%d repos need to be deleted." % len(repos))

        for repo_id in repos:
            self.delete_repo(repo_id)
            logger.info('Repo %s has been deleted from index.' % repo_id)
        logger.info("deleted repo has been cleared")

    def incr_error(self):
        self.error_counter += 1

    def delete_repo(self, repo_id):
        if len(repo_id) != 36:
            return
        self.index_manager.delete_index_repo_db(repo_id)


def start_index_local():
    if not check_concurrent_update():
        return

    seasearch_api = SeaSearchAPI(config.SEASEARCH_SERVER, config.SEASEARCH_TOKEN)
    index_manager = IndexManager()
    repo_status_index = RepoStatusIndex(seasearch_api, REPO_STATUS_FILE_INDEX_NAME)
    repo_file_index = RepoFileIndex(seasearch_api)

    embedding_api = SeaEmbeddingAPI(config.APP_NAME, config.SEA_EMBEDDING_SERVER)
    workers = config.INDEX_MANAGER_WORKERS

    try:
        index_local = RepoFileIndexLocal(index_manager, repo_status_index, repo_file_index, embedding_api, repo_data, workers)
    except Exception as e:
        logger.error("Index repo file process init error: %s." % e)
        return

    logger.info("Index repo file process initialized.")
    index_local.run()

    logger.info('\n\nRepo file index updated, statistic report:\n')
    logger.info('[commit read] %s', commit_mgr.read_count())
    logger.info('[dir read]    %s', fs_mgr.dir_read_count())
    logger.info('[file read]   %s', fs_mgr.file_read_count())
    logger.info('[block read]  %s', block_mgr.read_count())


def delete_indices():
    seasearch_api = SeaSearchAPI(config.SEASEARCH_SERVER, config.SEASEARCH_TOKEN)
    repo_status_index = RepoStatusIndex(seasearch_api, REPO_STATUS_FILE_INDEX_NAME)
    repo_file_index = RepoFileIndex(seasearch_api)
    index_manager = IndexManager()

    start, per_size = 0, 1000
    while True:
        index_repos = list(index_manager.get_index_repos_by_size(start, per_size))

        if len(index_repos) == 0:
            break

        for index_repo in index_repos:
            repo_file_index.delete_index_by_index_name(index_repo[0])
        start += per_size

    repo_status_index.delete_index_by_index_name()


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
    parser_update = subparsers.add_parser('update', help='update repo file index')
    parser_update.set_defaults(func=start_index_local)

    # clear
    parser_clear = subparsers.add_parser('clear', help='clear all repo file index')
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
    from seafevents.semantic_search import portalocker
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
