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
from seafevents.seasearch.index_store.repo_status_index import RepoStatusIndex
from seafevents.seasearch.utils.constants import REPO_STATUS_FILENAME_INDEX_NAME, REPO_FILENAME_INDEX_PREFIX, REPO_TYPE_WIKI
from seafevents.seasearch.index_store.repo_file_name_index import RepoFileNameIndex

logger = logging.getLogger('seasearch')

UPDATE_FILE_LOCK = os.path.join(os.path.dirname(__file__), 'update.lock')
lockfile = None
NO_TASKS = False


class RepoFileNameIndexLocal(object):
    """ Independent update repo file name index.
    """
    def __init__(self, index_manager, repo_status_filename_index, repo_filename_index, repo_data, workers=3):
        self.index_manager = index_manager
        self.repo_status_filename_index = repo_status_filename_index
        self.repo_filename_index = repo_filename_index
        self.repo_data = repo_data
        self.error_counter = 0
        self.worker_list = []
        self.workers = workers

    # 清空工作线程
    def clear_worker(self):
        for th in self.worker_list:
            th.join()
        logger.info("All worker threads has stopped.")

    # 定义了一个名为 `run` 的方法，该方法似乎负责在多线程环境中更新仓库文件名索引。
    # 1. 它启动指定数量的工作线程（`self.workers`）来处理仓库数据。
    # 2. 它批量获取仓库提交数据（每批 1000 个，`per_size`），并处理每个批次。
    # 3. 对于每个仓库，它检查是否是虚拟仓库或 wiki 仓库，如果不是，则将其添加到处理队列中。
    # 4. 一旦处理完所有批次，它停止工作线程并更新仓库文件名索引。
    # 5. 最后，它尝试清除已删除的仓库，并记录在此过程中发生的任何错误。
    # 该代码似乎旨在通过将工作负载分配到多个线程来高效地更新大型仓库索引。
    def run(self):
        time_start = time.time()
        repos_queue = queue.Queue(0)
        for i in range(self.workers):
            thread_name = "worker" + str(i)
            logger.info("starting %s worker threads for repo filename indexing"
                        % thread_name)
            t = threading.Thread(target=self.thread_task, args=(repos_queue, ), name=thread_name)
            t.start()
            self.worker_list.append(t)

        start, per_size = 0, 1000
        repos = {}
        while True:
            global NO_TASKS
            try:
                repo_commits = self.repo_data.get_repo_id_commit_id(start, per_size)
            except Exception as e:
                logger.error("get repo id commit id failed, Error: %s" % e)
                NO_TASKS = True
                self.clear_worker()
                return
            else:
                if len(repo_commits) == 0:
                    NO_TASKS = True
                    break

                metadata_query_time = time.time()
                repo_ids = [repo[0] for repo in repo_commits if repo[2] != REPO_TYPE_WIKI]
                try:
                    virtual_repos = repo_data.get_virtual_repo_in_repos(repo_ids)
                except Exception as e:
                    logger.error("get virtual repo failed, Error: %s" % e)
                    NO_TASKS = True
                    self.clear_worker()
                    return
                virtual_repo_set = {repo[0] for repo in virtual_repos}

                for repo_id, commit_id, repo_type in repo_commits:
                    if repo_id in virtual_repo_set or repo_type == REPO_TYPE_WIKI:
                        continue
                    repos_queue.put((repo_id, commit_id, metadata_query_time))
                    repos[repo_id] = commit_id
                start += per_size

        self.clear_worker()
        logger.info("repo filename index updated, total time %s seconds" % str(time.time() - time_start))
        try:
            self.clear_deleted_repo(list(repos.keys()))
        except Exception as e:
            logger.exception('Delete Repo Error: %s' % e)
            self.incr_error()

    # 这个代码片段定义了一个方法 thread_task，它在一个工作线程中运行。
    # 它不断地检查一个队列 (repos_queue) 中是否有任务需要处理。
    # 每个任务都是一个包含仓库 ID、提交 ID 和元数据查询时间的元组。
    # 如果找到一个任务，它会尝试使用 index_manager 更新库文件名索引。
    # 如果队列为空且不再有任务需要处理 (NO_TASKS 为 True)，线程就会停止。
    # 如果在处理过程中发生错误，它会记录错误并增加错误计数器。
    # 在线程完成后，它会记录完成时间和遇到的任何错误。
    def thread_task(self, repos_queue):
        while True:
            try:
                queue_data = repos_queue.get(False)
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
                repo_id = queue_data[0]
                commit_id = queue_data[1]
                metadata_query_time = queue_data[2]
                try:
                    self.index_manager.update_library_filename_index(repo_id, commit_id, self.repo_filename_index, self.repo_status_filename_index, metadata_query_time)
                except Exception as e:
                    logger.exception('Repo filename index error: %s, repo_id: %s' % (e, repo_id), exc_info=True)
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

    def clear_deleted_repo(self, repos):
        logger.info("start to clear deleted repo")
        repo_all = [e.get('repo_id') for e in self.repo_status_filename_index.get_all_repos_from_index()]

        repo_deleted = set(repo_all) - set(repos)
        logger.info("%d repos need to be deleted." % len(repo_deleted))

        for repo_id in repo_deleted:
            self.delete_repo(repo_id)
            logger.info('Repo %s has been deleted from index.' % repo_id)
        logger.info("deleted repo has been cleared")

    def incr_error(self):
        self.error_counter += 1

    def delete_repo(self, repo_id):
        if len(repo_id) != 36:
            return
        self.index_manager.delete_repo_filename_index(repo_id, self.repo_filename_index, self.repo_status_filename_index)


# 初始化并运行一个本地的仓库文件名索引过程。它：

# 1. 检查是否已经有另一个更新正在运行 (`check_concurrent_update`)。
# 2. 从环境变量和配置文件中获取配置设置。
# 3. 创建 `IndexManager`、`SeaSearchAPI`、`RepoStatusIndex` 和 `RepoFileNameIndex` 的实例。
# 4. 用这些实例初始化一个 `RepoFileNameIndexLocal` 对象。
# 5. 运行索引过程 (`index_local.run()`)。
# 6. 记录更新过程的统计信息。
# 其目的是更新仓库文件名的搜索索引，以便高效地搜索仓库中的文件。
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

    index_manager = IndexManager(config)
    seasearch_api = SeaSearchAPI(seasearch_url, seasearch_token)
    repo_status_filename_index = RepoStatusIndex(seasearch_api, REPO_STATUS_FILENAME_INDEX_NAME)
    repo_filename_index = RepoFileNameIndex(seasearch_api, repo_data, shard_num=1)

    try:
        index_local = RepoFileNameIndexLocal(index_manager, repo_status_filename_index, repo_filename_index,repo_data)
    except Exception as e:
        logger.error("Index repo filename process init error: %s." % e)
        return

    logger.info("Index repo filename process initialized.")
    index_local.run()

    logger.info('\n\nRepo filename index updated, statistic report:\n')
    logger.info('[commit read] %s', commit_mgr.read_count())
    logger.info('[dir read]    %s', fs_mgr.dir_read_count())
    logger.info('[file read]   %s', fs_mgr.file_read_count())
    logger.info('[block read]  %s', block_mgr.read_count())


# 这个代码片段从搜索API中删除非wiki和非虚拟仓库的索引。它批量迭代仓库的提交，过滤掉wiki和虚拟仓库，并删除相应的文件名索引。最后，它删除状态文件名索引。
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
    repo_status_filename_index = RepoStatusIndex(seasearch_api, REPO_STATUS_FILENAME_INDEX_NAME)
    repo_filename_index = RepoFileNameIndex(seasearch_api, repo_data, shard_num=1)

    start, count = 0, 1000
    while True:
        try:
            repo_commits = repo_data.get_repo_id_commit_id(start, count)
        except Exception as e:
            logger.error("Error: %s" % e)
            return
        start += 1000

        if len(repo_commits) == 0:
            break

        repo_ids = [repo[0] for repo in repo_commits if repo[2] != REPO_TYPE_WIKI]
        virtual_repos = repo_data.get_virtual_repo_in_repos(repo_ids)
        virtual_repo_set = {repo[0] for repo in virtual_repos}

        for repo_id, commit_id, repo_type in repo_commits:
            if repo_id in virtual_repo_set or repo_type == REPO_TYPE_WIKI:
                continue
            repo_filename_index_name = REPO_FILENAME_INDEX_PREFIX + repo_id
            repo_filename_index.delete_index_by_index_name(repo_filename_index_name)

    repo_status_filename_index.delete_index_by_index_name()


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
    parser_update = subparsers.add_parser('update', help='update seafile repo filename index')
    parser_update.set_defaults(func=start_index_local)

    # clear
    parser_clear = subparsers.add_parser('clear', help='clear all repo filename index')
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
