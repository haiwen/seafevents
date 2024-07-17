import logging
import queue
import uuid
from datetime import datetime
from threading import Thread, Lock

from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.gevent import GeventScheduler

from seafevents.semantic_search.index_store.index_manager import IndexManager
from seafevents.semantic_search.index_store.repo_status_index import RepoStatusIndex
from seafevents.semantic_search.index_store.repo_file_index import RepoFileIndex
from seafevents.semantic_search.index_store.repo_file_name_index import RepoFileNameIndex
from seafevents.semantic_search.utils.seasearch_api import SeaSearchAPI
from seafevents.semantic_search.utils.sea_embedding_api import SeaEmbeddingAPI
from seafevents.semantic_search.utils.constants import REPO_STATUS_FILE_INDEX_NAME
from seafevents.repo_data import repo_data
from seafevents.semantic_search import config

logger = logging.getLogger(__name__)


class IndexTask:

    def __init__(self, task_id, readable_id, func, args):
        self.id = task_id
        self.readable_id = readable_id
        self.func = func
        self.args = args

        self.status = 'init'

        self.started_at = None
        self.finished_at = None

        self.result = None
        self.error = None

    @staticmethod
    def get_readable_id(readable_id):
        return readable_id

    def run(self):
        self.status = 'running'
        self.started_at = datetime.now()
        return self.func(*self.args)

    def set_result(self, result):
        self.result = result
        self.status = 'success'
        self.finished_at = datetime.now()

    def set_error(self, error):
        self.error = error
        self.status = 'error'
        self.finished_at = datetime.now()

    def is_finished(self):
        return self.status in ['error', 'success']

    def get_cost_time(self):
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).seconds
        return None

    def get_info(self):
        return f'{self.id}--{self.readable_id}--{self.func}'

    def __str__(self):
        return f'<IndexTask {self.id} {self.readable_id} {self.func.__name__} {self.status}>'


class IndexTaskManager:

    def __init__(self):
        self.tasks_queue = queue.Queue()
        self.tasks_map = {}             # {task_id: task} all tasks
        self.readable_id2task_map = {}  # {task_readable_id: task} in queue or running
        self.check_task_lock = Lock()   # lock access to readable_id2task_map
        self.sched = GeventScheduler()
        self.app = None
        self.conf = {
            'workers': config.INDEX_MANAGER_WORKERS,
            'expire_time': config.INDEX_TASK_EXPIRE_TIME
        }
        self.sched.add_job(self.clear_expired_tasks, CronTrigger(minute='*/10'))
        self.sched.add_job(self.cron_update_library_sdoc_indexes, CronTrigger(hour='*'))
        self.index_manager = None
        self.repo_file_index = None

    def init(self):
        self.index_manager = IndexManager()
        self.seasearch_api = SeaSearchAPI(config.SEASEARCH_SERVER, config.SEASEARCH_TOKEN)
        self.repo_data = repo_data
        self.embedding_api = SeaEmbeddingAPI(config.SEA_EMBEDDING_SERVER)
        # for semantic search
        self.repo_status_index = RepoStatusIndex(self.seasearch_api, REPO_STATUS_FILE_INDEX_NAME)
        self.repo_file_index = RepoFileIndex(self.seasearch_api)
        # for keyword search
        self.repo_filename_index = RepoFileNameIndex(self.seasearch_api, self.repo_data)

    def get_pending_or_running_task(self, readable_id):
        task = self.readable_id2task_map.get(readable_id)
        return task

    def add_library_sdoc_index_task(self, repo_id, commit_id):
        readable_id = repo_id
        with self.check_task_lock:
            task = self.get_pending_or_running_task(readable_id)
            if task:
                return task.id

            task_id = str(uuid.uuid4())
            task = IndexTask(task_id, readable_id, self.index_manager.create_library_sdoc_index,
                             (repo_id, self.embedding_api, self.repo_file_index, self.repo_status_index, commit_id)
                             )

            self.tasks_map[task_id] = task
            self.readable_id2task_map[task.readable_id] = task

            self.tasks_queue.put(task)
            return task_id

    def keyword_search(self, query, repos, count, suffixes):
        return self.index_manager.keyword_search(query, repos, self.repo_filename_index, count, suffixes)

    def hybrid_search(self, query, repo, count):
        return self.index_manager.hybrid_search(query, repo, self.repo_filename_index,
                                                    self.embedding_api, self.repo_file_index, count)

    def add_update_a_library_sdoc_index_task(self, repo_id, commit_id):
        readable_id = repo_id
        with self.check_task_lock:
            task = self.get_pending_or_running_task(readable_id)
            if task:
                return task.id

            task_id = str(uuid.uuid4())
            task = IndexTask(task_id, readable_id, self.index_manager.update_library_sdoc_index,
                             (repo_id, self.embedding_api, self.repo_file_index, self.repo_status_index,
                              commit_id)
                             )
            self.tasks_map[task_id] = task
            self.readable_id2task_map[task.readable_id] = task
            self.tasks_queue.put(task)

            return task_id

    def update_library_sdoc_indexes(self):
        index_repos = self.index_manager.list_index_repos()
        for repo in index_repos:
            repo_id = repo[0]
            commit_id = self.repo_data.get_repo_head_commit(repo_id)
            self.add_update_a_library_sdoc_index_task(repo_id, commit_id)

    def cron_update_library_sdoc_indexes(self):
        """
            update library sdoc indexes periodly
            query tasks and add them to queue by calling self.add_update_a_library_sdoc_index_task
        """

        try:
            self.update_library_sdoc_indexes()
        except Exception as e:
            logger.exception('periodical update library sdoc indexes error: %s', e)

    def query_task(self, task_id):
        return self.tasks_map.get(task_id)

    def handle_task(self):
        while True:
            try:
                task = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(e)
                continue

            try:
                task_info = task.get_info()
                logger.info('Run task: %s' % task_info)

                # run
                task.run()
                task.set_result('success')

                logger.info('Run task success: %s cost %ds \n' % (task_info, task.get_cost_time()))
            except Exception as e:
                task.set_error(e)
                logger.exception('Failed to handle task %s, error: %s \n' % (task.get_info(), e))
            finally:
                with self.check_task_lock:
                    self.readable_id2task_map.pop(task.readable_id, None)

    def start(self):
        thread_num = self.conf['workers']
        for i in range(thread_num):
            t_name = 'IndexTaskManager Thread-' + str(i)
            t = Thread(target=self.handle_task, name=t_name)
            t.setDaemon(True)
            t.start()
        self.sched.start()

    def clear_expired_tasks(self):
        """clear tasks finished for conf['expire_time'] in tasks_map

        when a task end, it will not be pop from tasks_map immediately,
        because this task might be responsible for multi-http-requests(not only one), that might query task status

        but task will not restored forever, so need to clear
        """
        expire_tasks = []
        for task in self.tasks_map.values():
            if not task.is_finished():
                continue
            if (datetime.now() - task.finished_at).seconds >= self.conf['expire_time']:
                expire_tasks.append(task)
        logger.info('expired tasks: %s', len(expire_tasks))
        for task in expire_tasks:
            self.tasks_map.pop(task.id, None)


index_task_manager = IndexTaskManager()
