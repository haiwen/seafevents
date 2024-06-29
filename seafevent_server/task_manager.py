import logging
import queue
import uuid
from datetime import datetime
from threading import Thread, Lock

from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.gevent import GeventScheduler

from seafevents.db import init_db_session_class
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_metadata.repo_metadata import RepoMetadata
from seafevents.repo_metadata.metadata_manager import MetadataManager


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


class TaskManager:

    def __init__(self):
        self.tasks_queue = queue.Queue()
        self.tasks_map = {}             # {task_id: task} all tasks
        self.readable_id2task_map = {}  # {task_readable_id: task} in queue or running
        self.check_task_lock = Lock()   # lock access to readable_id2task_map
        self.sched = GeventScheduler()
        self.app = None
        self.conf = {
            'workers': 3,
            'expire_time': 30 * 60
        }
        self._db_session_class = None
        self._metadata_server_api = None
        self.repo_metadata = None
        self.metadata_manager = None

        self.sched.add_job(self.clear_expired_tasks, CronTrigger(minute='*/30'))

    def init(self, app, workers, task_expire_time, config):
        self.app = app
        self.conf['expire_time'] = task_expire_time
        self.conf['workers'] = workers

        self._db_session_class = init_db_session_class(config)
        self._metadata_server_api = MetadataServerAPI('seafevents')
        self.repo_metadata = RepoMetadata(self._metadata_server_api)
        self.metadata_manager = MetadataManager(self._db_session_class, self.repo_metadata)

    def get_pending_or_running_task(self, readable_id):
        task = self.readable_id2task_map.get(readable_id)
        return task

    def add_init_metadata_task(self, username, repo_id):

        readable_id = repo_id
        with self.check_task_lock:
            task = self.get_pending_or_running_task(readable_id)
            if task:
                return task.id

            task_id = str(uuid.uuid4())
            task = IndexTask(task_id, readable_id, self.metadata_manager.create_metadata,
                             (repo_id, )
                             )
            self.tasks_map[task_id] = task
            self.readable_id2task_map[task.readable_id] = task
            self.tasks_queue.put(task)

            return task_id

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

    def run(self):
        thread_num = self.conf['workers']
        for i in range(thread_num):
            t_name = 'TaskManager Thread-' + str(i)
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


task_manager = TaskManager()
