import logging
import queue
from threading import Thread
from uuid import uuid4

from seafevents.seaf_io.ledger_tasks import export_ledger_to_excel
from seafevents.seaf_io.utils import Task, TaskError

logger = logging.getLogger(__name__)


class TaskManager:

    def __init__(self):
        self.tasks_queue = queue.Queue(10)
        self.tasks_map = {}  # {task_id: task} all tasks
        self.conf = {
            'workers': 10
        }

    def add_task(self, task: Task):
        """add seafevents io task

        :return: a response for flask
        """
        if self.tasks_queue.full():
            return {'error_msg': 'Server is busy'}, 500
        self.tasks_queue.put(task)
        self.tasks_map[task.id] = task
        return {'task_id': task.id}, 200

    def query_task(self, task_id):
        """query status of a task

        :return: a response for flask
        """
        logger.info('self.tasks_map: %s', self.tasks_map)
        task = self.tasks_map.get(task_id, None)
        if not task:
            return {'error_msg': 'Task not found'}, 404
        if not task.is_finished():
            return {'is_finished': False}, 200
        self.tasks_map.pop(task_id, None)
        if task.status == 'error':
            return {'error_msg': task.error.msg}, task.error.response_code
        if task.func == export_ledger_to_excel:
            return {'is_finished': True}, 200

    def add_export_ledger_to_excel_task(self, repo_id, parent_dir=None):
        task_id = str(uuid4())
        logger.info('export_ledger_rows_to_excel, args: %s', ((repo_id, parent_dir),))
        task = Task(task_id, export_ledger_to_excel, args=(repo_id, parent_dir))
        return self.add_task(task)

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
                logger.info('Run task: %s\n' % task_info)

                # run
                result = task.run() or 'success'
                logger.info('result: ', result)
                task.set_result(result)

                logger.info('Run task success: %s cost %ds \n' % (task_info, task.get_cost_time()))
            except TaskError as e:
                logger.error('task error: %s', e)
                task.set_error(e)
                logger.info('self.tasks_map: %s', self.tasks_map)
            except Exception as e:
                logger.exception('Failed to handle task %s, error: %s \n' % (task.id, e))
                task.set_error(TaskError('Internal Server Error', 500))

    def start(self):
        thread_num = self.conf['workers']
        for i in range(thread_num):
            t_name = 'SeafileIO Thread-' + str(i)
            t = Thread(target=self.handle_task, name=t_name)
            t.setDaemon(True)
            t.start()


task_manager = TaskManager()
