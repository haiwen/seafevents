import time
import logging
import threading
import queue
import uuid

from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.repo_workflow.workflow_executor import on_add_file_event
from seafevents.db import init_db_session_class

logger = logging.getLogger(__name__)


class WorkflowTaskManager(object):

    def __init__(self):
        self.metadata_server_api = MetadataServerAPI('seafevents')

        self.tasks_map = {}
        self.task_queue = queue.Queue()
        self.threads = []
        self.current_task_info = {}
        self.worker_num = 3
        self._db_session_class = init_db_session_class()


    def add_file_added_workflow_task(self, record):
        session = self._db_session_class()
        if self.task_queue.full():
            logger.warning('workflow server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                            % (self.task_queue.qsize(), self.current_task_info,
                            self.threads_is_alive()))
            return False
        task_id = str(uuid.uuid4())
        task = (on_add_file_event, (session, record))
        self.task_queue.put(task_id)
        self.tasks_map[task_id] = task
        return True
    
    def threads_is_alive(self):
        info = {}
        for t in self.threads:
            info[t.name] = t.is_alive()
        return info

    def handle_workflow_task(self):
        while True:
            try:
                task_id = self.task_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(e)
                continue
            task = self.tasks_map.get(task_id)
            if type(task) != tuple or len(task) < 1:
                continue
            task_info = task_id + ' ' + str(task[0])
            try:
                self.current_task_info[task_id] = task_info
                logging.info('Run task: %s' % task_info)
                start_time = time.time()
                # run
                task[0](*task[1])

                finish_time = time.time()
                logging.info('Run workflow task success: %s cost %ds \n' % (task_info, int(finish_time - start_time)))
                self.current_task_info.pop(task_id, None)
            except Exception as e:
                logger.error('Failed to handle workflow task %s, error: %s \n' % (task_info, e))
                self.current_task_info.pop(task_id, None)
            finally:
                self.tasks_map.pop(task_id, None)
    
    def run(self, task_workers=3):
        workflow_name = 'Workflow Thread-'
        for thread_num in range(task_workers):
            image_t = threading.Thread(target=self.handle_workflow_task, name=workflow_name+str(thread_num))
            image_t.setDaemon(True)
            image_t.start()
            self.threads.append(image_t)

workflow_task_manager = WorkflowTaskManager()
