import queue
import threading
import logging
import time
import uuid
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from seafevents.utils.migration_repo import migrate_repo, remove_repo_objs
from seafevents.events.metrics import NODE_NAME, METRIC_CHANNEL_NAME
from seafevents.app.event_redis import redis_cache
from seafevents.db import create_engine_from_env
from seaserv import REPO_STATUS_READ_ONLY, REPO_STATUS_NORMAL
import json

logger = logging.getLogger('seafevents')

class RepoArchiveTaskManager(object):

    def __init__(self):
        self.app = None
        self.tasks_map = {}
        self.task_results_map = {}
        self.tasks_queue = queue.Queue(10)
        self.current_task_info = {}
        self.threads = []
        self.conf = {
            'workers': 1, # Archive operations are heavy, maybe limit to 1 or small number
            'expire_time': 30 * 60
        }

    def init(self, app, workers, task_expire_time):
        self.app = app
        self.conf['expire_time'] = task_expire_time
        # self.conf['workers'] = workers # Use config or default
        if workers > 1:
             self.conf['workers'] = workers

    def is_valid_task_id(self, task_id):
        return task_id in (self.tasks_map.keys() | self.task_results_map.keys())

    def publish_io_qsize_metric(self, qsize):
        publish_metric = {
            "metric_name": "repo_archive_task_queue_size",
            "metric_type": "gauge",
            "metric_help": "The size of the repo archive task queue",
            "component_name": "seafevents",
            "node_name": NODE_NAME,
            "metric_value": qsize,
            "details": {}
        }
        try:
            redis_cache.publish(METRIC_CHANNEL_NAME, json.dumps(publish_metric))
        except Exception as e:
            logger.warning("Failed to publish metrics: %s", e)


    def add_repo_archive_task(self, repo_id, orig_storage_id, dest_storage_id, op_type, username):
        task_id = str(uuid.uuid4())
        task = (self.do_archive, (repo_id, orig_storage_id, dest_storage_id, op_type, task_id, username))

        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        self.publish_io_qsize_metric(self.tasks_queue.qsize())
        return task_id

    def update_archive_status(self, repo_id, status):
        try:
            engine = create_engine_from_env('seafile')
            session = sessionmaker(engine)()
            
            if status is None:
                sql = "UPDATE RepoInfo SET archive_status=NULL WHERE repo_id='{}'".format(repo_id)
            else:
                sql = "UPDATE RepoInfo SET archive_status='{}' WHERE repo_id='{}'".format(status, repo_id)
            
            session.execute(text(sql))
            session.commit()
            session.close()
        except Exception as e:
            logger.error("Failed to update archive status for repo %s: %s", repo_id, e)
            raise e

    def send_notification(self, repo_id, op_type, username, success=True):
        try:
            # We need to get repo name first, from seafile_db
            engine = create_engine_from_env('seafile')
            session = sessionmaker(engine)()
            sql = "SELECT name FROM RepoInfo WHERE repo_id='{}'".format(repo_id)
            result = session.execute(text(sql)).fetchone()
            session.close()
            repo_name = result[0] if result else 'Unknown'
            
            # Insert notification
            engine = create_engine_from_env('seahub')
            session = sessionmaker(engine)()
            
            # Determine message type based on op_type and success
            if success:
                msg_type = 'repo_archived' if op_type == 'archive' else 'repo_unarchived'
            else:
                msg_type = 'repo_archive_failed' if op_type == 'archive' else 'repo_unarchive_failed'
            
            detail = json.dumps({'repo_id': repo_id, 'repo_name': repo_name})
            
            sql = "INSERT INTO notifications_usernotification (to_user, msg_type, detail, timestamp, seen) VALUES ('{}', '{}', '{}', NOW(), 0)".format(username, msg_type, detail)
            
            session.execute(text(sql))
            session.commit()
            session.close()
        except Exception as e:
            logger.error("Failed to send notification for repo %s: %s", repo_id, e)

    def do_archive(self, repo_id, orig_storage_id, dest_storage_id, op_type, task_id, username):
        logger.info("Starting %s for repo %s from %s to %s", op_type, repo_id, orig_storage_id, dest_storage_id)
        
        # Determine rollback status (what to restore if failed)
        # archive: was NULL -> if failed, restore to NULL
        # unarchive: was 'archived' -> if failed, restore to 'archived'
        rollback_status = None if op_type == 'archive' else 'archived'
        
        try:
            # 1. Migrate
            initial_status = REPO_STATUS_NORMAL if op_type == 'archive' else REPO_STATUS_READ_ONLY
            final_status = REPO_STATUS_READ_ONLY if op_type == 'archive' else REPO_STATUS_NORMAL
            migrate_repo(repo_id, orig_storage_id, dest_storage_id, list_src_by_commit=True, initial_status=initial_status, final_status=final_status)
            
            # test rollback
            # raise Exception("Test failure for rollback verification")

            # 2. Update Status
            new_status = 'archived' if op_type == 'archive' else None
            self.update_archive_status(repo_id, new_status)
            
            # 3. Cleanup Old Objects
            remove_repo_objs(repo_id, orig_storage_id)
            
            # 4. Success Notification
            self.send_notification(repo_id, op_type, username, success=True)

            logger.info("Successfully completed %s for repo %s", op_type, repo_id)
            
        except Exception as e:
            logger.exception("Failed to %s repo %s: %s", op_type, repo_id, e)
            
            # Rollback: restore archive_status to previous state
            try:
                self.update_archive_status(repo_id, rollback_status)
                logger.info("Rolled back archive_status for repo %s to %s", repo_id, rollback_status)
            except Exception as rollback_e:
                logger.error("Failed to rollback archive_status for repo %s: %s", repo_id, rollback_e)
            
            # Rollback storage_id and repo status
            try:
                from seaserv import seafile_api
                seafile_api.update_repo_storage_id(repo_id, orig_storage_id)
                seafile_api.set_repo_status(repo_id, initial_status)
                logger.info("Rolled back storage_id to %s and repo status to %s for repo %s", orig_storage_id, initial_status, repo_id)
            except Exception as se_e:
                logger.error("Failed to rollback repository properties for repo %s: %s", repo_id, se_e)
            
            # Send failure notification
            try:
                self.send_notification(repo_id, op_type, username, success=False)
            except Exception as notify_e:
                logger.error("Failed to send failure notification for repo %s: %s", repo_id, notify_e)
            
            raise e


    def query_status(self, task_id):
        task_result = self.task_results_map.pop(task_id, None)
        if task_result == 'success':
            return True, None
        if isinstance(task_result, str) and task_result.startswith('error_'):
            return True, task_result[6:]
        return False, None

    def threads_is_alive(self):
        info = {}
        for t in self.threads:
            info[t.name] = t.is_alive()
        return info

    def handle_task(self):
        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(e)
                continue
            
            task = self.tasks_map.get(task_id)
            if type(task) != tuple or len(task) < 1:
                continue
            if not callable(task[0]):
                continue
                
            task_info = task_id + ' ' + str(task[0])
            try:
                self.current_task_info[task_id] = task_info
                logging.info('Run task: %s' % task_info)
                start_time = time.time()

                # run
                task[0](*task[1])
                self.task_results_map[task_id] = 'success'
                self.publish_io_qsize_metric(self.tasks_queue.qsize())
                finish_time = time.time()
                logging.info('Run task success: %s cost %ds \n' % (task_info, int(finish_time - start_time)))
                self.current_task_info.pop(task_id, None)
            except Exception as e:
                logger.exception('Failed to handle task %s, error: %s \n' % (task_info, e))
                if len(e.args) > 0:
                    self.task_results_map[task_id] = 'error_' + str(e.args[0])
                else:
                    self.task_results_map[task_id] = 'error_' + str(e)
                self.current_task_info.pop(task_id, None)
            finally:
                self.tasks_map.pop(task_id, None)

    def run(self):
        thread_num = self.conf['workers']
        for i in range(thread_num):
            t_name = 'RepoArchiveManager Thread-' + str(i)
            t = threading.Thread(target=self.handle_task, name=t_name)
            self.threads.append(t)
            t.setDaemon(True)
            t.start()


repo_archive_task_manager = RepoArchiveTaskManager()
