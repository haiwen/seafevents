import json
import logging
import os
import queue
import shutil
import tempfile
import threading
import time
import uuid

from sqlalchemy import text

from seafevents.db import init_db_session_class
from seafevents.repo_metadata.backup import IGNORED_COLUMN_KEYS, export_metadata_backup, import_metadata_backup
from seafevents.repo_metadata.metadata_server_api import MetadataServerAPI
from seafevents.seasearch.index_task.index_task_manager import index_task_manager


logger = logging.getLogger(__name__)


class MetadataBackupTaskManager:

    def __init__(self):
        self.tasks = {}
        self.tasks_queue = queue.Queue(10)
        self.lock = threading.Lock()
        self.session_class = None
        self.workers = 1
        self.expire_time = 30 * 60

    def init(self, app, workers, task_expire_time):
        self.session_class = init_db_session_class('seahub')
        self.workers = max(1, workers)
        self.expire_time = task_expire_time

    def run(self):
        for index in range(self.workers):
            thread = threading.Thread(
                target=self._handle_tasks,
                name=f'MetadataBackupTaskManager-{index}',
                daemon=True,
            )
            thread.start()

    def add_export_task(self, repo_id, repo_name, username):
        task = self._new_task('export', repo_id, username)
        task['repo_name'] = repo_name
        self.tasks_queue.put(task['id'])
        return task['id']

    def add_import_task(self, repo_id, username, source):
        task = self._new_task('import', repo_id, username)
        source.save(task['source_path'])
        self.tasks_queue.put(task['id'])
        return task['id']

    def add_restore_task(self, task_id, repo_id, username):
        with self.lock:
            task = self.tasks.get(task_id)
            if not task or task['repo_id'] != repo_id or task['username'] != username:
                return False
            if task['operation'] != 'import' or task['status'] != 'ready':
                return False
            task['operation'] = 'restore'
            task['status'] = 'queued'
            task['updated_at'] = time.time()
        self.tasks_queue.put(task_id)
        return True

    def get_task(self, task_id, repo_id, username):
        self._cleanup()
        with self.lock:
            task = self.tasks.get(task_id)
            if not task or task['repo_id'] != repo_id or task['username'] != username:
                return None
            return {
                'id': task['id'],
                'operation': task['operation'],
                'status': task['status'],
                'result': task.get('result'),
                'error': task.get('error'),
            }

    def get_download(self, task_id, repo_id, username):
        with self.lock:
            task = self.tasks.get(task_id)
            if not task or task['repo_id'] != repo_id or task['username'] != username:
                return None
            if task['operation'] != 'export' or task['status'] != 'success':
                return None
            return task['output_path'], task['filename']

    def remove_task(self, task_id):
        with self.lock:
            task = self.tasks.pop(task_id, None)
        if task:
            shutil.rmtree(task['directory'], ignore_errors=True)

    def _new_task(self, operation, repo_id, username):
        self._cleanup()
        task_id = str(uuid.uuid4())
        directory = tempfile.mkdtemp(prefix=f'seafile-metadata-{task_id}-')
        task = {
            'id': task_id,
            'operation': operation,
            'repo_id': repo_id,
            'username': username,
            'status': 'queued',
            'directory': directory,
            'source_path': os.path.join(directory, 'source.xlsx'),
            'output_path': os.path.join(directory, 'metadata-backup.xlsx'),
            'updated_at': time.time(),
        }
        with self.lock:
            self.tasks[task_id] = task
        return task

    def _handle_tasks(self):
        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            with self.lock:
                task = self.tasks.get(task_id)
                if not task:
                    continue
                task['status'] = 'running'
                task['updated_at'] = time.time()
            try:
                if task['operation'] == 'export':
                    self._export(task)
                elif task['operation'] == 'import':
                    self._preview(task)
                elif task['operation'] == 'restore':
                    self._restore(task)
                else:
                    raise RuntimeError('Invalid metadata backup operation')
            except Exception as error:
                logger.exception('Metadata backup task %s failed', task_id)
                with self.lock:
                    task['status'] = 'error'
                    task['error'] = str(error)
                    task['updated_at'] = time.time()
            finally:
                self.tasks_queue.task_done()

    def _export(self, task):
        settings = self._get_settings(task['repo_id'])
        metadata_server_api = MetadataServerAPI(task['username'])
        export_metadata_backup(
            metadata_server_api, task['repo_id'], task['repo_name'], settings, task['output_path']
        )
        safe_name = task['repo_name'].replace('/', '_').replace('\\', '_')
        with self.lock:
            task['filename'] = f'{safe_name}-metadata-backup.xlsx'
            task['status'] = 'success'
            task['result'] = {'filename': task['filename']}
            task['updated_at'] = time.time()

    def _preview(self, task):
        parsed = import_metadata_backup(task['source_path'], task['repo_id'])
        with self.lock:
            task['parsed'] = parsed
            task['status'] = 'ready'
            task['result'] = parsed['summary']
            task['updated_at'] = time.time()

    def _restore(self, task):
        parsed = task.get('parsed')
        if not parsed:
            raise RuntimeError('Metadata backup preview not found')
        metadata_server_api = MetadataServerAPI(task['username'], timeout=300)
        metadata_server_api.restore_metadata(task['repo_id'], parsed['payload'])
        self._restore_settings(task['repo_id'], parsed['settings'])
        index_task_manager.delete_summary_vector_index(task['repo_id'])
        with self.lock:
            task['status'] = 'success'
            task['result'] = parsed['summary']
            task['updated_at'] = time.time()

    def _get_settings(self, repo_id):
        settings = {'views': {'views': [], 'navigation': []}, 'details_settings': {}, 'global_hidden_columns': []}
        with self.session_class() as session:
            metadata = session.execute(text(
                'SELECT details_settings, global_hidden_columns, tags_enabled, summary_enabled '
                'FROM repo_metadata WHERE repo_id=:repo_id LIMIT 1'
            ), {'repo_id': repo_id}).mappings().first()
            if metadata:
                settings['details_settings'] = _load_json(metadata['details_settings'], {})
                settings['global_hidden_columns'] = _load_json(metadata['global_hidden_columns'], [])
                settings['global_hidden_columns'] = [
                    key for key in settings['global_hidden_columns'] if key not in IGNORED_COLUMN_KEYS
                ]
                settings['tags_enabled'] = bool(metadata['tags_enabled'])
                settings['summary_enabled'] = bool(metadata['summary_enabled'])
            views = session.execute(text(
                'SELECT details FROM repo_metadata_view WHERE repo_id=:repo_id LIMIT 1'
            ), {'repo_id': repo_id}).scalar()
            settings['views'] = _filter_face_views(_load_json(views, settings['views']))
        return settings

    def _restore_settings(self, repo_id, settings):
        views = _filter_face_views(settings.get('views') or {'views': [], 'navigation': []})
        with self.session_class() as session:
            session.execute(text(
                'UPDATE repo_metadata SET details_settings=:details_settings, '
                'global_hidden_columns=:global_hidden_columns, tags_enabled=:tags_enabled, '
                'summary_enabled=:summary_enabled, face_recognition_enabled=0, ai_summary_indexed_at=NULL, '
                "ai_processing_status='' WHERE repo_id=:repo_id"
            ), {
                'repo_id': repo_id,
                'details_settings': json.dumps(settings.get('details_settings') or {}),
                'global_hidden_columns': json.dumps(settings.get('global_hidden_columns') or []),
                'tags_enabled': bool(settings.get('tags_enabled')),
                'summary_enabled': bool(settings.get('summary_enabled')),
            })
            view_id = session.execute(text(
                'SELECT id FROM repo_metadata_view WHERE repo_id=:repo_id LIMIT 1'
            ), {'repo_id': repo_id}).scalar()
            if view_id:
                session.execute(text(
                    'UPDATE repo_metadata_view SET details=:details WHERE id=:id'
                ), {'id': view_id, 'details': json.dumps(views)})
            else:
                session.execute(text(
                    'INSERT INTO repo_metadata_view (repo_id, details) VALUES (:repo_id, :details)'
                ), {'repo_id': repo_id, 'details': json.dumps(views)})
            session.commit()

    def _cleanup(self):
        now = time.time()
        expired = []
        with self.lock:
            for task_id, task in list(self.tasks.items()):
                if task['status'] in {'success', 'error', 'ready'} and now - task['updated_at'] > self.expire_time:
                    expired.append(self.tasks.pop(task_id))
        for task in expired:
            shutil.rmtree(task['directory'], ignore_errors=True)


def _load_json(value, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _filter_face_views(details):
    face_ids = {
        view.get('_id') for view in details.get('views', [])
        if view.get('type') == 'face_recognition' or view.get('_id') == '_face_recognition'
    }
    details['views'] = [view for view in details.get('views', []) if view.get('_id') not in face_ids]
    for view in details['views']:
        for key in ('filters', 'basic_filters', 'sorts', 'groupbys'):
            view[key] = [item for item in view.get(key, []) if item.get('column_key') not in IGNORED_COLUMN_KEYS]
        view['hidden_columns'] = [key for key in view.get('hidden_columns', []) if key not in IGNORED_COLUMN_KEYS]
    navigation = []
    for item in details.get('navigation', []):
        if item.get('_id') in face_ids:
            continue
        if item.get('type') == 'folder':
            item['children'] = [child for child in item.get('children', []) if child.get('_id') not in face_ids]
        navigation.append(item)
    details['navigation'] = navigation
    return details


metadata_backup_task_manager = MetadataBackupTaskManager()
