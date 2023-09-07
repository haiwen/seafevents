import logging
import hashlib
import stat
from collections import defaultdict
from threading import Thread, Lock

from sqlalchemy.orm import scoped_session

from seaserv import seafile_api

from seafevents.db import init_db_session_class

logger = logging.getLogger(__name__)


class ExtendedPropsTaskManager:

    def __init__(self):
        self.worker = 10
        self.worker_lock = Lock()
        self.worker_map = defaultdict(list)  # {repo_id: [folder_path...]}
        self.list_max = 1000
        self.step = 1000

    def init_config(self, config):
        self.config = config
        self.DB = scoped_session(init_db_session_class(self.config))

    def get_running_count(self):
        all_count = 0
        for paths in self.worker_map.values():
            all_count += len(paths)
        return all_count

    def can_mark_folder(self, repo_id, folder_path):
        """
        :return: can_mark -> bool or None, error_type -> string or None
        """
        if self.get_running_count() >= self.worker:
            return False, 'server_busy'
        for cur_repo_id, folder_paths in self.worker_map.items():
            if repo_id != cur_repo_id:
                continue
            for cur_folder_path in folder_paths:
                if cur_folder_path.startswith(folder_path):
                    return False, 'sub_folder_running'
        return True, None

    def can_mark_item(self, repo_id, file_path):
        for cur_repo_id, folder_paths in self.worker_map.items():
            if repo_id != cur_repo_id:
                continue
            for cur_folder_path in folder_paths:
                if file_path.startswith(cur_folder_path):
                    return False
        return True

    def add_mark_task(self, repo_id, folder_path):
        """
        :return: {success} {error_type, error_msg}
        """
        folder_path = folder_path.rstrip('/')
        with self.worker_lock:
            can_mark, error_type = self.can_mark_folder(repo_id, folder_path)
            if not can_mark:
                return {'error_type': error_type}
            thread = Thread(target=self.mark_folder, args=(repo_id, folder_path,))
            thread.start()
            self.worker_map[repo_id].append(folder_path)
            return {'sucess': True}

    def md5_repo_id_parent_path(self, repo_id, parent_path):
        parent_path = parent_path.rstrip('/') if parent_path != '/' else '/'
        return hashlib.md5((repo_id + parent_path).encode('utf-8')).hexdigest()

    def query_fileuuids(self, repo_id, file_paths):
        """
        :return: {file_path: fileuuid}
        """
        md5_2_file_path = {}
        fileuuids_dict = {}
        session = self.DB()
        try:
            for i in range(0, len(file_paths), self.step):
                for file_path in file_paths[i: i+self.step]:
                    md5_2_file_path[self.md5_repo_id_parent_path(repo_id, file_path)] = file_path
                sql = '''
                    SELECT uuid, repo_id_parent_path_md5 FROM tags_fileuuidmap WHERE repo_id_parent_path_md5 in :repo_id_parent_path_md5
                '''
                results = session.execute(sql, {'repo_id_parent_path_md5': list(md5_2_file_path.keys())})
                for path_md5 in results:
                    fileuuids_dict[md5_2_file_path[path_md5[1]]] = path_md5[0]
                # TODO: some files have no fileuuids
        except Exception as e:
            logger.exception('query repo: %s some fileuuids error: %s', e)
        finally:
            session.remove()

    def _mark_folder(self, repo_id, folder_path):
        stack = [folder_path]
        query_list = []  # [{path, type}]
        file_query_list = []  # [path]
        update_list = []  # [{}]
        insert_list = []  # [{}]
        while stack:
            dirents = seafile_api.list_dir_by_path(repo_id, folder_path)
            for dirent in dirents:
                if stat.S_ISDIR(dirent.mode):
                    query_list.append({'path': dirent.path, 'type': 'dir'})
                else:
                    query_list.append({'path': dirent.path, 'type': 'file'})
                    file_query_list.append(dirent.path)
            if len(query_list) >= self.list_max:
                fileuuids_dict = self.query_fileuuids(repo_id, file_query_list)

    def mark_folder(self, repo_id, folder_path):
        try:
            self._mark_folder()
        except Exception as e:
            logger.exception('folder_path: %s')
        finally:
            pass
