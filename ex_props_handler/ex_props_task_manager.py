import logging
import hashlib
import os
import stat
from collections import defaultdict
from datetime import datetime
from threading import Thread, Lock
from uuid import uuid4

from sqlalchemy.orm import scoped_session
from sqlalchemy.sql import text

from seaserv import seafile_api

from seafevents.app.config import DTABLE_WEB_SERVER, SEATABLE_EX_PROPS_BASE_API_TOKEN, EX_PROPS_TABLE, EX_EDITABLE_COLUMNS
from seafevents.db import init_db_session_class
from seafevents.utils.seatable_api import SeaTableAPI

logger = logging.getLogger(__name__)

EMPTY_SHA1 = '0000000000000000000000000000000000000000'


class QueryException(Exception):
    pass


class ExtendedPropsTaskManager:

    def __init__(self):
        self.worker = 10
        self.worker_lock = Lock()
        self.worker_map = defaultdict(list)  # {repo_id: [folder_path...]}, need to delete finished/failed task
        self.list_max = 1000
        self.step = 500

    def init_config(self, config):
        self.config = config
        self.DB = scoped_session(init_db_session_class(self.config))

    def get_running_count(self):
        all_count = 0
        for paths in self.worker_map.values():
            all_count += len(paths)
        return all_count

    def can_set_folder(self, repo_id, folder_path):
        """
        :return: can_set -> bool or None, error_type -> string or None
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

    def can_set_item(self, repo_id, path):
        for cur_repo_id, folder_paths in self.worker_map.items():
            if repo_id != cur_repo_id:
                continue
            for cur_folder_path in folder_paths:
                if path.startswith(cur_folder_path):
                    return False
        return True

    def add_set_task(self, repo_id, folder_path, context):
        """
        :return: {success} or {error_type, error_msg}
        """
        folder_path = folder_path.rstrip('/')
        with self.worker_lock:
            can_set, error_type = self.can_set_folder(repo_id, folder_path)
            if not can_set:
                return {'error_type': error_type}
            thread = Thread(target=self.set_folder, args=(repo_id, folder_path, context))
            thread.start()
            self.worker_map[repo_id].append(folder_path)
            return {'sucess': True}

    def md5_repo_id_parent_path(self, repo_id, parent_path):
        parent_path = parent_path.rstrip('/') if parent_path != '/' else '/'
        return hashlib.md5((repo_id + parent_path).encode('utf-8')).hexdigest()

    def query_fileuuids_map(self, repo_id, file_paths):
        """
        :return: {file_path: fileuuid}
        """
        file_path_2_uuid_map = {}
        no_uuid_file_paths = []
        session = self.DB()
        try:
            # query uuids
            for i in range(0, len(file_paths), self.step):
                parent_path_2_filenames_map = defaultdict(list)
                for file_path in file_paths[i: i+self.step]:
                    parent_path, filename = os.path.split(file_path)
                    parent_path_2_filenames_map[parent_path].append(filename)
                for parent_path, filenames in parent_path_2_filenames_map.items():
                    md5 = self.md5_repo_id_parent_path(repo_id, parent_path)
                    sql = "SELECT `uuid`, `filename` FROM `tags_fileuuidmap` WHERE repo_id=:repo_id AND repo_id_parent_path_md5=:md5 AND filename IN :filenames"
                    results = session.execute(text(sql), {'repo_id': repo_id, 'md5': md5, 'filenames': filenames})
                    for uuid_item in results:
                        file_path_2_uuid_map[os.path.join(parent_path, uuid_item[1])] = uuid_item[0]
                    ## some filename no uuids
                    for filename in filenames:
                        if os.path.join(parent_path, filename) not in file_path_2_uuid_map:
                            no_uuid_file_paths.append({'file_path': file_path, 'uuid': uuid4().hex, 'repo_id_parent_path_md5': md5})
            # create uuids
            for i in range(0, len(no_uuid_file_paths), self.step):
                values = []
                for j in range(i, min(i+self.step, len(no_uuid_file_paths))):
                    no_uuid_file_path = no_uuid_file_paths[j]
                    values.append({
                        'uuid': no_uuid_file_path['uuid'],
                        'repo_id': repo_id,
                        'repo_id_parent_path_md5': no_uuid_file_path['repo_id_parent_path_md5'],
                        'parent_path': os.path.dirname(no_uuid_file_path['file_path']),
                        'filename': os.path.basename(no_uuid_file_path['file_path']),
                        'is_dir': 0
                    })
                sql = '''
                    INSERT INTO tags_fileuuidmap (uuid, repo_id, repo_id_parent_path_md5, parent_path, filename, is_dir) VALUES %s
                ''' % (', '.join(["('%(uuid)s', '%(repo_id)s', '%(repo_id_parent_path_md5)s', '%(parent_path)s', '%(filename)s', '%(is_dir)s')" % value for value in values]))
                session.execute(text(sql))
                session.commit()
        except Exception as e:
            logger.exception('query repo: %s some fileuuids error: %s', e)
        finally:
            self.DB.remove()
        return file_path_2_uuid_map

    def query_path_2_row_id_map(self, repo_id, query_list, seatable_api: SeaTableAPI):
        """
        :return: path_2_row_id_map -> {path: row_id}
        """
        path_2_row_id_map = {}
        for i in range(0, len(query_list), self.step):
            paths_str = ', '.join(map(lambda x: f"'{x['path']}'", query_list[i: i+self.step]))
            sql = f"SELECT `_id`, `Path` FROM `{EX_PROPS_TABLE}` WHERE `Repo ID`='{repo_id}' AND `Path` IN ({paths_str})"
            try:
                resp_json = seatable_api.query(sql, convert=True)
                rows = resp_json['results']
            except Exception as e:
                raise QueryException('query repo: %s error: %s' % (repo_id, e))
            path_2_row_id_map.update({row['Path']: row['_id'] for row in rows})
        return path_2_row_id_map

    def query_ex_props_by_path(self, repo_id, path, seatable_api: SeaTableAPI):
        columns_str = ', '.join(map(lambda x: f"`{x}`", EX_EDITABLE_COLUMNS))
        sql = f"SELECT {columns_str} FROM `{EX_PROPS_TABLE}` WHERE `Repo ID` = '{repo_id}' AND `Path` = '{path}'"
        try:
            resp_json = seatable_api.query(sql, convert=True)
            if not resp_json['results']:
                raise QueryException('folder props not found')
            row = resp_json['results'][0]
        except Exception as e:
            raise QueryException('query repo: %s path: %s error: %s' % (repo_id, path, e))
        return row

    def update_ex_props(self, update_list, ex_props, seatable_api: SeaTableAPI):
        for i in range(0, len(update_list), self.step):
            updates = []
            for j in range(i, min(len(update_list), i+self.step)):
                updates.append({
                    'row_id': update_list[j]['row_id'],
                    'row': ex_props
                })
            try:
                seatable_api.update_rows_by_dtable_db(EX_PROPS_TABLE, updates)
            except Exception as e:
                logger.exception('update table: %s error: %s', EX_PROPS_TABLE, e)

    def insert_ex_props(self, repo_id, insert_list, ex_props, context, seatable_api: SeaTableAPI):
        for i in range(0, len(insert_list), self.step):
            rows = []
            for j in range(i, min(len(insert_list), i+self.step)):
                row = {
                    'Repo ID': repo_id,
                    'File': os.path.basename(insert_list[j]['path']),
                    'UUID': insert_list[j].get('fileuuid'),
                    'Path': insert_list[j]['path'],
                    '创建日期': str(datetime.fromtimestamp(insert_list[j]['mtime'])),
                    '文件负责人': context['文件负责人']
                }
                row.update(ex_props)
                rows.append(row)
            try:
                seatable_api.batch_append_rows(EX_PROPS_TABLE, rows)
            except Exception as e:
                logger.exception('update table: %s error: %s', EX_PROPS_TABLE, e)

    def _set_folder(self, repo_id, folder_path, context):
        stack = [folder_path]

        query_list = []  # [{path, type}]
        file_query_list = []  # [path]

        update_list = []  # [{}]
        insert_list = []  # [{}]

        try:
            seatable_api = SeaTableAPI(SEATABLE_EX_PROPS_BASE_API_TOKEN, DTABLE_WEB_SERVER)
        except Exception as e:
            logger.error('DTABLE_WEB_SERVER: %s, SEATABLE_EX_PROPS_BASE_API_TOKEN: %s auth error: %s', DTABLE_WEB_SERVER, SEATABLE_EX_PROPS_BASE_API_TOKEN, e)
            return

        # query folder props
        folder_props = self.query_ex_props_by_path(repo_id, folder_path, seatable_api)
        while stack:
            current_path = stack.pop()
            dirents = seafile_api.list_dir_by_path(repo_id, current_path)
            if not dirents:
                continue
            for dirent in dirents:
                dirent_path = os.path.join(current_path, dirent.obj_name)
                if stat.S_ISDIR(dirent.mode):
                    query_list.append({'path': dirent_path, 'type': 'dir', 'mtime': dirent.mtime})
                    stack.append(dirent_path)
                else:
                    if dirent.obj_id == EMPTY_SHA1:
                        continue
                    query_list.append({'path': dirent_path, 'type': 'file', 'mtime': dirent.mtime})
                    file_query_list.append(dirent_path)
            # query ex-props
            if len(query_list) >= self.list_max:
                file_path_2_uuid_map = self.query_fileuuids_map(repo_id, file_query_list)
                path_2_row_id_map = self.query_path_2_row_id_map(repo_id, query_list, seatable_api)
                for query_item in query_list:
                    if query_item['path'] in path_2_row_id_map:
                        query_item['row_id'] = path_2_row_id_map.get(query_item['path'])
                        update_list.append(query_item)
                    else:
                        if query_item['type'] == 'file':
                            query_item['fileuuid'] = file_path_2_uuid_map.get(query_item['path'])
                        insert_list.append(query_item)
                query_list = file_query_list = []
            # update ex-props
            if len(update_list) >= self.list_max:
                self.update_ex_props(update_list, folder_props, seatable_api)
                update_list = []
            # insert ex-props
            if len(insert_list) >= self.list_max:
                self.insert_ex_props(repo_id, insert_list, folder_props, context, seatable_api)
                insert_list = []

        # handle query/update/insert left
        file_path_2_uuid_map = self.query_fileuuids_map(repo_id, file_query_list)
        path_2_row_id_map = self.query_path_2_row_id_map(repo_id, query_list, seatable_api)
        for query_item in query_list:
            if query_item['path'] in path_2_row_id_map:
                query_item['row_id'] = path_2_row_id_map.get(query_item['path'])
                update_list.append(query_item)
            else:
                if query_item['type'] == 'file':
                    query_item['fileuuid'] = file_path_2_uuid_map.get(query_item['path'])
                insert_list.append(query_item)
        self.update_ex_props(update_list, folder_props, seatable_api)
        self.insert_ex_props(repo_id, insert_list, folder_props, context, seatable_api)

    def set_folder(self, repo_id, folder_path, context):
        try:
            self._set_folder(repo_id, folder_path, context)
        except Exception as e:
            logger.exception('folder_path: %s')
        finally:
            with self.worker_lock:
                self.clear_worker(repo_id, folder_path)

    def clear_worker(self, repo_id, folder_path):
        empty_repo_ids = []
        for cur_repo_id, folder_paths in self.worker_map.items():
            if cur_repo_id == repo_id:
                self.worker_map[cur_repo_id] = [item for item in folder_paths if item != folder_path]
            if not self.worker_map[repo_id]:
                empty_repo_ids.append(repo_id)
        for repo_id in empty_repo_ids:
            del self.worker_map[repo_id]


ex_props_task_manager = ExtendedPropsTaskManager()
