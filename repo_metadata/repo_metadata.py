import json
import os
import logging

from seafevents.repo_metadata.utils import METADATA_TABLE, get_file_type_ext_by_name
from seafevents.utils import timestamp_to_isoformat_timestr
from seafevents.repo_metadata.metadata_manager import ZERO_OBJ_ID

logger = logging.getLogger(__name__)

EXCLUDED_PATHS = ['/_Internal', '/images']
METADATA_OP_LIMIT = 1000


class RepoMetadata:

    def __init__(self, metadata_server_api, redis_mq):
        self.metadata_server_api = metadata_server_api
        self.redis_mq = redis_mq

    def cal_renamed_and_moved_files(self, added_files, deleted_files):

        need_added_files = []
        obj_id_to_file = {}
        for file in added_files:
            if self.is_excluded_path(file.path):
                continue

            obj_id = file.obj_id
            if obj_id == ZERO_OBJ_ID:
                need_added_files.append(file)
            else:
                obj_id_to_file[obj_id] = file

        need_updated_files = []
        new_deleted_files = []
        for file in deleted_files:
            if self.is_excluded_path(file.path):
                continue
            obj_id = file.obj_id
            add_file = obj_id_to_file.get(file.obj_id)
            if add_file and obj_id != ZERO_OBJ_ID:
                need_updated_files.append(add_file)
                obj_id_to_file.pop(file.obj_id)
            else:
                new_deleted_files.append(file)
        new_added_files = list(obj_id_to_file.values()) + need_added_files

        return new_added_files, new_deleted_files, need_updated_files

    def update_renamed_or_moved_files(self, repo_id, renamed_or_moved_files):
        if not renamed_or_moved_files:
            return

        obj_ids = []
        base_sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.obj_id.name}` FROM `{METADATA_TABLE.name}` WHERE `_obj_id` IN ('
        sql = base_sql
        obj_id_to_file_dict = {}
        parameters = []
        for file in renamed_or_moved_files:
            path = file.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            obj_id = file.obj_id
            obj_id_to_file_dict[obj_id] = file

            sql += f'?, '
            parameters.append(obj_id)

            file_type, file_ext = get_file_type_ext_by_name(os.path.basename(path))
            if file_type == '_picture' and file_ext != 'gif':
                obj_ids.append(file.obj_id)

            if len(parameters) >= METADATA_OP_LIMIT:
                sql = sql.rstrip(', ') + ')'
                self.update_rows_by_obj_ids(repo_id, sql, parameters, obj_id_to_file_dict)
                sql = base_sql
                parameters = []
                obj_id_to_file_dict = {}

                if obj_ids:
                    data = {
                        'task_type': 'modify_image_index',
                        'repo_id': repo_id,
                        'obj_ids': obj_ids
                    }
                    self.add_slow_task_to_queue(json.dumps(data))
                    obj_ids = []
        if parameters:
            sql = sql.rstrip(', ') + ')'
            self.update_rows_by_obj_ids(repo_id, sql, parameters, obj_id_to_file_dict)

        if obj_ids:
            data = {
                'task_type': 'modify_image_index',
                'repo_id': repo_id,
                'obj_ids': obj_ids
            }
            self.add_slow_task_to_queue(json.dumps(data))

    def update(self, repo_id, added_files, deleted_files, added_dirs, deleted_dirs, modified_files,
                        renamed_files, moved_files, renamed_dirs, moved_dirs):

        new_added_files, new_deleted_files, renamed_or_moved_files = self.cal_renamed_and_moved_files(added_files, deleted_files)

        # delete added_files delete added dirs for preventing duplicate insertions
        self.delete_files(repo_id, new_added_files)
        self.delete_dirs(repo_id, added_dirs)

        self.add_files(repo_id, new_added_files)
        self.delete_files(repo_id, new_deleted_files)
        # update renamed or moved files
        self.update_renamed_or_moved_files(repo_id, renamed_or_moved_files)
        self.add_dirs(repo_id, added_dirs)
        self.delete_dirs(repo_id, deleted_dirs)
        # update normal updated files
        self.update_modified_files(repo_id, modified_files)

        # self.rename_files(repo_id, renamed_files)
        # self.move_files(repo_id, moved_files)
        # self.rename_dirs(repo_id, renamed_dirs)
        # self.move_dirs(repo_id, moved_dirs)

    def is_excluded_path(self, path):
        if not path or path == '/':
            return True
        for ex_path in EXCLUDED_PATHS:
            if path.startswith(ex_path):
                return True

    def delete_rows_by_query(self, repo_id, sql, parameters):
        query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])

        if not query_result:
            return

        row_ids = []
        for row in query_result:
            row_ids.append(row[METADATA_TABLE.columns.id.name])
            if len(row_ids) >= METADATA_OP_LIMIT:
                self.metadata_server_api.delete_rows(repo_id, METADATA_TABLE.id, row_ids)
                row_ids = []

        if not row_ids:
            return
        self.metadata_server_api.delete_rows(repo_id, METADATA_TABLE.id, row_ids)

    def update_rows_by_query(self, repo_id, sql, parameters, path_to_file_dict):
        query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])

        if not query_result:
            return

        updated_rows = []
        for row in query_result:
            row_id = row[METADATA_TABLE.columns.id.name]
            parent_dir = row[METADATA_TABLE.columns.parent_dir.name]
            file_name = row[METADATA_TABLE.columns.file_name.name]
            key = parent_dir + file_name
            new_row = path_to_file_dict.get(key)

            update_row = {
                METADATA_TABLE.columns.id.name: row_id,
                METADATA_TABLE.columns.file_modifier.name: new_row.modifier,
                METADATA_TABLE.columns.file_mtime.name: timestamp_to_isoformat_timestr(new_row.mtime),
                METADATA_TABLE.columns.obj_id.name: new_row.obj_id,
                METADATA_TABLE.columns.size.name: new_row.size,
            }
            updated_rows.append(update_row)

            if len(updated_rows) >= METADATA_OP_LIMIT:
                self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
                updated_rows = []

        if not updated_rows:
            return
        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def update_rows_by_obj_ids(self, repo_id, sql, parameters, obj_id_to_file_dict):
        query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])

        if not query_result:
            return

        updated_rows = []
        for row in query_result:
            row_id = row[METADATA_TABLE.columns.id.name]
            obj_id = row[METADATA_TABLE.columns.obj_id.name]
            new_row = obj_id_to_file_dict.get(obj_id)

            path = new_row.path.rstrip('/')
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            file_type, file_ext = get_file_type_ext_by_name(file_name)

            update_row = {
                METADATA_TABLE.columns.id.name: row_id,
                METADATA_TABLE.columns.parent_dir.name: parent_dir,
                METADATA_TABLE.columns.file_name.name: file_name,
                METADATA_TABLE.columns.suffix.name: file_ext
            }
            updated_rows.append(update_row)

            if len(updated_rows) >= METADATA_OP_LIMIT:
                self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
                updated_rows = []

        if not updated_rows:
            return
        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def add_files(self, repo_id, added_files):
        if not added_files:
            return

        rows = []
        obj_ids = []
        for de in added_files:
            path = de.path.rstrip('/')
            mtime = de.mtime
            obj_id = de.obj_id
            size = de.size
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            modifier = de.modifier
            file_type, file_ext = get_file_type_ext_by_name(file_name)

            if self.is_excluded_path(path):
                continue

            row = {
                METADATA_TABLE.columns.file_creator.name: modifier,
                METADATA_TABLE.columns.file_ctime.name: timestamp_to_isoformat_timestr(mtime),
                METADATA_TABLE.columns.file_modifier.name: modifier,
                METADATA_TABLE.columns.file_mtime.name: timestamp_to_isoformat_timestr(mtime),
                METADATA_TABLE.columns.parent_dir.name: parent_dir,
                METADATA_TABLE.columns.file_name.name: file_name,
                METADATA_TABLE.columns.is_dir.name: False,
                METADATA_TABLE.columns.obj_id.name: obj_id,
                METADATA_TABLE.columns.size.name: size,
                METADATA_TABLE.columns.suffix.name: file_ext,
            }

            if file_type:
                row[METADATA_TABLE.columns.file_type.name] = file_type
            if file_type == '_picture' and file_ext != 'gif':
                obj_ids.append(de.obj_id)
            rows.append(row)

            if len(rows) >= METADATA_OP_LIMIT:
                self.metadata_server_api.insert_rows(repo_id, METADATA_TABLE.id, rows)

                if obj_ids:
                    data = {
                        'task_type': 'image_info_extract',
                        'repo_id': repo_id,
                        'obj_ids': obj_ids
                    }
                    self.add_slow_task_to_queue(json.dumps(data))
                    obj_ids = []
                rows = []
        if not rows:
            return
        self.metadata_server_api.insert_rows(repo_id, METADATA_TABLE.id, rows)
        if obj_ids:
            data = {
                'task_type': 'image_info_extract',
                'repo_id': repo_id,
                'obj_ids': obj_ids
            }
            self.add_slow_task_to_queue(json.dumps(data))

    def add_slow_task_to_queue(self, data):
        self.redis_mq.lpush('metadata_slow_task', data)

    def delete_files(self, repo_id, deleted_files):
        if not deleted_files:
            return

        paths = []
        base_sql = f'SELECT `{METADATA_TABLE.columns.id.name}` FROM `{METADATA_TABLE.name}` WHERE'
        sql = base_sql
        parameters = []
        for file in deleted_files:
            path = file.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            sql += f' (`{METADATA_TABLE.columns.parent_dir.name}` = ? AND `{METADATA_TABLE.columns.file_name.name}` = ?) OR'
            parameters.append(parent_dir)
            parameters.append(file_name)
            file_type, file_ext = get_file_type_ext_by_name(file_name)
            if file_type == '_picture' and file_ext != 'gif':
                paths.append(path)

            if len(parameters) >= METADATA_OP_LIMIT:
                sql = sql.rstrip(' OR')
                self.delete_rows_by_query(repo_id, sql, parameters)
                sql = base_sql
                parameters = []

                if paths:
                    data = {
                        'task_type': 'delete_image_index',
                        'repo_id': repo_id,
                        'paths': paths
                    }
                    self.add_slow_task_to_queue(json.dumps(data))
                    paths = []

        if not parameters:
            return
        sql = sql.rstrip(' OR')
        self.delete_rows_by_query(repo_id, sql, parameters)

        if paths:
            data = {
                'task_type': 'delete_image_index',
                'repo_id': repo_id,
                'paths': paths
            }
            self.add_slow_task_to_queue(json.dumps(data))

    def update_modified_files(self, repo_id, modified_files):
        if not modified_files:
            return

        base_sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}` FROM `{METADATA_TABLE.name}` WHERE'
        sql = base_sql
        path_to_file_dict = {}
        parameters = []
        for file in modified_files:
            path = file.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            key = parent_dir + file_name
            path_to_file_dict[key] = file

            sql += f' (`{METADATA_TABLE.columns.parent_dir.name}` = ? AND `{METADATA_TABLE.columns.file_name.name}` = ?) OR'
            parameters.append(parent_dir)
            parameters.append(file_name)

            if len(parameters) >= METADATA_OP_LIMIT:
                sql = sql.rstrip(' OR')
                self.update_rows_by_query(repo_id, sql, parameters, path_to_file_dict)
                sql = base_sql
                parameters = []
                path_to_file_dict = {}

        if parameters:
            sql = sql.rstrip(' OR')
            self.update_rows_by_query(repo_id, sql, parameters, path_to_file_dict)

    def add_dirs(self, repo_id, added_dirs):
        if not added_dirs:
            return

        rows = []
        for de in added_dirs:
            path = de.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            mtime = de.mtime

            row = {
                METADATA_TABLE.columns.file_creator.name: '',
                METADATA_TABLE.columns.file_ctime.name: timestamp_to_isoformat_timestr(mtime),
                METADATA_TABLE.columns.file_modifier.name: '',
                METADATA_TABLE.columns.file_mtime.name: timestamp_to_isoformat_timestr(mtime),
                METADATA_TABLE.columns.parent_dir.name: parent_dir,
                METADATA_TABLE.columns.file_name.name: file_name,
                METADATA_TABLE.columns.is_dir.name: True,

            }
            rows.append(row)

            if len(rows) >= METADATA_OP_LIMIT:
                self.metadata_server_api.insert_rows(repo_id, METADATA_TABLE.id, rows)
                rows = []
        if not rows:
            return

        self.metadata_server_api.insert_rows(repo_id, METADATA_TABLE.id, rows)

    def delete_dirs(self, repo_id, deleted_dirs):
        if not deleted_dirs:
            return
        base_sql = f'SELECT `{METADATA_TABLE.columns.id.name}` FROM `{METADATA_TABLE.name}` WHERE'
        sql = base_sql
        parameters = []
        for d in deleted_dirs:
            path = d.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            parent_dir = os.path.dirname(path)
            dir_name = os.path.basename(path)
            sql += f' (`{METADATA_TABLE.columns.parent_dir.name}` = ? AND `{METADATA_TABLE.columns.file_name.name}` = ?) OR'
            parameters.append(parent_dir)
            parameters.append(dir_name)
            if len(parameters) >= METADATA_OP_LIMIT:
                sql = sql.rstrip(' OR')
                self.delete_rows_by_query(repo_id, sql, parameters)
                sql = base_sql
                parameters = []

        if not parameters:
            return
        sql = sql.rstrip(' OR')
        self.delete_rows_by_query(repo_id, sql, parameters)

    def rename_files(self, repo_id, renamed_files):
        if not renamed_files:
            return

        sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}` FROM `{METADATA_TABLE.name}` WHERE'
        path_to_file_dict = {}
        need_update = False
        parameters = []
        for file in renamed_files:
            path = file.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            need_update = True
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            key = parent_dir + file_name
            path_to_file_dict[key] = file

            sql += f' (`{METADATA_TABLE.columns.parent_dir.name}` = ? AND `{METADATA_TABLE.columns.file_name.name}` = ?) OR'
            parameters.append(parent_dir)
            parameters.append(file_name)
        if not need_update:
            return
        sql = sql.rstrip(' OR')
        query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])

        if not query_result:
            return

        updated_rows = []
        for row in query_result:
            row_id = row[METADATA_TABLE.columns.id.name]
            parent_dir = row[METADATA_TABLE.columns.parent_dir.name]
            file_name = row[METADATA_TABLE.columns.file_name.name]
            key = parent_dir + file_name
            new_row = path_to_file_dict.get(key)

            new_path = new_row.new_path
            new_parent_dir = os.path.dirname(new_path)
            new_file_name = os.path.basename(new_path)

            update_row = {
                METADATA_TABLE.columns.id.name: row_id,
                METADATA_TABLE.columns.parent_dir.name: new_parent_dir,
                METADATA_TABLE.columns.file_name.name: new_file_name,
                METADATA_TABLE.columns.file_modifier.name: new_row.modifier,
                METADATA_TABLE.columns.file_mtime.name: timestamp_to_isoformat_timestr(new_row.mtime),
            }
            updated_rows.append(update_row)

        if not updated_rows:
            return

        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def move_files(self, repo_id, moved_files):
        if not moved_files:
            return

        sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}` FROM `{METADATA_TABLE.name}` WHERE'
        path_to_file_dict = {}
        need_update = False
        parameters = []
        for file in moved_files:
            path = file.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            need_update = True
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            key = parent_dir + file_name
            path_to_file_dict[key] = file

            sql += f' (`{METADATA_TABLE.columns.parent_dir.name}` = ? AND `{METADATA_TABLE.columns.file_name.name}` = ?) OR'
            parameters.append(parent_dir)
            parameters.append(file_name)

        if not need_update:
            return
        sql = sql.rstrip(' OR')
        query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])

        if not query_result:
            return

        updated_rows = []
        for row in query_result:
            row_id = row[METADATA_TABLE.columns.id.name]
            parent_dir = row[METADATA_TABLE.columns.parent_dir.name]
            file_name = row[METADATA_TABLE.columns.file_name.name]
            key = parent_dir + file_name
            new_row = path_to_file_dict.get(key)

            new_path = new_row.new_path
            new_parent_dir = os.path.dirname(new_path)
            update_row = {
                METADATA_TABLE.columns.id.name: row_id,
                METADATA_TABLE.columns.parent_dir.name: new_parent_dir,
            }
            updated_rows.append(update_row)

        if not updated_rows:
            return

        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def rename_dirs(self, repo_id, renamed_dirs):
        if not renamed_dirs:
            return
        base_sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}` FROM `{METADATA_TABLE.name}` WHERE'
        sql = base_sql
        for d in renamed_dirs:
            parameters = []
            old_path = d.path.rstrip('/')
            parent_dir = os.path.dirname(old_path)
            dir_name = os.path.basename(old_path)
            new_path = d.new_path

            if self.is_excluded_path(old_path):
                continue

            sql += f' (`{METADATA_TABLE.columns.parent_dir.name}` = ? AND `{METADATA_TABLE.columns.file_name.name}` = ?) OR' \
                   f' (`{METADATA_TABLE.columns.parent_dir.name}` LIKE ?)'
            parameters.append(parent_dir)
            parameters.append(dir_name)
            parameters.append(old_path + '%')
            query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])
            sql = base_sql

            if not query_result:
                return

            updated_rows = []
            for row in query_result:
                row_id = row[METADATA_TABLE.columns.id.name]
                p_dir = row[METADATA_TABLE.columns.parent_dir.name]
                name = row[METADATA_TABLE.columns.file_name.name]
                new_parent_dir = os.path.dirname(new_path)
                new_name = os.path.basename(new_path)

                if parent_dir == p_dir and dir_name == name:
                    update_row = {
                        METADATA_TABLE.columns.id.name: row_id,
                        METADATA_TABLE.columns.parent_dir.name: new_parent_dir,
                        METADATA_TABLE.columns.file_name.name: new_name
                    }
                    updated_rows.append(update_row)
                else:
                    old_dir_prefix = os.path.join(parent_dir, dir_name)
                    new_dir_prefix = os.path.join(new_parent_dir, new_name)
                    new_parent_dir = p_dir.replace(old_dir_prefix, new_dir_prefix)

                    update_row = {
                        METADATA_TABLE.columns.id.name: row_id,
                        METADATA_TABLE.columns.parent_dir.name: new_parent_dir,
                    }
                    updated_rows.append(update_row)

            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def move_dirs(self, repo_id, moved_dirs):
        if not moved_dirs:
            return
        base_sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}` FROM `{METADATA_TABLE.name}` WHERE'
        sql = base_sql
        for d in moved_dirs:
            parameters = []
            old_path = d.path.rstrip('/')
            parent_dir = os.path.dirname(old_path)
            dir_name = os.path.basename(old_path)
            if self.is_excluded_path(old_path):
                continue

            new_path = d.new_path
            sql += f' (`{METADATA_TABLE.columns.parent_dir.name}` = ? AND `{METADATA_TABLE.columns.file_name.name}` = ?) OR' \
                   f' (`{METADATA_TABLE.columns.parent_dir.name}` LIKE ?)'
            parameters.append(parent_dir)
            parameters.append(dir_name)
            parameters.append(old_path + '%')
            query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])
            sql = base_sql

            if not query_result:
                continue

            updated_rows = []
            for row in query_result:
                row_id = row[METADATA_TABLE.columns.id.name]
                p_dir = row[METADATA_TABLE.columns.parent_dir.name]
                name = row[METADATA_TABLE.columns.file_name.name]
                new_parent_dir = os.path.dirname(new_path)
                new_name = os.path.basename(new_path)

                if parent_dir == p_dir and dir_name == name:
                    update_row = {
                        METADATA_TABLE.columns.id.name: row_id,
                        METADATA_TABLE.columns.parent_dir.name: new_parent_dir,
                        METADATA_TABLE.columns.file_name.name: new_name,
                    }
                    updated_rows.append(update_row)
                else:
                    old_dir_prefix = os.path.join(parent_dir, dir_name)
                    new_dir_prefix = os.path.join(new_parent_dir, new_name)
                    new_parent_dir = p_dir.replace(old_dir_prefix, new_dir_prefix)

                    update_row = {
                        METADATA_TABLE.columns.id.name: row_id,
                        METADATA_TABLE.columns.parent_dir.name: new_parent_dir,
                    }
                    updated_rows.append(update_row)

            if not updated_rows:
                continue

            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def delete_base(self, repo_id):
        self.metadata_server_api.delete_base(repo_id)
