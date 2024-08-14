import os
import logging

from seafevents.repo_metadata.utils import METADATA_TABLE, get_file_type_ext_by_name, get_latlng
from seafevents.utils import timestamp_to_isoformat_timestr
from seaserv import seafile_api

logger = logging.getLogger(__name__)

EXCLUDED_PATHS = ['/_Internal', '/images']
METADATA_OP_LIMIT = 1000


class RepoMetadata:

    def __init__(self, metadata_server_api):
        self.metadata_server_api = metadata_server_api

    def update(self, repo_id, added_files, deleted_files, added_dirs, deleted_dirs, modified_files,
                        renamed_files, moved_files, renamed_dirs, moved_dirs, commit_id):

        # delete added_files delete added dirs for preventing duplicate insertions
        self.delete_files(repo_id, added_files)
        self.delete_dirs(repo_id, added_dirs)

        self.add_files(repo_id, added_files, commit_id)
        self.delete_files(repo_id, deleted_files)
        self.add_dirs(repo_id, added_dirs)
        self.delete_dirs(repo_id, deleted_dirs)
        self.update_files(repo_id, modified_files, commit_id)

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

    def update_dir_rows_by_query(self, repo_id, sql, parameters, path_to_obj_id):
        query_result = self.metadata_server_api.query_rows(repo_id, sql, parameters).get('results', [])

        if not query_result:
            return

        updated_rows = []
        for row in query_result:
            row_id = row[METADATA_TABLE.columns.id.name]
            parent_dir = row[METADATA_TABLE.columns.parent_dir.name]
            file_name = row[METADATA_TABLE.columns.file_name.name]
            key = os.path.join(parent_dir, file_name)
            obj_id = path_to_obj_id.get(key)

            update_row = {
                METADATA_TABLE.columns.id.name: row_id,
                METADATA_TABLE.columns.obj_id.name: obj_id
            }
            updated_rows.append(update_row)

            if len(updated_rows) >= METADATA_OP_LIMIT:
                self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)
                updated_rows = []

        if not updated_rows:
            return
        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def add_files(self, repo_id, added_files, commit_id):
        if not added_files:
            return

        rows = []
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
            if file_type == '_picture' and file_ext != 'png':
                obj_id = de.obj_id
                try:
                    lat, lng = get_latlng(repo_id, commit_id, obj_id)
                    row[METADATA_TABLE.columns.location.name] = {'lng': lng, 'lat': lat}
                except:
                    pass
            rows.append(row)

            if len(rows) >= METADATA_OP_LIMIT:
                self.metadata_server_api.insert_rows(repo_id, METADATA_TABLE.id, rows)
                rows = []
        if not rows:
            return
        self.metadata_server_api.insert_rows(repo_id, METADATA_TABLE.id, rows)

    def delete_files(self, repo_id, deleted_files):
        if not deleted_files:
            return

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

            if len(parameters) >= METADATA_OP_LIMIT:
                sql = sql.rstrip(' OR')
                self.delete_rows_by_query(repo_id, sql, parameters)
                sql = base_sql
                parameters = []

        if not parameters:
            return
        sql = sql.rstrip(' OR')
        self.delete_rows_by_query(repo_id, sql, parameters)

    def update_files(self, repo_id, modified_files, commit_id):
        if not modified_files:
            return

        base_sql = f'SELECT `{METADATA_TABLE.columns.id.name}`, `{METADATA_TABLE.columns.parent_dir.name}`, `{METADATA_TABLE.columns.file_name.name}` FROM `{METADATA_TABLE.name}` WHERE'
        sql = base_sql
        path_to_file_dict = {}
        parameters = []

        parent_dir_sql = base_sql
        dir_parameters = []
        path_to_obj_id = {}
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

            # update parent dir obj_id
            dir_paths = parent_dir.strip('/').split('/')
            # ['level1 folder', 'level2 folder', 'level3 folder']
            dir_parent_dir_name = '/'
            for dir_path in dir_paths:
                parent_dir_sql += f' (`{METADATA_TABLE.columns.parent_dir.name}` = ? AND `{METADATA_TABLE.columns.file_name.name}` = ?) OR'
                parent_dir_parameter = dir_parent_dir_name

                dir_parameters.append(parent_dir_parameter)
                dir_parameters.append(dir_path)

                if dir_parent_dir_name == '/':
                    dir_parent_dir_name = os.path.join(dir_parent_dir_name, dir_path)
                else:
                    dir_parent_dir_name = os.path.join(dir_parent_dir_name.rstrip('/'), dir_path)

                obj_id = seafile_api.get_dir_id_by_commit_and_path(repo_id, commit_id, dir_parent_dir_name)
                path_to_obj_id[dir_parent_dir_name] = obj_id

            if len(dir_parameters) >= METADATA_OP_LIMIT:
                parent_dir_sql = parent_dir_sql.rstrip(' OR')
                self.update_dir_rows_by_query(repo_id, parent_dir_sql, dir_parameters, path_to_obj_id)
                parent_dir_sql = base_sql
                dir_parameters = []
                path_to_obj_id = {}

        if parameters:
            sql = sql.rstrip(' OR')
            self.update_rows_by_query(repo_id, sql, parameters, path_to_file_dict)

        if dir_parameters:
            parent_dir_sql = parent_dir_sql.rstrip(' OR')
            self.update_dir_rows_by_query(repo_id, parent_dir_sql, dir_parameters, path_to_obj_id)

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
            obj_id = de.obj_id

            row = {
                METADATA_TABLE.columns.file_creator.name: '',
                METADATA_TABLE.columns.file_ctime.name: timestamp_to_isoformat_timestr(mtime),
                METADATA_TABLE.columns.file_modifier.name: '',
                METADATA_TABLE.columns.file_mtime.name: timestamp_to_isoformat_timestr(mtime),
                METADATA_TABLE.columns.parent_dir.name: parent_dir,
                METADATA_TABLE.columns.file_name.name: file_name,
                METADATA_TABLE.columns.is_dir.name: True,
                METADATA_TABLE.columns.obj_id.name: obj_id,

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
