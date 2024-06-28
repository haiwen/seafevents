import os
import logging

from seafevents.repo_metadata.metadata_server_api import METADATA_TABLE, METADATA_COLUMN_ID, \
    METADATA_COLUMN_CREATOR, METADATA_COLUMN_CREATED_TIME, METADATA_COLUMN_MODIFIER, \
    METADATA_COLUMN_MODIFIED_TIME, METADATA_COLUMN_PARENT_DIR, METADATA_COLUMN_NAME, METADATA_COLUMN_IS_DIR

from seafevents.utils import timestamp_to_isoformat_timestr

logger = logging.getLogger(__name__)

EXCLUDED_PATHS = ['/_Internal', '/images']


class RepoMetadata:

    def __init__(self, metadata_server_api):
        self.metadata_server_api = metadata_server_api

    def update(self, repo_id, added_files, deleted_files, added_dirs, deleted_dirs, modified_files,
                        renamed_files, moved_files, renamed_dirs, moved_dirs):

        # delete added_files delete added dirs for preventing duplicate insertions
        self.delete_files(repo_id, added_files)
        self.delete_dirs(repo_id, added_dirs)

        self.add_files(repo_id, added_files)
        self.delete_files(repo_id, deleted_files)
        self.add_dirs(repo_id, added_dirs)
        self.delete_dirs(repo_id, deleted_dirs)
        self.update_files(repo_id, modified_files)

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

    def add_files(self, repo_id, added_files):
        if not added_files:
            return
        rows = []
        for de in added_files:
            path = de.path.rstrip('/')
            mtime = de.mtime
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            modifier = de.modifier

            if self.is_excluded_path(path):
                continue

            row = {
                METADATA_COLUMN_CREATOR.name: modifier,
                METADATA_COLUMN_CREATED_TIME.name: timestamp_to_isoformat_timestr(mtime),
                METADATA_COLUMN_MODIFIER.name: modifier,
                METADATA_COLUMN_MODIFIED_TIME.name: timestamp_to_isoformat_timestr(mtime),
                METADATA_COLUMN_PARENT_DIR.name: parent_dir,
                METADATA_COLUMN_NAME.name: file_name,
                METADATA_COLUMN_IS_DIR.name: 'False',
            }
            rows.append(row)
        if not rows:
            return
        self.metadata_server_api.insert_rows(repo_id, METADATA_TABLE.id, rows)

    def delete_files(self, repo_id, deleted_files):
        if not deleted_files:
            return

        sql = f'SELECT `{METADATA_COLUMN_ID.name}` FROM `{METADATA_TABLE.name}` WHERE'
        need_deleted = False
        for file in deleted_files:
            path = file.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            need_deleted = True
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            sql += f' (`{METADATA_COLUMN_PARENT_DIR.name}` = "{parent_dir}" AND `{METADATA_COLUMN_NAME.name}` = "{file_name}") OR'

        if not need_deleted:
            return
        sql = sql.rstrip(' OR')
        query_result = self.metadata_server_api.query_rows(repo_id, sql).get('results', [])

        if not query_result:
            return

        row_ids = []
        for row in query_result:
            row_ids.append(row[METADATA_COLUMN_ID.name])

        self.metadata_server_api.delete_rows(repo_id, METADATA_TABLE.id, row_ids)

    def update_files(self, repo_id, modified_files):
        if not modified_files:
            return

        sql = f'SELECT `{METADATA_COLUMN_ID.name}`, `{METADATA_COLUMN_PARENT_DIR.name}`, `{METADATA_COLUMN_NAME.name}` FROM `{METADATA_TABLE.name}` WHERE'
        path_to_file_dict = {}
        need_update = False
        for file in modified_files:
            path = file.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            need_update = True
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            key = parent_dir + file_name
            path_to_file_dict[key] = file

            sql += f' (`{METADATA_COLUMN_PARENT_DIR.name}` = "{parent_dir}" AND `{METADATA_COLUMN_NAME.name}` = "{file_name}") OR'

        if not need_update:
            return
        sql = sql.rstrip(' OR')
        query_result = self.metadata_server_api.query_rows(repo_id, sql).get('results', [])

        if not query_result:
            return

        updated_rows = []
        for row in query_result:
            row_id = row[METADATA_COLUMN_ID.name]
            parent_dir = row[METADATA_COLUMN_PARENT_DIR.name]
            file_name = row[METADATA_COLUMN_NAME.name]
            key = parent_dir + file_name
            new_row = path_to_file_dict.get(key)

            update_row = {
                METADATA_COLUMN_ID.name: row_id,
                METADATA_COLUMN_MODIFIER.name: new_row.modifier,
                METADATA_COLUMN_MODIFIED_TIME.name: timestamp_to_isoformat_timestr(new_row.mtime),
            }
            updated_rows.append(update_row)

        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

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
                METADATA_COLUMN_CREATOR.name: '',
                METADATA_COLUMN_CREATED_TIME.name: timestamp_to_isoformat_timestr(mtime),
                METADATA_COLUMN_MODIFIER.name: '',
                METADATA_COLUMN_MODIFIED_TIME.name: timestamp_to_isoformat_timestr(mtime),
                METADATA_COLUMN_PARENT_DIR.name: parent_dir,
                METADATA_COLUMN_NAME.name: file_name,
                METADATA_COLUMN_IS_DIR.name: 'True',

            }
            rows.append(row)

        if not rows:
            return

        self.metadata_server_api.insert_rows(repo_id, METADATA_TABLE.id, rows)

    def delete_dirs(self, repo_id, deleted_dirs):
        if not deleted_dirs:
            return
        sql = f'SELECT `{METADATA_COLUMN_ID.name}` FROM `{METADATA_TABLE.name}` WHERE'
        need_delete = False
        for d in deleted_dirs:
            path = d.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            need_delete = True
            parent_dir = os.path.dirname(path)
            dir_name = os.path.basename(path)
            sql += f' (`{METADATA_COLUMN_PARENT_DIR.name}` = "{parent_dir}" AND `{METADATA_COLUMN_NAME.name}` = "{dir_name}") OR'

        if not need_delete:
            return
        sql = sql.rstrip(' OR')
        query_result = self.metadata_server_api.query_rows(repo_id, sql).get('results', [])

        if not query_result:
            return

        row_ids = []
        for row in query_result:
            row_ids.append(row[METADATA_COLUMN_ID.name])

        self.metadata_server_api.delete_rows(repo_id, METADATA_TABLE.id, row_ids)

    def rename_files(self, repo_id, renamed_files):
        if not renamed_files:
            return

        sql = f'SELECT `{METADATA_COLUMN_ID.name}`, `{METADATA_COLUMN_PARENT_DIR.name}`, `{METADATA_COLUMN_NAME.name}` FROM `{METADATA_TABLE.name}` WHERE'
        path_to_file_dict = {}
        need_update = False
        for file in renamed_files:
            path = file.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            need_update = True
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            key = parent_dir + file_name
            path_to_file_dict[key] = file

            sql += f' (`{METADATA_COLUMN_PARENT_DIR.name}` = "{parent_dir}" AND `{METADATA_COLUMN_NAME.name}` = "{file_name}") OR'
        if not need_update:
            return
        sql = sql.rstrip(' OR')
        query_result = self.metadata_server_api.query_rows(repo_id, sql).get('results', [])

        if not query_result:
            return

        updated_rows = []
        for row in query_result:
            row_id = row[METADATA_COLUMN_ID.name]
            parent_dir = row[METADATA_COLUMN_PARENT_DIR.name]
            file_name = row[METADATA_COLUMN_NAME.name]
            key = parent_dir + file_name
            new_row = path_to_file_dict.get(key)

            new_path = new_row.new_path
            new_parent_dir = os.path.dirname(new_path)
            new_file_name = os.path.basename(new_path)

            update_row = {
                METADATA_COLUMN_ID.name: row_id,
                METADATA_COLUMN_PARENT_DIR.name: new_parent_dir,
                METADATA_COLUMN_NAME.name: new_file_name,
                METADATA_COLUMN_MODIFIER.name: new_row.modifier,
                METADATA_COLUMN_MODIFIED_TIME.name: timestamp_to_isoformat_timestr(new_row.mtime),
            }
            updated_rows.append(update_row)

        if not updated_rows:
            return

        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def move_files(self, repo_id, moved_files):
        if not moved_files:
            return

        sql = f'SELECT `{METADATA_COLUMN_ID.name}`, `{METADATA_COLUMN_PARENT_DIR.name}`, `{METADATA_COLUMN_NAME.name}` FROM `{METADATA_TABLE.name}` WHERE'
        path_to_file_dict = {}
        need_update = False
        for file in moved_files:
            path = file.path.rstrip('/')
            if self.is_excluded_path(path):
                continue
            need_update = True
            parent_dir = os.path.dirname(path)
            file_name = os.path.basename(path)
            key = parent_dir + file_name
            path_to_file_dict[key] = file

            sql += f' (`{METADATA_COLUMN_PARENT_DIR.name}` = "{parent_dir}" AND `{METADATA_COLUMN_NAME.name}` = "{file_name}") OR'

        if not need_update:
            return
        sql = sql.rstrip(' OR')
        query_result = self.metadata_server_api.query_rows(repo_id, sql).get('results', [])

        if not query_result:
            return

        updated_rows = []
        for row in query_result:
            row_id = row[METADATA_COLUMN_ID.name]
            parent_dir = row[METADATA_COLUMN_PARENT_DIR.name]
            file_name = row[METADATA_COLUMN_NAME.name]
            key = parent_dir + file_name
            new_row = path_to_file_dict.get(key)

            new_path = new_row.new_path
            new_parent_dir = os.path.dirname(new_path)
            update_row = {
                METADATA_COLUMN_ID.name: row_id,
                METADATA_COLUMN_PARENT_DIR.name: new_parent_dir,
            }
            updated_rows.append(update_row)

        if not updated_rows:
            return

        self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def rename_dirs(self, repo_id, renamed_dirs):
        if not renamed_dirs:
            return
        sql = f'SELECT `{METADATA_COLUMN_ID.name}`, `{METADATA_COLUMN_PARENT_DIR.name}`, `{METADATA_COLUMN_NAME.name}` FROM `{METADATA_TABLE.name}` WHERE'
        for d in renamed_dirs:
            old_path = d.path.rstrip('/')
            parent_dir = os.path.dirname(old_path)
            dir_name = os.path.basename(old_path)
            new_path = d.new_path

            if self.is_excluded_path(old_path):
                continue

            sql += f' (`{METADATA_COLUMN_PARENT_DIR.name}` = "{parent_dir}" AND `{METADATA_COLUMN_NAME.name}` = "{dir_name}") OR' \
                   f' (`{METADATA_COLUMN_PARENT_DIR.name}` LIKE "{old_path}%")'
            query_result = self.metadata_server_api.query_rows(repo_id, sql).get('results', [])

            if not query_result:
                return

            updated_rows = []
            for row in query_result:
                row_id = row[METADATA_COLUMN_ID.name]
                p_dir = row[METADATA_COLUMN_PARENT_DIR.name]
                name = row[METADATA_COLUMN_NAME.name]
                new_parent_dir = os.path.dirname(new_path)
                new_name = os.path.basename(new_path)

                if parent_dir == p_dir and dir_name == name:
                    update_row = {
                        METADATA_COLUMN_ID.name: row_id,
                        METADATA_COLUMN_PARENT_DIR.name: new_parent_dir,
                        METADATA_COLUMN_NAME.name: new_name
                    }
                    updated_rows.append(update_row)
                else:
                    old_dir_prefix = os.path.join(parent_dir, dir_name)
                    new_dir_prefix = os.path.join(new_parent_dir, new_name)
                    new_parent_dir = p_dir.replace(old_dir_prefix, new_dir_prefix)

                    update_row = {
                        METADATA_COLUMN_ID.name: row_id,
                        METADATA_COLUMN_PARENT_DIR.name: new_parent_dir,
                    }
                    updated_rows.append(update_row)

            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def move_dirs(self, repo_id, moved_dirs):
        if not moved_dirs:
            return
        sql = f'SELECT `{METADATA_COLUMN_ID.name}`, `{METADATA_COLUMN_PARENT_DIR.name}`, `{METADATA_COLUMN_NAME.name}` FROM `{METADATA_TABLE.name}` WHERE'
        for d in moved_dirs:
            old_path = d.path.rstrip('/')
            parent_dir = os.path.dirname(old_path)
            dir_name = os.path.basename(old_path)
            if self.is_excluded_path(old_path):
                continue

            new_path = d.new_path
            sql += f' (`{METADATA_COLUMN_PARENT_DIR.name}` = "{parent_dir}" AND `{METADATA_COLUMN_NAME.name}` = "{dir_name}") OR' \
                   f' (`{METADATA_COLUMN_PARENT_DIR.name}` LIKE "{old_path}%")'
            query_result = self.metadata_server_api.query_rows(repo_id, sql).get('results', [])

            if not query_result:
                return

            updated_rows = []
            for row in query_result:
                row_id = row[METADATA_COLUMN_ID.name]
                p_dir = row[METADATA_COLUMN_PARENT_DIR.name]
                name = row[METADATA_COLUMN_NAME.name]
                new_parent_dir = os.path.dirname(new_path)
                new_name = os.path.basename(new_path)

                if parent_dir == p_dir and dir_name == name:
                    update_row = {
                        METADATA_COLUMN_ID.name: row_id,
                        METADATA_COLUMN_PARENT_DIR.name: new_parent_dir,
                        METADATA_COLUMN_NAME.name: new_name,
                    }
                    updated_rows.append(update_row)
                else:
                    old_dir_prefix = os.path.join(parent_dir, dir_name)
                    new_dir_prefix = os.path.join(new_parent_dir, new_name)
                    new_parent_dir = p_dir.replace(old_dir_prefix, new_dir_prefix)

                    update_row = {
                        METADATA_COLUMN_ID.name: row_id,
                        METADATA_COLUMN_PARENT_DIR.name: new_parent_dir,
                    }
                    updated_rows.append(update_row)

            if not updated_rows:
                continue

            self.metadata_server_api.update_rows(repo_id, METADATA_TABLE.id, updated_rows)

    def delete_base(self, repo_id):
        self.metadata_server_api.delete_base(repo_id)

    def init_columns(self, repo_id):
        # initial md-server base and insert records
        # Add columns: creator, created_time, modifier, modified_time, parent_dir, name
        self.metadata_server_api.add_column(repo_id, METADATA_TABLE.id, METADATA_COLUMN_CREATOR.to_build_column_dict())
        self.metadata_server_api.add_column(repo_id, METADATA_TABLE.id, METADATA_COLUMN_CREATED_TIME.to_build_column_dict())
        self.metadata_server_api.add_column(repo_id, METADATA_TABLE.id, METADATA_COLUMN_MODIFIER.to_build_column_dict())
        self.metadata_server_api.add_column(repo_id, METADATA_TABLE.id, METADATA_COLUMN_MODIFIED_TIME.to_build_column_dict())
        self.metadata_server_api.add_column(repo_id, METADATA_TABLE.id, METADATA_COLUMN_PARENT_DIR.to_build_column_dict())
        self.metadata_server_api.add_column(repo_id, METADATA_TABLE.id, METADATA_COLUMN_NAME.to_build_column_dict())
        self.metadata_server_api.add_column(repo_id, METADATA_TABLE.id, METADATA_COLUMN_IS_DIR.to_build_column_dict())

    def create_base(self, repo_id):
        self.metadata_server_api.create_base(repo_id)
        self.init_columns(repo_id)
