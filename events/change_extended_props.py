import os
import logging

from seafevents.app.config import DTABLE_WEB_SERVER, SEATABLE_EX_PROPS_BASE_API_TOKEN, EX_PROPS_TABLE
from seafevents.utils.seatable_api import SeaTableAPI

logger = logging.getLogger(__name__)


class ChangeExtendedPropsHandler:

    def __init__(self):
        self.need_change_ex_props = all((DTABLE_WEB_SERVER, SEATABLE_EX_PROPS_BASE_API_TOKEN, EX_PROPS_TABLE))

    def change_file_ex_props(self, repo_id, path, new_path):
        if not self.need_change_ex_props:
            return
        try:
            self._change_file_ex_props(repo_id, path, new_path)
        except Exception as e:
            logger.warning('change file ex-props repo: %s path: %s new_path: %s error: %s', repo_id, path, new_path, e)

    def _change_file_ex_props(self, repo_id, path, new_path):
        try:
            seatable_api = SeaTableAPI(SEATABLE_EX_PROPS_BASE_API_TOKEN, DTABLE_WEB_SERVER)
        except Exception as e:
            logger.error('DTABLE_WEB_SERVER: %s, SEATABLE_EX_PROPS_BASE_API_TOKEN: %s auth error: %s', DTABLE_WEB_SERVER, SEATABLE_EX_PROPS_BASE_API_TOKEN, e)
            return
        file_name = os.path.basename(new_path)
        sql = f"UPDATE `{EX_PROPS_TABLE}` SET `Path`='{new_path}', `File`='{file_name}' WHERE `Repo ID`='{repo_id}' AND `Path`='{path}'"
        logger.info('sql: %s', sql)
        try:
            seatable_api.query(sql)
        except Exception as e:
            logger.error('update ex-props repo: %s path: %s new_path: %s', repo_id, path, new_path)

    def delete_file_ex_props(self, repo_id, path):
        if not self.need_change_ex_props:
            return
        try:
            self._delete_file_ex_props(repo_id, path)
        except Exception as e:
            logger.warning('delete file ex-props repo: %s path: %s error: %s', repo_id, path, e)

    def _delete_file_ex_props(self, repo_id, path):
        try:
            seatable_api = SeaTableAPI(SEATABLE_EX_PROPS_BASE_API_TOKEN, DTABLE_WEB_SERVER)
        except Exception as e:
            logger.error('DTABLE_WEB_SERVER: %s, SEATABLE_EX_PROPS_BASE_API_TOKEN: %s auth error: %s', DTABLE_WEB_SERVER, SEATABLE_EX_PROPS_BASE_API_TOKEN, e)
            return
        sql = f"DELETE FROM `{EX_PROPS_TABLE}` WHERE `Repo ID`='{repo_id}' AND `Path`='{path}'"
        logger.info('sql: %s', sql)
        try:
            seatable_api.query(sql)
        except Exception as e:
            logger.error('update ex-props repo: %s path: %s', repo_id, path)

    def change_dir_ex_props(self, repo_id, path, new_path):
        if not self.need_change_ex_props:
            return
        try:
            self._change_dir_ex_props(repo_id, path, new_path)
        except Exception as e:
            logger.warning('change dir ex-props repo: %s path: %s new_path: %s error: %s', repo_id, path, new_path, e)

    def _change_dir_ex_props(self, repo_id, path, new_path):
        self._change_file_ex_props(repo_id, path, new_path)
        sql_temp = f"SELECT `_id`, `Path` FROM `{EX_PROPS_TABLE}` WHERE `Repo ID`='{repo_id}' AND `Path` LIKE '{path}/%'"
        path_column = None
        try:
            seatable_api = SeaTableAPI(SEATABLE_EX_PROPS_BASE_API_TOKEN, DTABLE_WEB_SERVER)
            step = 1000
            metadata = None
            while True:
                sql = sql_temp + f' LIMIT {step}'
                resp_json = seatable_api.query(sql)
                results = resp_json['results']
                metadata = resp_json['metadata']
                if not path_column:
                    for column in metadata:
                        if column['name'] == 'Path':
                            path_column = column
                            break
                if not path_column:  # It usually doesn't run here
                    logger.error('No Path column found!')
                    return
                for row in results:
                    row_id, row_path = row['_id'], row[path_column['key']]
                    new_row_path = row_path.replace(path, new_path, 1)
                    update_sql = f"UPDATE `{EX_PROPS_TABLE}` SET `Path`='{new_row_path}' WHERE `_id`='{row_id}'"
                    logger.info('update_sql: %s', update_sql)
                    seatable_api.query(update_sql)
                if len(results) < step:
                    break
        except Exception as e:
            logger.error('DTABLE_WEB_SERVER: %s, SEATABLE_EX_PROPS_BASE_API_TOKEN: %s auth error: %s', DTABLE_WEB_SERVER, SEATABLE_EX_PROPS_BASE_API_TOKEN, e)
            return

    def delete_dir_ex_props(self, repo_id, path):
        if not self.need_change_ex_props:
            return
        try:
            self._delete_dir_ex_props(repo_id, path)
        except Exception as e:
            logger.warning('delete dir ex-props repo: %s path: %s error: %s', repo_id, path, e)

    def _delete_dir_ex_props(self, repo_id, path):
        try:
            seatable_api = SeaTableAPI(SEATABLE_EX_PROPS_BASE_API_TOKEN, DTABLE_WEB_SERVER)
        except Exception as e:
            logger.error('DTABLE_WEB_SERVER: %s, SEATABLE_EX_PROPS_BASE_API_TOKEN: %s auth error: %s', DTABLE_WEB_SERVER, SEATABLE_EX_PROPS_BASE_API_TOKEN, e)
            return
        sql = f"DELETE FROM `{EX_PROPS_TABLE}` WHERE `Repo ID`='{repo_id}' AND `Path`='{path}'"
        sub_items_sql = f"DELETE FROM `{EX_PROPS_TABLE}` WHERE `Repo ID`='{repo_id}' AND `Path` LIKE '{path}/%'"
        logger.info('sql: %s sub_items_sql: %s', sql, sub_items_sql)
        try:
            seatable_api.query(sql)
            seatable_api.query(sub_items_sql)
        except Exception as e:
            logger.error('update ex-props repo: %s path: %s', repo_id, path)
