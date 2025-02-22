import os
import configparser
import logging
from seafevents.app.config import get_config
from seafevents.db import init_db_session_class
from sqlalchemy.sql import text

logger = logging.getLogger('seafevents')


def get_seafile_db_name():
    return os.environ.get('SEAFILE_MYSQL_DB_SEAFILE_DB_NAME', '') or 'SYSDBA'


class SeafileDB(object):
    def __init__(self):
        self.db_session = None
        self.init_seafile_db()
        self.db_name = get_seafile_db_name()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_seafile_db()

    def init_seafile_db(self):
        seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
        if not seafile_conf_dir:
            logging.warning('Environment variable seafile_conf_dir is not define')
            return

        seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
        seafile_config = get_config(seafile_conf_path)

        self.db_session = init_db_session_class(seafile_config, 'seafile')()

    def close_seafile_db(self):
        self.db_session.close()

    def repo_info(self, item):
        info = {
            'repo_name': item[1],
            'owner': item[2]
        }
        return info

    def get_repo_info_by_ids(self, repo_ids):
        if not repo_ids:
            return {}
        repo_ids_str = ','.join(["'%s'" % str(repo_id) for repo_id in repo_ids])
        sql1 = f"""
        SELECT r.repo_id, name, owner_id
        FROM {self.db_name}.RepoInfo r
        LEFT JOIN {self.db_name}.RepoOwner o 
        ON o.repo_id = r.repo_id
        WHERE r.repo_id IN ({repo_ids_str})
        """
        sql2 = f"""
        SELECT r.repo_id, name, user
        FROM {self.db_name}.RepoInfo r
        LEFT JOIN {self.db_name}.OrgRepo o 
        ON o.repo_id = r.repo_id
        WHERE r.repo_id IN ({repo_ids_str})
        """
        cursor = self.db_session.execute(text(sql1))
        rows1 = cursor.fetchall()
        cursor = self.db_session.execute(text(sql2))
        rows2 = cursor.fetchall()
        rows = rows1 + rows2
        repos_map = {}
        for row in rows:
            if row[0] not in repos_map or repos_map[row[0]]['owner'] is None:
                repos_map[row[0]] = self.repo_info(row)

        return repos_map

    def reset_download_rate_limit(self):
        sql1 = f"""
                TRUNCATE TABLE {self.db_name}.UserDownloadRateLimit;
                """
        sql2 = f"""
                TRUNCATE TABLE {self.db_name}.OrgDownloadRateLimit
                """

        self.db_session.execute(sql1)
        self.db_session.execute(sql2)
