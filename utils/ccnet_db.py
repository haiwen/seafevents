import os
import configparser
import logging
from seafevents.app.config import get_config
from seafevents.db import init_db_session_class
from sqlalchemy.sql import text


logger = logging.getLogger('seafevents')


def get_ccnet_db_name():
    return os.environ.get('SEAFILE_MYSQL_DB_CCNET_DB_NAME', '') or 'SYSDBA'


class CcnetDB(object):
    def __init__(self):
        self.db_session = None
        self.init_ccnet_db()
        self.db_name = get_ccnet_db_name()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_ccnet_db()

    def init_ccnet_db(self):
        seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
        if not seafile_conf_dir:
            logging.warning('Environment variable seafile_conf_dir is not define')
            return

        seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
        seafile_config = get_config(seafile_conf_path)

        if not seafile_config.has_section('database'):
            logger.warning('Failed to init ccnet db, can not find db info in seafile.conf.')
            return

        self.db_session = init_db_session_class(seafile_config, 'seafile')()

    def close_ccnet_db(self):
        self.db_session.close()

    def get_group_info(self, group):
        info = {
            'group_id': group[0],
            'group_name': group[1],
            'creator_name': group[2],
            'timestamp': group[3],
            'type': group[4],
            'parent_group_id': group[5]
        }
        return info

    def get_groups_by_ids(self, group_ids):
        group_ids_str = ','.join(["'%s'" % str(id) for id in group_ids])
        sql = f"""
            SELECT * 
            FROM
                {self.db_name}.Group
            WHERE
                group_id IN ({group_ids_str})
        """

        if not group_ids:
            return {}
        cursor = self.db_session.execute(text(sql))
        groups_map = {}
        for item in cursor.fetchall():
            groups_map[item[0]] = self.get_group_info(item)

        return groups_map

    def get_org_user_count(self, org_id):
        sql = f"""
        SELECT COUNT(1) FROM {self.db_name}.OrgUser WHERE org_id={org_id}
        """
        return self.db_session.execute(text(sql)).fetchone()[0]

    def get_user_role(self, email):
        sql = f"""
        SELECT role FROM {self.db_name}.UserRole
        WHERE email="{email}"
        """
        result = self.db_session.execute(text(sql)).fetchone()
        return result[0] if result else 'default'
