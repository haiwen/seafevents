import os
import configparser
import logging
from seafevents.app.config import get_config


logger = logging.getLogger('seafevents')


def get_ccnet_db_name():
    return os.environ.get('SEAFILE_MYSQL_DB_CCNET_DB_NAME', '') or 'ccnet_db'


class CcnetDB(object):
    def __init__(self):
        self.ccnet_db_conn = None
        self.ccnet_db_cursor = None
        self.init_ccnet_db()
        self.db_name = get_ccnet_db_name()
        if self.ccnet_db_cursor is None:
            raise RuntimeError('Failed to init ccnet db.')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_ccnet_db()

    def init_ccnet_db(self):
        try:
            import pymysql
            pymysql.install_as_MySQLdb()
        except ImportError as e:
            logger.warning('Failed to init ccnet db: %s.' % e)
            return

        seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
        if not seafile_conf_dir:
            logging.warning('Environment variable seafile_conf_dir is not define')
            return

        seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
        seafile_config = get_config(seafile_conf_path)

        if not seafile_config.has_section('database'):
            logger.warning('Failed to init ccnet db, can not find db info in seafile.conf.')
            return

        if seafile_config.get('database', 'type') != 'mysql':
            logger.warning('Failed to init ccnet db, only mysql db supported.')
            return

        db_name = os.environ.get('SEAFILE_MYSQL_DB_CCNET_DB_NAME', '') or 'ccnet_db'
        db_host = seafile_config.get('database', 'host', fallback='127.0.0.1')
        db_port = seafile_config.getint('database', 'port', fallback=3306)
        db_user = seafile_config.get('database', 'user')
        db_passwd = seafile_config.get('database', 'password')

        try:
            self.ccnet_db_conn = pymysql.connect(host=db_host, port=db_port, user=db_user,
                                                 passwd=db_passwd, db=db_name, charset='utf8')
            self.ccnet_db_conn.autocommit(True)
            self.ccnet_db_cursor = self.ccnet_db_conn.cursor()
        except Exception as e:
            self.cursor = None
            logger.warning('Failed to init ccnet db: %s.' % e)
            return

    def close_ccnet_db(self):
        if self.ccnet_db_cursor:
            self.ccnet_db_cursor.close()
        if self.ccnet_db_conn:
            self.ccnet_db_conn.close()

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
                `{self.db_name}`.`Group`
            WHERE
                group_id IN ({group_ids_str})
        """

        with self.ccnet_db_cursor as cursor:
            if not group_ids:
                return {}
            cursor.execute(sql)
            groups_map = {}
            for item in cursor.fetchall():
                groups_map[item[0]] = self.get_group_info(item)

            return groups_map

    def get_org_user_count(self, org_id):
        sql = f"""
        SELECT COUNT(1) FROM `{self.db_name}`.`OrgUser` WHERE org_id={org_id}
        """
        with self.ccnet_db_cursor as cursor:
            cursor.execute(sql)

            return cursor.fetchone()[0]

    def get_user_role(self, email):
        sql = f"""
        SELECT role FROM `{self.db_name}`.`UserRole`
        WHERE email="{email}"
        """
        with self.ccnet_db_cursor as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()

            return result[0] if result else 'default'
