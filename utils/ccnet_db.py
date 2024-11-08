import os
import configparser
import logging
from seafevents.app.config import get_config


logger = logging.getLogger('seafevents')


def get_ccnet_db_name():
    ccnet_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('CCNET_CONF_DIR')
    if not ccnet_conf_dir:
        error_msg = 'Environment variable ccnet_conf_dir is not define.'
        return None, error_msg

    ccnet_conf_path = os.path.join(ccnet_conf_dir, 'ccnet.conf')
    config = configparser.ConfigParser()
    config.read(ccnet_conf_path)

    if config.has_section('Database'):
        db_name = config.get('Database', 'DB', fallback='ccnet')
    else:
        db_name = 'ccnet'

    if config.get('Database', 'ENGINE') != 'mysql':
        error_msg = 'Failed to init ccnet db, only mysql db supported.'
        return None, error_msg
    return db_name, None


class CcnetDB(object):
    def __init__(self):
        self.ccnet_db_conn = None
        self.ccnet_db_cursor = None
        self.init_ccnet_db()
        self.db_name = get_ccnet_db_name()[0]
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

        ccnet_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('CCNET_CONF_DIR')
        if not ccnet_conf_dir:
            logging.warning('Environment variable ccnet_conf_dir is not define')
            return

        ccnet_conf_path = os.path.join(ccnet_conf_dir, 'ccnet.conf')
        ccnet_config = get_config(ccnet_conf_path)

        if not ccnet_config.has_section('Database'):
            logger.warning('Failed to init ccnet db, can not find db info in ccnet.conf.')
            return

        if ccnet_config.get('Database', 'ENGINE') != 'mysql':
            logger.warning('Failed to init ccnet db, only mysql db supported.')
            return

        db_name = ccnet_config.get('Database', 'DB', fallback='ccnet')
        db_host = ccnet_config.get('Database', 'HOST', fallback='127.0.0.1')
        db_port = ccnet_config.getint('Database', 'PORT', fallback=3306)
        db_user = ccnet_config.get('Database', 'USER')
        db_passwd = ccnet_config.get('Database', 'PASSWD')

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

            return cursor.fetchone()[0]
