import os
import configparser
import logging
from seafevents.app.config import get_config
from seaserv import seafile_api


logger = logging.getLogger('seafevents')


def get_seafile_db_name():
    seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
    if not seafile_conf_dir:
        error_msg = 'Environment variable seafile_conf_dir is not define.'
        return None, error_msg

    seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
    config = configparser.ConfigParser()
    config.read(seafile_conf_path)

    if config.has_section('database'):
        db_name = config.get('database', 'db_name', fallback='seafile')
    else:
        db_name = 'seafile'

    if config.get('database', 'type') != 'mysql':
        error_msg = 'Failed to init seafile db, only mysql db supported.'
        return None, error_msg
    return db_name, None


class SeafileDB(object):
    def __init__(self):
        self.seafile_db_conn = None
        self.seafile_db_cursor = None
        self.init_seafile_db()
        self.db_name = get_seafile_db_name()[0]
        if self.seafile_db_cursor is None:
            raise RuntimeError('Failed to init seafile db.')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_seafile_db()

    def init_seafile_db(self):
        try:
            import pymysql
            pymysql.install_as_MySQLdb()
        except ImportError as e:
            logger.warning('Failed to init seafile db: %s.' % e)
            return

        seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
        if not seafile_conf_dir:
            logging.warning('Environment variable seafile_conf_dir is not define')
            return

        seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
        seafile_config = get_config(seafile_conf_path)

        if not seafile_config.has_section('database'):
            logger.warning('Failed to init seafile db, can not find db info in seafile.conf.')
            return

        if seafile_config.get('database', 'type') != 'mysql':
            logger.warning('Failed to init seafile db, only mysql db supported.')
            return

        db_name = os.environ.get('SEAFILE_MYSQL_DB_SEAFILE_DB_NAME', '') or seafile_config.get('database', 'db_name', fallback='seafile')
        db_host = os.getenv('SEAFILE_MYSQL_DB_HOST') or seafile_config.get('database', 'host', fallback='127.0.0.1')
        db_port = int(os.getenv('SEAFILE_MYSQL_DB_PORT', 0)) or seafile_config.getint('database', 'port', fallback=3306)
        db_user = os.getenv('SEAFILE_MYSQL_DB_USER') or seafile_config.get('database', 'user')
        db_passwd = os.getenv('SEAFILE_MYSQL_DB_PASSWORD') or seafile_config.get('database', 'password')

        try:
            self.seafile_db_conn = pymysql.connect(host=db_host, port=db_port, user=db_user,
                                                 passwd=db_passwd, db=db_name, charset='utf8')
            self.seafile_db_conn.autocommit(True)
            self.seafile_db_cursor = self.seafile_db_conn.cursor()
        except Exception as e:
            self.cursor = None
            logger.warning('Failed to init seafile db: %s.' % e)
            return

    def close_seafile_db(self):
        if self.seafile_db_cursor:
            self.seafile_db_cursor.close()
        if self.seafile_db_conn:
            self.seafile_db_conn.close()

    def repo_info(self, item):
        info = {
            'repo_name': item[1],
            'owner': item[2]
        }
        return info

    def get_repo_info_by_ids(self, repo_ids):
        repo_ids_str = ','.join(["'%s'" % str(repo_id) for repo_id in repo_ids])
        sql1 = f"""
        SELECT r.repo_id, name, owner_id
        FROM `{self.db_name}`.`RepoInfo` r
        LEFT JOIN `{self.db_name}`.`RepoOwner` o 
        ON o.repo_id = r.repo_id
        WHERE r.repo_id IN ({repo_ids_str})
        """
        sql2 = f"""
        SELECT r.repo_id, name, user
        FROM `{self.db_name}`.`RepoInfo` r
        LEFT JOIN `{self.db_name}`.`OrgRepo` o 
        ON o.repo_id = r.repo_id
        WHERE r.repo_id IN ({repo_ids_str})
        """
        with self.seafile_db_cursor as cursor:
            if not repo_ids:
                return {}
            cursor.execute(sql1)
            rows1 = cursor.fetchall()
            cursor.execute(sql2)
            rows2 = cursor.fetchall()
            rows = rows1 + rows2
            repos_map = {}
            for row in rows:
                if row[0] not in repos_map or repos_map[row[0]]['owner'] is None:
                    repos_map[row[0]] = self.repo_info(row)

            return repos_map

    def reset_download_rate_limit(self):
        sql1 = f"""
                TRUNCATE TABLE `{self.db_name}`.`UserDownloadRateLimit`;
                """
        sql2 = f"""
                TRUNCATE TABLE `{self.db_name}`.`OrgDownloadRateLimit`
                """
        with self.seafile_db_cursor as cursor:
            cursor.execute(sql1)
            cursor.execute(sql2)
            
    
    def get_repo_owner(self, repo_id):
        sql = f"""SELECT owner_id FROM `{self.db_name}`.`RepoOwner` WHERE repo_id="{repo_id}" """
        
        self.seafile_db_cursor.execute(sql)
        row = self.seafile_db_cursor.fetchone()
        if not row:
            return None
        
        return row[0]
        
    
    def get_org_repo_owner(self, repo_id):
        sql = f"""SELECT user FROM `{self.db_name}`.`OrgRepo` WHERE repo_id="{repo_id}" """
        
        self.seafile_db_cursor.execute(sql)
        row = self.seafile_db_cursor.fetchone()
        
        if not row:
            return None
        
        return row[0]
        
    
    def get_user_self_usage(self, email):
        sql = f"""
        SELECT SUM(size) FROM
        `{self.db_name}`.`RepoOwner` o LEFT JOIN `{self.db_name}`.`VirtualRepo` v ON o.repo_id=v.repo_id,
        `{self.db_name}`.`RepoSize` WHERE owner_id="{email}" AND
        o.repo_id=RepoSize.repo_id AND
        v.repo_id IS NULL
        
        """
        self.seafile_db_cursor.execute(sql)
        row = self.seafile_db_cursor.fetchone()
        if not row:
            return None
        
        return row[0]
        
    
    def get_org_user_quota_usage(self, org_id, email):
        sql = f"""
        SELECT SUM(size) FROM
        `{self.db_name}`.`OrgRepo` o LEFT JOIN `{self.db_name}`.`VirtualRepo` v ON o.repo_id=v.repo_id,
        `{self.db_name}`.`RepoSize` WHERE org_id={org_id} AND
         user="{email}" AND
         o.repo_id=RepoSize.repo_id AND
         v.repo_id IS NULL

        """
        
        self.seafile_db_cursor.execute(sql)
        row = self.seafile_db_cursor.fetchone()
        
        if not row:
            return None
        
        return row[0]


    def get_org_id_by_repo_id(self, repo_id):
        sql = f"""SELECT org_id FROM `{self.db_name}`.`OrgRepo` WHERE repo_id="{repo_id}" """
        self.seafile_db_cursor.execute(sql)
        row = self.seafile_db_cursor.fetchone()
        
        if not row:
            return -1
        
        return row[0]
        
    
    def get_org_quota_usage(self, org_id):
        sql = f"""
                SELECT SUM(size) FROM
                `{self.db_name}`.`OrgRepo` o LEFT JOIN `{self.db_name}`.`VirtualRepo` v ON o.repo_id=v.repo_id,
                `{self.db_name}`.`RepoSize` WHERE org_id={org_id} AND
                 o.repo_id=RepoSize.repo_id AND
                 v.repo_id IS NULL

                """
        
        self.seafile_db_cursor.execute(sql)
        row = self.seafile_db_cursor.fetchone()
        
        if not row:
            return None
        
        return row[0]
    

    def get_user_quota(self, email):
        '''
        Geting user / org_user / org quota is related not only to the records in the database，
        but also to the configurations in seafile.conf.

        To simplify the logic here, the seafile_api is used to directly obtain the quota
        instead of directly searching in the database.

        '''
    
        return seafile_api.get_user_quota(email)
    

    def get_org_user_quota(self, org_id, email):
    
        return seafile_api.get_org_user_quota(org_id, email)
    

    def get_org_quota(self, org_id):
    
        return seafile_api.get_org_quota(org_id)
