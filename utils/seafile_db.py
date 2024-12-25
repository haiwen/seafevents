import os
import configparser
import logging
from seafevents.app.config import get_config


logger = logging.getLogger('seafevents')


# 用于获取 Seafile 数据库的名称
def get_seafile_db_name():
    # 它首先检查环境变量 SEAFILE_CENTRAL_CONF_DIR 或 SEAFILE_CONF_DIR 是否定义
    seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
    if not seafile_conf_dir:
        error_msg = 'Environment variable seafile_conf_dir is not define.'
        return None, error_msg

    # 通过 os.path.join() 函数将 seafile_conf_dir 和 seafile.conf 文件名拼接，生成配置文件路径。
    seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
    config = configparser.ConfigParser()
    config.read(seafile_conf_path)

    # 然后通过 configparser.ConfigParser() 类读取配置文件，并检查配置文件中是否存在 database 部分。
    if config.has_section('database'):
        db_name = config.get('database', 'db_name', fallback='seafile')
    else:
        db_name = 'seafile'

    # 检查配置文件中 database 部分的 type 是否为 mysql
    if config.get('database', 'type') != 'mysql':
        error_msg = 'Failed to init seafile db, only mysql db supported.'
        return None, error_msg
    return db_name, None


# 用于连接和操作 Seafile 数据库
class SeafileDB(object):
    # 初始化类实例，连接到 Seafile 数据库，并检查数据库连接是否成功。
    def __init__(self):
        self.seafile_db_conn = None
        self.seafile_db_cursor = None
        self.init_seafile_db()
        self.db_name = get_seafile_db_name()[0]
        if self.seafile_db_cursor is None:
            raise RuntimeError('Failed to init seafile db.')

    # 用于确保数据库连接在使用后被正确关闭
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_seafile_db()

    # 初始化 Seafile 数据库连接，读取配置文件并连接到数据库。
    def init_seafile_db(self):
        # 安装 pymysql
        try:
            import pymysql
            pymysql.install_as_MySQLdb()
        except ImportError as e:
            logger.warning('Failed to init seafile db: %s.' % e)
            return

        # 读取环境变量
        seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
        if not seafile_conf_dir:
            logging.warning('Environment variable seafile_conf_dir is not define')
            return

        # 读取配置文件路径，读取文件内容
        seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
        seafile_config = get_config(seafile_conf_path)

        # 配置有效性检测
        if not seafile_config.has_section('database'):
            logger.warning('Failed to init seafile db, can not find db info in seafile.conf.')
            return

        if seafile_config.get('database', 'type') != 'mysql':
            logger.warning('Failed to init seafile db, only mysql db supported.')
            return

        # 获取配置中的数据库信息
        db_name = os.environ.get('SEAFILE_MYSQL_DB_SEAFILE_DB_NAME', '') or seafile_config.get('database', 'db_name', fallback='seafile')
        db_host = os.getenv('SEAFILE_MYSQL_DB_HOST') or seafile_config.get('database', 'host', fallback='127.0.0.1')
        db_port = int(os.getenv('SEAFILE_MYSQL_DB_PORT', 0)) or seafile_config.getint('database', 'port', fallback=3306)
        db_user = os.getenv('SEAFILE_MYSQL_DB_USER') or seafile_config.get('database', 'user')
        db_passwd = os.getenv('SEAFILE_MYSQL_DB_PASSWORD') or seafile_config.get('database', 'password')

        # 链接数据库，并自动提交，获取游标保存到类中
        try:
            self.seafile_db_conn = pymysql.connect(host=db_host, port=db_port, user=db_user,
                                                 passwd=db_passwd, db=db_name, charset='utf8')
            self.seafile_db_conn.autocommit(True)
            self.seafile_db_cursor = self.seafile_db_conn.cursor()
        except Exception as e:
            self.cursor = None
            logger.warning('Failed to init seafile db: %s.' % e)
            return

    # 关闭 Seafile 数据库连接（关闭游标+关闭数据库连接）
    def close_seafile_db(self):
        if self.seafile_db_cursor:
            self.seafile_db_cursor.close()
        if self.seafile_db_conn:
            self.seafile_db_conn.close()

    # 转换数据结构：将数据库读出的行，转换成对象输出
    def repo_info(self, item):
        info = {
            'repo_name': item[1],
            'owner': item[2]
        }
        return info

    # 根据资料库 ID，获取资料库信息
    def get_repo_info_by_ids(self, repo_ids):
        # 先把 ids 转换成字符串
        repo_ids_str = ','.join(["'%s'" % str(repo_id) for repo_id in repo_ids])
        # 从 RepoInfo 和 RepoOwner 中查询信息，左连接，当 repo_id 相同，且在 repo_ids_str 中
        sql1 = f"""
        SELECT r.repo_id, name, owner_id
        FROM `{self.db_name}`.`RepoInfo` r
        LEFT JOIN `{self.db_name}`.`RepoOwner` o 
        ON o.repo_id = r.repo_id
        WHERE r.repo_id IN ({repo_ids_str})
        """
        # 联合查询：RepoInfo 和 OrgRepo 获取用户信息
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
            # 执行两个SQL
            cursor.execute(sql1)
            rows1 = cursor.fetchall()
            cursor.execute(sql2)
            rows2 = cursor.fetchall()
            # 合并这两个列表，并转换数据
            rows = rows1 + rows2
            repos_map = {}
            for row in rows:
                if row[0] not in repos_map or repos_map[row[0]]['owner'] is None:
                    repos_map[row[0]] = self.repo_info(row)

            return repos_map

    # 重置下载速率限制表
    def reset_download_rate_limit(self):
        # TRUNCATE TABLE 清空数据，但是不删除表 DROP 是删除表（同时清空两个数据库表）
        sql1 = f"""
                TRUNCATE TABLE `{self.db_name}`.`UserDownloadRateLimit`;
                """
        sql2 = f"""
                TRUNCATE TABLE `{self.db_name}`.`OrgDownloadRateLimit`
                """
        with self.seafile_db_cursor as cursor:
            cursor.execute(sql1)
            cursor.execute(sql2)
