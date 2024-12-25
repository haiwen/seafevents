import os
import configparser
import logging
from seafevents.app.config import get_config


logger = logging.getLogger('seafevents')


# 获取数据库名：先检查环境变量 SEAFILE_MYSQL_DB_CCNET_DB_NAME 是否设置自定义名称。如果未设置，则默认返回 'ccnet_db'。
def get_ccnet_db_name():
    return os.environ.get('SEAFILE_MYSQL_DB_CCNET_DB_NAME', '') or 'ccnet_db'


# CcnetDB 类：用于连接到 MySQL 数据库（ccnet 数据库），它处理数据库初始化、连接和查询执行。
class CcnetDB(object):

    # 初始化数据库连接，并设置数据库名称
    def __init__(self):
        self.ccnet_db_conn = None
        self.ccnet_db_cursor = None
        # 初始化数据库
        self.init_ccnet_db()
        # 获取数据库名称
        self.db_name = get_ccnet_db_name()
        # 如果初始化后没有数据库连接，则抛出异常，连接 ccnet 失败
        if self.ccnet_db_cursor is None:
            raise RuntimeError('Failed to init ccnet db.')

    # 返回类实例
    def __enter__(self):
        return self

    # 退出时关闭数据库连接
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_ccnet_db()

    # 初始化 ccnet 数据库
    # 读取 seafile.conf 配置文件，并连接到 MySQL 数据库，来初始化数据库连接。
    def init_ccnet_db(self):
        try:
            # 可以使用 pymysql 库来连接到 MySQL 数据库，而不需要修改原来的代码。
            import pymysql
            pymysql.install_as_MySQLdb()
        except ImportError as e:
            logger.warning('Failed to init ccnet db: %s.' % e)
            return

        # 从环境变量中获取 seafile.conf 文件路径。
        seafile_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR') or os.environ.get('SEAFILE_CONF_DIR')
        if not seafile_conf_dir:
            logging.warning('Environment variable seafile_conf_dir is not define')
            return

        # 读取 seafile.conf 文件，获取数据库连接信息。
        seafile_conf_path = os.path.join(seafile_conf_dir, 'seafile.conf')
        # 读取配置文件内容
        seafile_config = get_config(seafile_conf_path)

        if not seafile_config.has_section('database'):
            logger.warning('Failed to init ccnet db, can not find db info in seafile.conf.')
            return

        if seafile_config.get('database', 'type') != 'mysql':
            logger.warning('Failed to init ccnet db, only mysql db supported.')
            return

        # 获取配置
        db_name = os.environ.get('SEAFILE_MYSQL_DB_CCNET_DB_NAME', '') or 'ccnet_db'
        db_host = os.getenv('SEAFILE_MYSQL_DB_HOST') or seafile_config.get('database', 'host', fallback='127.0.0.1')
        db_port = int(os.getenv('SEAFILE_MYSQL_DB_PORT', 0)) or seafile_config.getint('database', 'port', fallback=3306)
        db_user = os.getenv('SEAFILE_MYSQL_DB_USER') or seafile_config.get('database', 'user')
        db_passwd = os.getenv('SEAFILE_MYSQL_DB_PASSWORD') or seafile_config.get('database', 'password')

        try:
            self.ccnet_db_conn = pymysql.connect(host=db_host, port=db_port, user=db_user,
                                                 passwd=db_passwd, db=db_name, charset='utf8')
            # 设置自动提交                                #    
            self.ccnet_db_conn.autocommit(True)
            # 创建游标
            self.ccnet_db_cursor = self.ccnet_db_conn.cursor()
        except Exception as e:
            self.cursor = None
            logger.warning('Failed to init ccnet db: %s.' % e)
            return

    # 关闭游标，关闭数据库连接
    def close_ccnet_db(self):
        if self.ccnet_db_cursor:
            self.ccnet_db_cursor.close()
        if self.ccnet_db_conn:
            self.ccnet_db_conn.close()

    # 把群组列表转换为字典
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

    # 查询多个群组信息（使用 group_ids）
    def get_groups_by_ids(self, group_ids):
        # 把群组列表转换为字符串
        group_ids_str = ','.join(["'%s'" % str(id) for id in group_ids])

        # 查询数据库，找到 group_id 对应的群组
        sql = f"""
            SELECT * 
            FROM
                `{self.db_name}`.`Group`
            WHERE
                group_id IN ({group_ids_str})
        """

        # 查询
        with self.ccnet_db_cursor as cursor:
            # 没有参数，返回空
            if not group_ids:
                return {}
            # 执行 sql 语句
            cursor.execute(sql)
            groups_map = {}
            # 查询全部，把列表结果转换成字典
            # fetchall() 方法会从结果集中获取所有行，并将它们作为一个列表返回。
            for item in cursor.fetchall():
                groups_map[item[0]] = self.get_group_info(item)

            return groups_map

    # 获取某个机构的用户数
    def get_org_user_count(self, org_id):
        # COUNT(1) 函数统计列中非空值的数量，而 1 是一个占位符。
        sql = f"""
        SELECT COUNT(1) FROM `{self.db_name}`.`OrgUser` WHERE org_id={org_id}
        """
        with self.ccnet_db_cursor as cursor:
            cursor.execute(sql)

            # 用于从数据库游标中获取下一行的第一个列值。
            return cursor.fetchone()[0]

    # 获取用户的角色
    def get_user_role(self, email):
        sql = f"""
        SELECT role FROM `{self.db_name}`.`UserRole`
        WHERE email="{email}"
        """
        with self.ccnet_db_cursor as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()

            return result[0] if result else 'default'
