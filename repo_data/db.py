import logging
import configparser
import os

from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.event import contains as has_event_listener, listen as add_event_listener
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import Pool


# base class of model classes in events.models and stats.models
class Base(DeclarativeBase):
    pass


logger = logging.getLogger('seafes')

# 该函数从配置文件创建一个 SQLAlchemy 引擎。
# 它从配置文件中读取数据库类型、主机、端口、用户名、密码和数据库名称，构造一个数据库 URL
# 然后使用该 URL 创建一个 SQLAlchemy 引擎。
# 如果数据库类型不是 'mysql'，则会引发一个错误。引擎配置了连接池和一个 ping 函数来处理连接超时。
def create_engine_from_conf(config_file):
    seaf_conf = configparser.ConfigParser()
    seaf_conf.read(config_file)

    if seaf_conf.has_section('database'):
        if (backend := seaf_conf.get('database', 'type')) == 'mysql':
            db_server = 'localhost'
            db_port = 3306

            if seaf_conf.has_option('database', 'host'):
                db_server = seaf_conf.get('database', 'host')
            if seaf_conf.has_option('database', 'port'):
                db_port =seaf_conf.getint('database', 'port')
            db_username = seaf_conf.get('database', 'user')
            db_passwd = seaf_conf.get('database', 'password')
            db_name = seaf_conf.get('database', 'db_name')
        else:
            logger.critical("Unknown Database backend: %s" % backend)
            raise RuntimeError("Unknown Database backend: %s" % backend)

    db_server = os.getenv('SEAFILE_MYSQL_DB_HOST') or db_server
    db_port = int(os.getenv('SEAFILE_MYSQL_DB_PORT', 0)) or db_port
    db_username = os.getenv('SEAFILE_MYSQL_DB_USER') or db_username
    db_passwd = os.getenv('SEAFILE_MYSQL_DB_PASSWORD') or db_passwd
    db_name = os.getenv('SEAFILE_MYSQL_DB_SEAFILE_DB_NAME') or db_name

    db_url = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % \
        (db_username, quote_plus(db_passwd),
        db_server, db_port, db_name)
    kwargs = dict(pool_recycle=300, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)
    if not has_event_listener(Pool, 'checkout', ping_connection):
        # We use has_event_listener to double check in case we call create_engine
        # multipe times in the same process.
        add_event_listener(Pool, 'checkout', ping_connection)

    return engine

# 该函数使用配置文件初始化 MySQL 数据库会话类。
# 它从配置文件创建数据库引擎，处理潜在的配置错误，并返回绑定到引擎的会话类。
def init_db_session_class(config_file):
    """Configure Session class for mysql according to the config file."""
    try:
        engine = create_engine_from_conf(config_file)
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.error(e)
        raise RuntimeError("invalid config file %s", config_file)
    
    Session = sessionmaker(bind=engine)
    return Session

# This is used to fix the problem of "MySQL has gone away" that happens when
# mysql server is restarted or the pooled connections are closed by the mysql
# server beacause being idle for too long.
#
# See http://stackoverflow.com/a/17791117/1467959
def ping_connection(dbapi_connection, connection_record, connection_proxy): # pylint: disable=unused-argument
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SELECT 1")
        cursor.close()
    except:
        logger.info('fail to ping database server, disposing all cached connections')
        connection_proxy._pool.dispose() # pylint: disable=protected-access
    
        # Raise DisconnectionError so the pool would create a new connection
        raise DisconnectionError()
