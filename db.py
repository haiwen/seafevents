import os
import configparser
import logging
import uuid
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.sqltypes import Integer, String
from sqlalchemy.event import contains as has_event_listener, listen as add_event_listener
from sqlalchemy.exc import DisconnectionError
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import Pool
from sqlalchemy.ext.automap import automap_base

logger = logging.getLogger(__name__)


# base class of model classes in events.models and stats.models
class Base(DeclarativeBase):
    pass


SeafBase = automap_base()


def create_engine_from_conf(config, db='seafevent'):
    need_connection_pool_fix = True

    db_sec = 'DATABASE'
    user = 'username'
    db_name = 'name'

    if db == 'seafile':
        db_sec = 'database'
        user = 'user'
        db_name = 'db_name'

    db_url = ''
    host = 'db'
    port = '3306'
    username = 'seafile'
    passwd = ''
    dbname = 'seafile_db'

    if config.has_section(db_sec):
        backend = config.get(db_sec, 'type')
        if backend == 'mysql':
            if config.has_option(db_sec, 'host'):
                host = config.get(db_sec, 'host').lower()
            else:
                host = 'localhost'

            if config.has_option(db_sec, 'port'):
                port = config.getint(db_sec, 'port')
            else:
                port = 3306
            username = config.get(db_sec, user)
            dbname = config.get(db_sec, db_name)

            if config.has_option(db_sec, 'password'):
                passwd = config.get(db_sec, 'password')
                

            if config.has_option(db_sec, 'unix_socket'):
                unix_socket = config.get(db_sec, 'unix_socket')
                db_url = f"mysql+pymysql://{username}:@{host}:{port}/{dbname}?unix_socket={unix_socket}&charset=utf8"

        else:
            logger.error("Unknown database backend: %s" % backend)
            raise RuntimeError("Unknown database backend: %s" % backend)
    
    if not db_url: # connect mysql traditionally
        host = os.getenv('SEAFILE_MYSQL_DB_HOST') or host
        port = int(os.getenv('SEAFILE_MYSQL_DB_PORT', 0)) or port
        username = os.getenv('SEAFILE_MYSQL_DB_USER') or username
        passwd = os.getenv('SEAFILE_MYSQL_DB_PASSWORD') or passwd
        dbname = os.getenv('SEAFILE_MYSQL_DB_SEAFILE_DB_NAME' if db == 'seafile' else 'SEAFILE_MYSQL_DB_SEAHUB_DB_NAME') or dbname
        db_url = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % (username, quote_plus(passwd), host, port, dbname)

    # Add pool recycle, or mysql connection will be closed by mysqld if idle
    # for too long.
    kwargs = dict(pool_recycle=300, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)

    if need_connection_pool_fix and not has_event_listener(Pool, 'checkout', ping_connection):
        # We use has_event_listener to double check in case we call create_engine
        # multipe times in the same process.
        add_event_listener(Pool, 'checkout', ping_connection)

    return engine


def init_db_session_class(config, db='seafevent'):
    """Configure Session class for mysql according to the config file."""
    try:
        engine = create_engine_from_conf(config, db)
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.error(e)
        raise RuntimeError("create db engine error: %s" % e)

    Session = sessionmaker(bind=engine)
    return Session


def create_db_tables(config):
    # create seafevents tables if not exists.
    try:
        engine = create_engine_from_conf(config)
    except Exception as e:
        logger.error(e)
        raise RuntimeError("create db engine error: %s" % e)

    try:
        Base.metadata.create_all(engine)
    except Exception as e:
        logger.error("Failed to create database tables: %s" % e)
        raise RuntimeError("Failed to create database tables")
    engine.dispose()


def prepare_db_tables(seafile_config):
    # reflect the seafile_db tables
    try:
        engine = create_engine_from_conf(seafile_config, db='seafile')
    except Exception as e:
        logger.error(e)
        raise RuntimeError("create db engine error: %s" % e)

    SeafBase.prepare(autoload_with=engine)
    engine.dispose()


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


class GroupIdLDAPUuidPair(Base):
    """
    for ldap group sync
    """
    __tablename__ = 'GroupIdLDAPUuidPair'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_id = mapped_column(Integer, unique=True, nullable=False)
    group_uuid = mapped_column(String(36), default=uuid.uuid4, unique=True, nullable=False)

    def __init__(self, record):
        super().__init__()
        self.group_id = record['group_id']
        self.group_uuid = record['group_uuid']

    def __str__(self):
        return 'GroupIdLDAPUuidPair<id: %s, group_id: %s, group_uuid: %s>' % \
               (self.id, self.group_id, self.group_uuid)
