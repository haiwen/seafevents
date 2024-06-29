import logging
import configparser

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

def create_engine_from_conf(config_file):
    seaf_conf = configparser.ConfigParser()
    seaf_conf.read(config_file)
    backend = seaf_conf.get('database', 'type')
    if backend == 'mysql':
        db_server = 'localhost'
        db_port = 3306

        if seaf_conf.has_option('database', 'host'):
            db_server = seaf_conf.get('database', 'host')
        if seaf_conf.has_option('database', 'port'):
            db_port =seaf_conf.getint('database', 'port')
        db_username = seaf_conf.get('database', 'user')
        db_passwd = seaf_conf.get('database', 'password')
        db_name = seaf_conf.get('database', 'db_name')
        db_url = "mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8" % \
                 (db_username, quote_plus(db_passwd),
                 db_server, db_port, db_name)
    else:
        logger.critical("Unknown Database backend: %s" % backend)
        raise RuntimeError("Unknown Database backend: %s" % backend)

    kwargs = dict(pool_recycle=300, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)
    if not has_event_listener(Pool, 'checkout', ping_connection):
        # We use has_event_listener to double check in case we call create_engine
        # multipe times in the same process.
        add_event_listener(Pool, 'checkout', ping_connection)

    return engine

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
