import os
import ConfigParser
import logging

from urllib import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

logger = logging.getLogger(__name__)

## base class of model classes in events.models and stats.models
Base = declarative_base()

def create_engine_from_conf(config_file):
    config = ConfigParser.ConfigParser()
    config.read(config_file)

    backend = config.get('DATABASE', 'type')
    if backend == 'sqlite' or backend == 'sqlite3':
        path = config.get('DATABASE', 'path')
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(config_file), path)
        db_url = "sqlite:///%s" % path
        logger.info('[seafevents] database: sqlite3, path: %s', path)
    elif backend == 'mysql':
        if config.has_option('DATABASE', 'host'):
            host = config.get('DATABASE', 'host').lower()
        else:
            host = 'localhost'

        if config.has_option('DATABASE', 'port'):
            port = config.getint('DATABASE', 'port')
        else:
            port = 3306
        username = config.get('DATABASE', 'username')
        passwd = config.get('DATABASE', 'password')
        dbname = config.get('DATABASE', 'name')
        db_url = "mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8" % (username, quote_plus(passwd), host, port, dbname)
        logger.info('[seafevents] database: mysql, name: %s', dbname)
    else:
        raise RuntimeError("Unknown database backend: %s" % backend)

    # Add pool recycle, or mysql connection will be closed by mysqld if idle
    # for too long.
    kwargs = dict(pool_recycle=300, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)

    return engine

def init_db_session_class(config_file):
    """Configure Session class for mysql according to the config file."""
    try:
        engine = create_engine_from_conf(config_file)
    except ConfigParser.NoOptionError, ConfigParser.NoSectionError:
        raise RuntimeError("invalid config file %s", config_file)

    # Create tables if not exists.
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    return Session
