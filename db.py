import os

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import ConfigParser

__all__ = [
    "init_db_session",
    "RepoUpdateEvent",
]

Base = declarative_base()

class RepoUpdateEvent(Base):
    __tablename__ = 'RepoUpdateEvent'

    id = Column(Integer, primary_key=True)
    repo_id = Column(String(length=36), nullable=False)
    commit_id = Column(String(length=40), nullable=False)
    timestamp = Column(DateTime, nullable=False)

    def __init__(self, repo_id, commit_id, timestamp):
        self.repo_id = repo_id
        self.commit_id = commit_id
        self.timestamp = timestamp

def create_engine_from_conf(config_file):
    config = ConfigParser.ConfigParser()
    config.read(config_file)

    db_type = config.get('DATABASE', 'type').lower()

    if db_type == 'mysql':
        if config.has_option('DATABASE', 'host'):
            host = config.get('DATABASE', 'host').lower()
        else:
            host = 'localhost'
        username = config.get('DATABASE', 'username')
        passwd = config.get('DATABASE', 'password')
        dbname = config.get('DATABASE', 'name')
        db_url = "mysql://%s:%s@%s/%s" % (username, passwd, host, dbname)
    else:
        dbname = config.get('DATABASE', 'name')
        db_url = "sqlite3:///%s"

    engine = create_engine(db_url)
    
    return engine
    
def init_db_session(args):
    try:
        engine = create_engine_from_conf(args.config_file)
    except ConfigParser.NoOptionError, ConfigParser.NoSectionError:
        raise RuntimeError("invalid config file %s", args.config_file)
        
    Base.metadata.create_all(engine) 

    Session = sessionmaker(bind=engine)
    session = Session()
    return session
