import os

from sqlalchemy import Column, BigInteger, String, DateTime, Text
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sqlalchemy import ForeignKey
from sqlalchemy import desc

import uuid
import ConfigParser

__all__ = [
    "init_db_session",
    "UserEvent",
]

Base = declarative_base()

class Event(Base):
    """General class for events. Specific information is stored in json format
    in Event.detail.

    """
    __tablename__ = 'Event'
    
    uuid = Column(String(length=36), primary_key=True)
    etype = Column(String(length=128), nullable=False)
    timestamp = Column(DateTime, nullable=False)

    # Json format detail for this event
    detail = Column(Text, nullable=False)

    def __init__(self, timestamp, etype, detail):
        self.uuid = str(uuid.uuid4())
        self.timestamp = timestamp
        self.etype = etype
        self.detail = detail

    def __str__(self):
        return 'Event<uuid: %s, type: %s, detail: %s>' % \
            (self.uuid, self.etype, self.detail)
    
class UserEvent(Base):
    __tablename__ = 'UserEvent'

    id = Column(BigInteger, primary_key=True)

    username = Column(String(length=256), nullable=False)

    eid = Column(String(length=36), ForeignKey('Event.uuid'))

    def __init__(self, username, eid):
        self.username = username
        self.eid = eid

    def __str__(self):
        return "UserEvent<user = %s, event id = %s>" % \
            (self.username, self.eid)

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
        if not os.path.isabs(dbname):
            dbname = os.path.join(os.path.dirname(config_file), dbname)
            
        db_url = "sqlite3:///%s" % dbname

    engine = create_engine(db_url)
    
    return engine
    
def init_db_session(config_file):
    """Create a sqlite3/mysql database session according to the config file.""" 
    try:
        engine = create_engine_from_conf(config_file)
    except ConfigParser.NoOptionError, ConfigParser.NoSectionError:
        raise RuntimeError("invalid config file %s", config_file)
        
    # Create tables if not exists.
    Base.metadata.create_all(engine) 

    Session = sessionmaker(bind=engine)
    return Session

def get_user_events(session, username, limit):
    """Return events related to username with given limit"""
    events = session.query(Event).filter(UserEvent.username==username).filter(UserEvent.eid==Event.uuid).order_by(desc(Event.timestamp))[:limit]
    return events
