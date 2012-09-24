from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import desc

import ConfigParser
import simplejson as json

from models import Base, UserEvent, Event

__all__ = [
    "init_db_session",
    "get_user_events",
]

def create_engine_from_conf(config_file):
    config = ConfigParser.ConfigParser()
    config.read(config_file)

    if config.has_option('DATABASE', 'host'):
        host = config.get('DATABASE', 'host').lower()
    else:
        host = 'localhost'

    username = config.get('DATABASE', 'username')
    passwd = config.get('DATABASE', 'password')
    dbname = config.get('DATABASE', 'name')
    db_url = "mysql://%s:%s@%s/%s" % (username, passwd, host, dbname)

    engine = create_engine(db_url)
    
    return engine
    
def init_db_session(config_file):
    """Configure Session class for mysql according to the config file."""
    try:
        engine = create_engine_from_conf(config_file)
    except ConfigParser.NoOptionError, ConfigParser.NoSectionError:
        raise RuntimeError("invalid config file %s", config_file)
        
    # Create tables if not exists.
    Base.metadata.create_all(engine) 

    Session = sessionmaker(bind=engine)
    return Session

class UserEventDetail(object):
    """Regular objects which can be used by seaub without worrying about ORM"""
    def __init__(self, user_name, event):
        self.username = user_name

        self.etype = event.etype
        self.timestamp = event.timestamp

        dt = json.loads(event.detail)
        for key in dt:
            self.__dict__[key] = dt[key]

def get_user_events(session, username, start, limit):
    """Return events related to username with given limit"""
    events = session.query(Event).filter(UserEvent.username==username).filter(UserEvent.eid==Event.uuid).order_by(desc(Event.timestamp))[start:limit]

    return [ UserEventDetail(username, ev) for ev in events ]
