from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import desc

import ConfigParser
import simplejson as json
import datetime

from models import Base, Event, UserEvent, OrgUserEvent

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

    # Add pool recycle, or mysql connection will be close by mysqld if idle
    # for too long.
    engine = create_engine(db_url, pool_recycle=3600)
    
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

class UserEventDetail(object):
    """Regular objects which can be used by seahub without worrying about ORM"""
    def __init__(self, user_name, event):
        self.username = user_name

        self.etype = event.etype
        self.timestamp = event.timestamp

        dt = json.loads(event.detail)
        for key in dt:
            self.__dict__[key] = dt[key]

class OrgUserEventDetail(object):
    """Org version of UserEventDetail"""
    def __init__(self, org_id, user_name, event):
        self.org_id = org_id
        self.username = user_name

        self.etype = event.etype
        self.timestamp = event.timestamp

        dt = json.loads(event.detail)
        for key in dt:
            self.__dict__[key] = dt[key]

def get_user_events(session, username, start, limit):
    """Return events related to username with given start and limit"""
    events = session.query(Event).filter(UserEvent.username==username).filter(UserEvent.eid==Event.uuid).order_by(desc(Event.timestamp))[start:limit]

    return [ UserEventDetail(username, ev) for ev in events ]

def get_org_user_events(session, org_id, username, start, limit):
    """Org version of get_user_events"""
    events = session.query(Event).filter(OrgUserEvent.org_id==org_id).filter(OrgUserEvent.username==username).filter(OrgUserEvent.eid==Event.uuid).order_by(desc(Event.timestamp))[start:limit]

    return [ OrgUserEventDetail(org_id, username, ev) for ev in events ]

def save_user_events(session, etype, detail, usernames, timestamp):
    """Save a user event. Detail is a dict which contains all event-speicific
    information. A UserEvent will be created for every user in 'usernames'.

    """
    if timestamp is None:
        timestamp = datetime.datetime.now()

    event = Event(timestamp, etype, detail)
    session.add(event)
    session.commit()

    for username in usernames:
        user_event = UserEvent(username, event.uuid)
        session.add(user_event)

    session.commit()

def save_org_user_events(session, org_id, etype, detail, usernames, timestamp):
    """Org version of save_user_events"""
    if timestamp is None:
        timestamp = datetime.datetime.now()

    event = Event(timestamp, etype, detail)
    session.add(event)
    session.commit()

    for username in usernames:
        org_user_event = OrgUserEvent(org_id, username, event.uuid)
        session.add(org_user_event)

    session.commit()
