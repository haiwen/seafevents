import os
import ConfigParser
import simplejson as json
import datetime
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import desc
from sqlalchemy.pool import QueuePool

from models import Base, Event, UserEvent
from gevent.coros import Semaphore

logger = logging.getLogger('seafevents')

class GreenQueuePool(QueuePool):

    def __init__(self, *args, **kwargs):
        super(GreenQueuePool, self).__init__(*args, **kwargs)
        if self._overflow_lock is not None:
            self._overflow_lock = Semaphore()

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
        username = config.get('DATABASE', 'username')
        passwd = config.get('DATABASE', 'password')
        dbname = config.get('DATABASE', 'name')
        db_url = "mysql+mysqlconnector://%s:%s@%s/%s" % (username, passwd, host, dbname)
        logger.info('[seafevents] database: mysql, name: %s', dbname)
    else:
        raise RuntimeError("Unknown database backend: %s" % backend)

    # Add pool recycle, or mysql connection will be closed by mysqld if idle
    # for too long.
    kwargs = dict(pool_recycle=3600, echo=False, echo_pool=False)
    if backend == 'mysql':
        kwargs['poolclass'] = GreenQueuePool
        kwargs['max_overflow'] = -1

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

class UserEventDetail(object):
    """Regular objects which can be used by seahub without worrying about ORM"""
    def __init__(self, org_id, user_name, event):
        self.org_id = org_id
        self.username = user_name

        self.etype = event.etype
        self.timestamp = event.timestamp
        self.uuid = event.uuid

        dt = json.loads(event.detail)
        for key in dt:
            self.__dict__[key] = dt[key]

# org_id > 0 --> get org events
# org_id < 0 --> get non-org events
# org_id = 0 --> get all events
def _get_user_events(session, org_id, username, start, limit):
    if start < 0:
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        raise RuntimeError('limit must be positive')

    q = session.query(Event).filter(UserEvent.username==username).filter(UserEvent.eid==Event.uuid)
    if org_id > 0:
        q = q.filter(UserEvent.org_id==org_id)
    elif org_id < 0:
        q = q.filter(UserEvent.org_id<=0)

    events = q.order_by(desc(Event.timestamp))[start:start + limit]
    return [ UserEventDetail(org_id, username, ev) for ev in events ]

def get_user_events(session, username, start, limit):
    return _get_user_events(session, -1, username, start, limit)

def get_org_user_events(session, org_id, username, start, limit):
    """Org version of get_user_events"""
    return _get_user_events(session, org_id , username, start, limit)

def get_user_all_events(session, username, start, limit):
    """Get all events of a user"""
    return _get_user_events(session, 0, username, start, limit)

def delete_event(session, uuid):
    '''Delete the event with the given UUID
    TODO: delete a list of uuid to reduce sql queries
    '''
    session.query(Event).filter(Event.uuid==uuid).delete()
    session.commit()

def _save_user_events(session, org_id, etype, detail, usernames, timestamp):
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    if org_id > 0 and not detail.has_key('org_id'):
        detail['org_id'] = org_id

    event = Event(timestamp, etype, detail)
    session.add(event)
    session.commit()

    for username in usernames:
        user_event = UserEvent(org_id, username, event.uuid)
        session.add(user_event)

    session.commit()

def save_user_events(session, etype, detail, usernames, timestamp):
    """Save a user event. Detail is a dict which contains all event-speicific
    information. A UserEvent will be created for every user in 'usernames'.

    """
    return _save_user_events(session, -1, etype, detail, usernames, timestamp)

def save_org_user_events(session, org_id, etype, detail, usernames, timestamp):
    """Org version of save_user_events"""
    return _save_user_events(session, org_id, etype, detail, usernames, timestamp)
