import os
import ConfigParser
import simplejson as json
import datetime
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import desc

from .models import Base, Event, UserEvent, UserTrafficStat

logger = logging.getLogger('seafevents')

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
        db_url = "mysql+mysqldb://%s:%s@%s:%s/%s" % (username, passwd, host, port, dbname)
        logger.info('[seafevents] database: mysql, name: %s', dbname)
    else:
        raise RuntimeError("Unknown database backend: %s" % backend)

    # Add pool recycle, or mysql connection will be closed by mysqld if idle
    # for too long.
    kwargs = dict(pool_recycle=3600, echo=False, echo_pool=False)

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
        self.fix_trailing_zero_bug(dt)
        for key in dt:
            self.__dict__[key] = dt[key]

    def fix_trailing_zero_bug(self, dt):
        '''Fix the errornous trailing zero byte in ccnet 9d99718d77e93fce77561c5437c67dc21724dd9a'''
        if 'commit_id' in dt:
            commit_id = dt['commit_id']
            if commit_id[-1] == u'\x00':
                dt['commit_id'] = commit_id[:-1]

# org_id > 0 --> get org events
# org_id < 0 --> get non-org events
# org_id = 0 --> get all events
def _get_user_events(session, org_id, username, start, limit):
    if start < 0:
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        raise RuntimeError('limit must be positive')

    q = session.query(Event).filter(UserEvent.username==username)
    if org_id > 0:
        q = q.filter(UserEvent.org_id==org_id)
    elif org_id < 0:
        q = q.filter(UserEvent.org_id<=0)

    q = q.filter(UserEvent.eid==Event.uuid).order_by(desc(UserEvent.id)).slice(start, start + limit)

    # select Event.etype, Event.timestamp, UserEvent.username from UserEvent, Event where UserEvent.username=username and UserEvent.org_id <= 0 and UserEvent.eid = Event.uuid order by UserEvent.id desc limit 0, 15;

    events = q.all()
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

def update_block_download_traffic(session, email, size):
    update_traffic_common(session, email, size, UserTrafficStat.block_download, 'block_download')

def update_file_view_traffic(session, email, size):
    update_traffic_common(session, email, size, UserTrafficStat.file_view, 'file_view')

def update_file_download_traffic(session, email, size):
    update_traffic_common(session, email, size, UserTrafficStat.file_download, 'file_download')

def update_dir_download_traffic(session, email, size):
    update_traffic_common(session, email, size, UserTrafficStat.dir_download, 'dir_download')

def update_traffic_common(session, email, size, type, name):
    '''common code to update different types of traffic stat'''
    if not isinstance(size, (int, long)) or size <= 0:
        logging.warning('invalid %s update: size = %s', type, size)
        return

    month = datetime.datetime.now().strftime('%Y%m')

    q = session.query(UserTrafficStat).filter_by(email=email, month=month)
    n = q.update({ type: type + size })
    if n != 1:
        stat = UserTrafficStat(email, month, **{name:size})
        session.add(stat)

    session.commit()

def get_user_traffic_stat(session, email, month=None):
    '''Return the total traffic of a user in the given month. If month is not
    supplied, defaults to the current month

    '''
    if month == None:
        month = datetime.datetime.now().strftime('%Y%m')

    rows = session.query(UserTrafficStat).filter_by(email=email, month=month).all()
    if not rows:
        return None
    else:
        stat = rows[0]
        return stat.as_dict()
