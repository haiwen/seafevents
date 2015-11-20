import datetime
import json
import logging

from sqlalchemy import desc

from .models import Event, UserEvent, FileAudit, FileUpdate, PermAudit

logger = logging.getLogger('seafevents')

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

def save_file_update_event(session, timestamp, user, org_id, repo_id, \
                           commit_id, file_oper):
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    event = FileUpdate(timestamp, user, org_id, repo_id, commit_id, file_oper)
    session.add(event)
    session.commit()

def get_events(session, obj, username, org_id, repo_id, file_path, start, limit):
    if start < 0:
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        raise RuntimeError('limit must be positive')

    q = session.query(obj)

    if username is not None:
        if hasattr(obj, 'user'):
            q = q.filter(obj.user==username)
        else:
            q = q.filter(obj.from_user==username)

    if repo_id is not None:
        q = q.filter(obj.repo_id==repo_id)

    if file_path is not None and hasattr(obj, 'file_path'):
        q = q.filter(obj.file_path==file_path)

    if org_id > 0:
        q = q.filter(obj.org_id==org_id)
    elif org_id < 0:
        q = q.filter(obj.org_id<=0)

    q = q.order_by(desc(obj.eid)).slice(start, start + limit)

    events = q.all()

    return events

def get_file_update_events(session, user, org_id, repo_id, start, limit):
    return get_events(session, FileUpdate, user, org_id, repo_id, None, start, limit)

def get_file_audit_events(session, user, org_id, repo_id, start, limit):
    return get_events(session, FileAudit, user, org_id, repo_id, None, start, limit)

def get_file_audit_events_by_path(session, user, org_id, repo_id, file_path, start, limit):
    return get_events(session, FileAudit, user, org_id, repo_id, file_path, start, limit)

def save_file_audit_event(session, timestamp, etype, user, ip, device, \
                           org_id, repo_id, file_path):
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    file_audit = FileAudit(timestamp, etype, user, ip, device, org_id, \
                           repo_id, file_path)

    session.add(file_audit)
    session.commit()

def save_perm_audit_event(session, timestamp, etype, from_user, to, \
                          org_id, repo_id, file_path, perm):
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    perm_audit = PermAudit(timestamp, etype, from_user, to, org_id, \
                           repo_id, file_path, perm)

    session.add(perm_audit)
    session.commit()

def get_perm_audit_events(session, from_user, org_id, repo_id, start, limit):
    return get_events(session, PermAudit, from_user, org_id, repo_id, None, start, limit)

def get_event_log_by_time(session, log_type, tstart, tend):
    if not log_type in ('file_update', 'file_audit', 'perm_audit'):
        raise RuntimeError('Invalid log_type parameter')

    if not isinstance(tstart, (long, float)) or not isinstance(tend, (long, float)):
        raise RuntimeError('Invalid time range parameter')

    if log_type == 'file_update':
        obj = FileUpdate
    elif log_type == 'file_audit':
        obj = FileAudit
    elif log_type == 'perm_audit':
        obj = PermAudit

    q = session.query(obj)
    q = q.filter(obj.timestamp.between(datetime.datetime.utcfromtimestamp(tstart),
                                       datetime.datetime.utcfromtimestamp(tend)))
    return q.all()
