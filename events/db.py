import json
import uuid
import logging
import datetime
from datetime import timedelta
import hashlib

from sqlalchemy import desc
from sqlalchemy.sql import exists

from .models import Event, UserEvent, FileAudit, FileUpdate, PermAudit, \
        Activity, UserActivity, FileHistory

from seafevents.app.config import appconfig

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
        for key in dt:
            self.__dict__[key] = dt[key]

class UserActivityDetail(object):
    """Regular objects which can be used by seahub without worrying about ORM"""
    def __init__(self, event, username=None):
        self.username = username

        self.id = event.id
        self.op_type = event.op_type
        self.op_user = event.op_user
        self.obj_type = event.obj_type
        self.repo_id = event.repo_id
        self.commit_id = event.commit_id
        self.timestamp = event.timestamp
        self.path = event.path

        dt = json.loads(event.detail)
        for key in dt:
            self.__dict__[key] = dt[key]

    def __getitem__(self, key):
        return self.__dict__[key]

# org_id > 0 --> get org events
# org_id < 0 --> get non-org events
# org_id = 0 --> get all events
def _get_user_events(session, org_id, username, start, limit):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        logger.error('limit must be positive')
        raise RuntimeError('limit must be positive')

    q = session.query(Event).filter(UserEvent.username == username)
    if org_id > 0:
        q = q.filter(UserEvent.org_id == org_id)
    elif org_id < 0:
        q = q.filter(UserEvent.org_id <= 0)

    q = q.filter(UserEvent.eid == Event.uuid).order_by(desc(UserEvent.id)).slice(start, start + limit)

    # select Event.etype, Event.timestamp, UserEvent.username from UserEvent, Event where UserEvent.username=username and UserEvent.org_id <= 0 and UserEvent.eid = Event.uuid order by UserEvent.id desc limit 0, 15;

    events = q.all()
    return [ UserEventDetail(org_id, username, ev) for ev in events ]

def get_user_events(session, username, start, limit):
    return _get_user_events(session, -1, username, start, limit)

def get_org_user_events(session, org_id, username, start, limit):
    """Org version of get_user_events"""
    return _get_user_events(session, org_id, username, start, limit)

def get_user_all_events(session, username, start, limit):
    """Get all events of a user"""
    return _get_user_events(session, 0, username, start, limit)

def delete_event(session, uuid):
    '''Delete the event with the given UUID
    TODO: delete a list of uuid to reduce sql queries
    '''
    session.query(Event).filter(Event.uuid == uuid).delete()
    session.commit()

def _get_user_activities(session, username, start, limit):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        logger.error('limit must be positive')
        raise RuntimeError('limit must be positive')

    q = session.query(Activity).filter(UserActivity.username == username)
    q = q.filter(UserActivity.activity_id == Activity.id)

    events = q.order_by(desc(UserActivity.timestamp)).slice(start, start + limit).all()

    return [ UserActivityDetail(ev, username=username) for ev in events ]

def get_user_activities(session, username, start, limit):
    return _get_user_activities(session, username, start, limit)

def _get_user_activities_by_timestamp(username, start, end):
    events = []
    try:
        session = appconfig.session_cls()
        q = session.query(Activity).filter(UserActivity.username == username,
                                           UserActivity.timestamp.between(start, end))
        q = q.filter(UserActivity.activity_id == Activity.id)

        events = q.order_by(UserActivity.timestamp).all()
    except Exception as e:
        logging.warning('Failed to get activities of %s: %s.', username, e)
    finally:
        session.close()

    return [ UserActivityDetail(ev, username=username) for ev in events ]

def get_user_activities_by_timestamp(username, start, end):
    return _get_user_activities_by_timestamp(username, start, end)

def get_file_history(session, repo_id, path, start, limit, history_limit=-1):
    repo_id_path_md5 = hashlib.md5((repo_id + path).encode('utf8')).hexdigest()
    current_item = session.query(FileHistory).filter(FileHistory.repo_id_path_md5 == repo_id_path_md5)\
        .order_by(desc(FileHistory.id)).first()

    events = []
    total_count = 0
    if current_item:
        total_count = session.query(FileHistory).filter(FileHistory.file_uuid == current_item.file_uuid).count()
        q = session.query(FileHistory).filter(FileHistory.file_uuid == current_item.file_uuid)\
            .order_by(desc(FileHistory.id)).slice(start, start + limit + 1)

        if int(history_limit) >= 0:
            present_time = datetime.datetime.utcnow()
            delta = timedelta(days=history_limit)
            history_time = present_time - delta

            total_count = session.query(FileHistory).filter(FileHistory.file_uuid == current_item.file_uuid).\
                filter(FileHistory.timestamp.between(history_time, present_time)).count()
            q = session.query(FileHistory).filter(FileHistory.file_uuid == current_item.file_uuid).\
                filter(FileHistory.timestamp.between(history_time, present_time)).\
                order_by(desc(FileHistory.id)).slice(start, start + limit + 1)

        events = q.all()
        if events and len(events) == limit + 1:
            next_start = start + limit
            events = events[:-1]

    return events, total_count

def not_include_all_keys(record, keys):
    return any(record.get(k, None) is None for k in keys)

def save_user_activity(session, record):
    activity = Activity(record)
    session.add(activity)
    session.commit()
    for username in record['related_users']:
        user_activity = UserActivity(username, activity.id, record['timestamp'])
        session.add(user_activity)
    session.commit()

def update_user_activity_timestamp(session, activity_id, record):
    q = session.query(Activity).filter(Activity.id==activity_id)
    q = q.update({"timestamp": record["timestamp"]})
    q = session.query(UserActivity).filter(UserActivity.activity_id==activity_id)
    q = q.update({"timestamp": record["timestamp"]})
    session.commit()

def update_file_history_record(session, history_id, record):
    q = session.query(FileHistory).filter(FileHistory.id==history_id)
    q = q.update({"timestamp": record["timestamp"],
                  "file_id": record["obj_id"],
                  "commit_id": record["commit_id"],
                  "size": record["size"]})
    session.commit()

def query_prev_record(session, record):
    if record['op_type'] == 'create':
        return None

    if record['op_type'] in ['rename', 'move']:
        repo_id_path_md5 = hashlib.md5((record['repo_id'] + record['old_path']).encode('utf8')).hexdigest()
    else:
        repo_id_path_md5 = hashlib.md5((record['repo_id'] + record['path']).encode('utf8')).hexdigest()

    q = session.query(FileHistory)
    prev_item = q.filter(FileHistory.repo_id_path_md5 == repo_id_path_md5).order_by(desc(FileHistory.timestamp)).first()

    # The restore operation may not be the last record to be restored, so you need to switch to the last record
    if record['op_type'] == 'recover':
        q = session.query(FileHistory)
        prev_item = q.filter(FileHistory.file_uuid == prev_item.file_uuid).order_by(desc(FileHistory.timestamp)).first()

    return prev_item

def save_filehistory(session, record):
    # use same file_uuid if prev item already exists, otherwise new one
    prev_item = query_prev_record(session, record)
    if prev_item:
        # If a file was edited many times in a few minutes, just update timestamp.
        dt = datetime.datetime.utcnow()
        delta = timedelta(minutes=appconfig.fh.threshold)
        if record['op_type'] == 'edit' and prev_item.op_type == 'edit' \
                                       and prev_item.op_user == record['op_user'] \
                                       and prev_item.timestamp > dt - delta:
            update_file_history_record(session, prev_item.id, record)
            return

        if record['path'] != prev_item.path and record['op_type'] == 'recover':
            pass
        else:
            record['file_uuid'] = prev_item.file_uuid

    if 'file_uuid' not in record:
        file_uuid = uuid.uuid4().__str__()
        # avoid hash conflict
        while session.query(exists().where(FileHistory.file_uuid == file_uuid)).scalar():
            file_uuid = uuid.uuid4().__str__()
        record['file_uuid'] = file_uuid

    filehistory = FileHistory(record)
    session.add(filehistory)
    session.commit()

def _save_user_events(session, org_id, etype, detail, usernames, timestamp):
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    if org_id > 0 and 'org_id' not in detail:
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

def save_file_update_event(session, timestamp, user, org_id, repo_id,
                           commit_id, file_oper):
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    event = FileUpdate(timestamp, user, org_id, repo_id, commit_id, file_oper)
    session.add(event)
    session.commit()

def get_events(session, obj, username, org_id, repo_id, file_path, start, limit):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        logger.error('limit must be positive')
        raise RuntimeError('limit must be positive')

    q = session.query(obj)

    if username is not None:
        if hasattr(obj, 'user'):
            q = q.filter(obj.user == username)
        else:
            q = q.filter(obj.from_user == username)

    if repo_id is not None:
        q = q.filter(obj.repo_id == repo_id)

    if file_path is not None and hasattr(obj, 'file_path'):
        q = q.filter(obj.file_path == file_path)

    if org_id > 0:
        q = q.filter(obj.org_id == org_id)
    elif org_id < 0:
        q = q.filter(obj.org_id <= 0)

    q = q.order_by(desc(obj.eid)).slice(start, start + limit)

    events = q.all()

    return events

def get_file_update_events(session, user, org_id, repo_id, start, limit):
    return get_events(session, FileUpdate, user, org_id, repo_id, None, start, limit)

def get_file_audit_events(session, user, org_id, repo_id, start, limit):
    return get_events(session, FileAudit, user, org_id, repo_id, None, start, limit)

def get_file_audit_events_by_path(session, user, org_id, repo_id, file_path, start, limit):
    return get_events(session, FileAudit, user, org_id, repo_id, file_path, start, limit)

def save_file_audit_event(session, timestamp, etype, user, ip, device,
                           org_id, repo_id, file_path):
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    file_audit = FileAudit(timestamp, etype, user, ip, device, org_id,
                           repo_id, file_path)

    session.add(file_audit)
    session.commit()

def save_perm_audit_event(session, timestamp, etype, from_user, to,
                          org_id, repo_id, file_path, perm):
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    perm_audit = PermAudit(timestamp, etype, from_user, to, org_id,
                           repo_id, file_path, perm)

    session.add(perm_audit)
    session.commit()

def get_perm_audit_events(session, from_user, org_id, repo_id, start, limit):
    return get_events(session, PermAudit, from_user, org_id, repo_id, None, start, limit)

def get_event_log_by_time(session, log_type, tstart, tend):
    if log_type not in ('file_update', 'file_audit', 'perm_audit'):
        logger.error('Invalid log_type parameter')
        raise RuntimeError('Invalid log_type parameter')

    if not isinstance(tstart, (int, float)) or not isinstance(tend, (int, float)):
        logger.error('Invalid time range parameter')
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

# If a file was moved or renamed, find the new file by old path.
def get_new_file_path(repo_id, old_path):
    ret = None
    repo_id_path_md5 = hashlib.md5((repo_id + old_path).encode('utf8')).hexdigest()
    try:
        session = appconfig.session_cls()
        q = session.query(FileHistory.file_uuid).filter(FileHistory.repo_id_path_md5==repo_id_path_md5)
        q = q.order_by(desc(FileHistory.timestamp))
        file_uuid = q.first()[0]
        if not file_uuid:
            session.close()
            return None

        q = session.query(FileHistory.path).filter(FileHistory.file_uuid==file_uuid)
        q = q.order_by(desc(FileHistory.timestamp))
        ret = q.first()[0]
    except Exception as e:
        logging.warning('Failed to get new file path for %.8s:%s: %s.', repo_id, old_path, e)
    finally:
        session.close()

    return ret
