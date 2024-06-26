import json
import uuid
import logging
import datetime
from datetime import timedelta
import hashlib

from sqlalchemy import desc, select, update, func, text, and_
from sqlalchemy.sql import exists

from .models import FileAudit, FileUpdate, PermAudit, \
        Activity, UserActivity, FileHistory, UserLogin

from .utils import write_xls, utc_to_local, generate_file_audit_event_type
from .task_manager import task_manager
from seaserv import seafile_api, ccnet_api
from django.http import HttpResponse, JsonResponse
from django.utils.translation import gettext as _
import os

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


def _get_user_activities(session, username, start, limit):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        logger.error('limit must be positive')
        raise RuntimeError('limit must be positive')

    sub_query = (
        select(UserActivity.activity_id)
        .where(UserActivity.username == username)

    )
    stmt = (
        select(Activity)
        .where(Activity.id.in_(sub_query))
        .order_by(desc(Activity.timestamp))
        .slice(start, start + limit)

    )
    events = session.scalars(stmt).all()

    return [ UserActivityDetail(ev, username=username) for ev in events ]

def get_user_activities(session, username, start, limit):
    return _get_user_activities(session, username, start, limit)

def _get_user_activities_by_timestamp(session, username, start, end):
    events = []
    try:
        sub_query = (
            select(UserActivity.activity_id)
                .where(
                    and_(
                        UserActivity.username == username,
                        UserActivity.timestamp.between(start, end)
                    )
                )
        )
        stmt = (
            select(Activity)
                .where(Activity.id.in_(sub_query))
                .order_by(Activity.timestamp)
        )
        events = session.scalars(stmt).all()
    except Exception as e:
        logging.warning('Failed to get activities of %s: %s.', username, e)
    finally:
        session.close()

    return [ UserActivityDetail(ev, username=username) for ev in events ]

def get_user_activities_by_timestamp(session, username, start, end):
    return _get_user_activities_by_timestamp(session, username, start, end)

def get_file_history(session, repo_id, path, start, limit, history_limit=-1):
    repo_id_path_md5 = hashlib.md5((repo_id + path).encode('utf8')).hexdigest()
    current_item = session.scalars(select(FileHistory).where(FileHistory.repo_id_path_md5 == repo_id_path_md5).
                                   order_by(desc(FileHistory.id)).limit(1)).first()

    events = []
    total_count = 0
    if current_item:
        count_stmt = select(func.count(FileHistory.id)).where(FileHistory.file_uuid == current_item.file_uuid)
        query_stmt = select(FileHistory).where(FileHistory.file_uuid == current_item.file_uuid)\
            .order_by(desc(FileHistory.id)).slice(start, start + limit + 1)

        if int(history_limit) >= 0:
            present_time = datetime.datetime.utcnow()
            delta = timedelta(days=history_limit)
            history_time = present_time - delta

            count_stmt = select(func.count(FileHistory.id)).\
                where(FileHistory.file_uuid == current_item.file_uuid,
                      FileHistory.timestamp.between(history_time, present_time))
            query_stmt = select(FileHistory).\
                where(FileHistory.file_uuid == current_item.file_uuid,
                      FileHistory.timestamp.between(history_time, present_time))\
                .order_by(desc(FileHistory.id)).slice(start, start + limit + 1)

        total_count = session.scalar(count_stmt)
        events = session.scalars(query_stmt).all()
        if events and len(events) == limit + 1:
            events = events[:-1]

    return events, total_count

def convert_file_history_to_dict(file_history):
    new_file_history = {}
    new_file_history['id'] = file_history.id
    new_file_history['op_type'] = file_history.op_type
    new_file_history['op_user'] = file_history.op_user
    new_file_history['timestamp'] = file_history.timestamp
    new_file_history['repo_id'] = file_history.repo_id
    new_file_history['commit_id'] = file_history.commit_id
    new_file_history['file_id'] = file_history.file_id
    new_file_history['file_uuid'] = file_history.file_uuid
    new_file_history['path'] = file_history.path
    new_file_history['repo_id_path_md5'] = file_history.repo_id_path_md5
    new_file_history['size'] = file_history.size
    new_file_history['old_path'] = file_history.old_path

    try:
        new_file_history['count'] = file_history.count
        new_file_history['date'] = file_history.date
    except Exception as e:
        pass
    return new_file_history

def get_file_history_by_day(session, repo_id, path, start, limit, to_tz, history_limit=-1):
    repo_id_path_md5 = hashlib.md5((repo_id + path).encode('utf8')).hexdigest()
    current_item = session.scalars(select(FileHistory).where(FileHistory.repo_id_path_md5 == repo_id_path_md5).
                                   order_by(desc(FileHistory.id)).limit(1)).first()

    new_events = []
    if current_item:
        query_stmt = select(
                FileHistory.id, FileHistory.op_type, FileHistory.op_user, FileHistory.timestamp, FileHistory.repo_id, FileHistory.commit_id,
                FileHistory.file_id, FileHistory.file_uuid, FileHistory.path, FileHistory.repo_id_path_md5, FileHistory.size, FileHistory.old_path,
                func.date_format(func.convert_tz(FileHistory.timestamp, '+00:00', to_tz), '%Y-%m-%d 00:00:00').label('date'),
                func.count(FileHistory.id).label('count'),
                func.max(FileHistory.id).label('max_id')
                )

        if int(history_limit) >= 0:
            present_time = datetime.datetime.utcnow()
            delta = timedelta(days=history_limit)
            history_time = present_time - delta
            query_stmt = query_stmt.where(FileHistory.file_uuid == current_item.file_uuid, FileHistory.timestamp.between(history_time, present_time))
        else:
            query_stmt = query_stmt.where(FileHistory.file_uuid == current_item.file_uuid)

        query_stmt = query_stmt.order_by(desc(FileHistory.id)).\
                                group_by('date').\
                                slice(start, start + limit + 1)

        events = session.execute(query_stmt).all()
        if events and len(events) == limit + 1:
            events = events[:-1]

        for event in events:
            if event.max_id == event.id:
                new_event = convert_file_history_to_dict(event)
                new_events.append(new_event)
            else:
                max_record_sql = select(FileHistory).where(FileHistory.file_uuid == current_item.file_uuid, FileHistory.id == event.max_id).limit(1)
                max_id_event = session.scalars(max_record_sql).first()
                new_event = convert_file_history_to_dict(max_id_event)
                new_event['count'] = event.count
                new_event['date'] = event.date
                new_events.append(new_event)

    return new_events

def get_file_daily_history_detail(session, repo_id, path, start_time, end_time, to_tz):
    repo_id_path_md5 = hashlib.md5((repo_id + path).encode('utf8')).hexdigest()
    current_item = session.scalars(select(FileHistory).where(FileHistory.repo_id_path_md5 == repo_id_path_md5).
                                   order_by(desc(FileHistory.id)).limit(1)).first()

    details = list()
    try:
        q = select(FileHistory.id, FileHistory.op_type, FileHistory.op_user, FileHistory.timestamp, FileHistory.repo_id, FileHistory.commit_id,
                FileHistory.file_id, FileHistory.file_uuid, FileHistory.path, FileHistory.repo_id_path_md5, FileHistory.size, FileHistory.old_path,
                func.date_format(func.convert_tz(FileHistory.timestamp, '+00:00', to_tz), '%Y-%m-%d 00:00:00').label('date')).\
            where(FileHistory.file_uuid == current_item.file_uuid, func.convert_tz(FileHistory.timestamp, '+00:00', to_tz).between(start_time, end_time)).\
            order_by(desc(FileHistory.id))
        details = session.execute(q).all()
    except Exception as e:
        logger.error('Get table activities detail failed: %s' % e)

    return details

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
    activity_stmt = update(Activity).where(Activity.id == activity_id).\
        values(timestamp=record["timestamp"])
    session.execute(activity_stmt)
    user_activity_stmt = update(UserActivity).where(UserActivity.activity_id == activity_id).\
        values(timestamp=record["timestamp"])
    session.execute(user_activity_stmt)
    session.commit()

def update_file_history_record(session, history_id, record):
    stmt = update(FileHistory).where(FileHistory.id == history_id).\
        values(timestamp=record["timestamp"], file_id=record["obj_id"],
               commit_id=record["commit_id"], size=record["size"])
    session.execute(stmt)
    session.commit()

def query_prev_record(session, record):
    if record['op_type'] == 'create':
        return None

    if record['op_type'] in ['rename', 'move']:
        repo_id_path_md5 = hashlib.md5((record['repo_id'] + record['old_path']).encode('utf8')).hexdigest()
    else:
        repo_id_path_md5 = hashlib.md5((record['repo_id'] + record['path']).encode('utf8')).hexdigest()

    stmt = select(FileHistory).where(FileHistory.repo_id_path_md5 == repo_id_path_md5).\
        order_by(desc(FileHistory.timestamp)).limit(1)
    prev_item = session.scalars(stmt).first()

    # The restore operation may not be the last record to be restored, so you need to switch to the last record
    if record['op_type'] == 'recover':
        stmt = select(FileHistory).where(FileHistory.file_uuid == prev_item.file_uuid).\
            order_by(desc(FileHistory.timestamp)).limit(1)
        prev_item = session.scalars(stmt).first()

    return prev_item

def save_filehistory(session, fh_threshold, record):
    # use same file_uuid if prev item already exists, otherwise new one
    prev_item = query_prev_record(session, record)
    if prev_item:
        # If a file was edited many times in a few minutes, just update timestamp.
        dt = datetime.datetime.utcnow()
        delta = timedelta(minutes=fh_threshold)
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
        while session.scalar(select(exists().where(FileHistory.file_uuid == file_uuid))):
            file_uuid = uuid.uuid4().__str__()
        record['file_uuid'] = file_uuid

    filehistory = FileHistory(record)
    session.add(filehistory)
    session.commit()


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

    stmt = select(obj)

    if username is not None:
        if hasattr(obj, 'user'):
            stmt = stmt.where(obj.user == username)
        else:
            stmt = stmt.where(obj.from_user == username)

    if repo_id is not None:
        stmt = stmt.where(obj.repo_id == repo_id)

    if file_path is not None and hasattr(obj, 'file_path'):
        stmt = stmt.where(obj.file_path == file_path)

    if org_id > 0:
        stmt = stmt.where(obj.org_id == org_id)
    elif org_id < 0:
        stmt = stmt.where(obj.org_id == -1)

    stmt = stmt.order_by(desc(obj.eid)).slice(start, start + limit)

    events = session.scalars(stmt).all()

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
    else:
        obj = PermAudit

    stmt = select(obj).where(obj.timestamp.between(datetime.datetime.utcfromtimestamp(tstart),
                                                   datetime.datetime.utcfromtimestamp(tend)))
    res = session.scalars(stmt).all()

    return res


def get_sys_logs_task(session, tstart, tend, log_type):
    if task_manager.tasks_queue.full():
        logger.warning('seafile io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return HttpResponse('seafile io server busy.', status=400)

    try:
        task_id = task_manager.add_export_logs_task(session, tstart, tend, log_type)
    except Exception as e:
        logger.error(e)
        return HttpResponse(e, status=500)

    return task_id


def query_status(task_id):
    if not task_manager.is_valid_task_id(task_id):
        return HttpResponse('task_id not found.', status=404)

    try:
        is_finished, error = task_manager.query_status(task_id)
    except Exception as e:
        logger.debug(e)
        return HttpResponse(e, status=500)

    if error:
        return HttpResponse(error, status=500)
    return JsonResponse({'is_finished': is_finished})


def get_event_log_by_time_to_excel(session, tstart, tend, log_type, task_id):

    if not isinstance(tstart, (int, float)) or not isinstance(tend, (int, float)):
        logger.error('Invalid time range parameter')
        raise RuntimeError('Invalid time range parameter')

    if log_type == 'fileaudit':
        obj = FileAudit
        stmt = select(obj).where(obj.timestamp.between(datetime.datetime.utcfromtimestamp(tstart),
                                                       datetime.datetime.utcfromtimestamp(tend)))
        stmt = stmt.order_by(desc(obj.timestamp))
        res = session.scalars(stmt).all()

        head = [_("User"), _("Type"), _("IP"), _("Device"), _("Date"),
                _("Library Name"), _("Library ID"), _("Library Owner"), _("File Path")]
        data_list = []

        for ev in res:
            event_type, ev.show_device = generate_file_audit_event_type(ev)

            repo_id = ev.repo_id
            repo = seafile_api.get_repo(repo_id)
            if repo:
                repo_name = repo.name
                repo_owner = seafile_api.get_repo_owner(repo_id) or \
                             seafile_api.get_org_repo_owner(repo_id)
            else:
                repo_name = _('Deleted')
                repo_owner = '--'

            username = ev.user if ev.user else _('Anonymous User')
            date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if \
                ev.timestamp else ''

            row = [username, event_type, ev.ip, ev.show_device,
                   date, repo_name, ev.repo_id, repo_owner, ev.file_path]
            data_list.append(row)

        wb = write_xls('file-access-logs', head, data_list)
        if not wb:
            logger.error('Failed to export excel')
            raise RuntimeError('Failed to export excel')

        target_dir = os.path.join('/tmp/seafile_events/', task_id)
        os.makedirs(target_dir, exist_ok=True)
        excel_name = 'file-access-logs.xlsx'
        target_path = os.path.join(target_dir, excel_name)
        wb.save(target_path)
    elif log_type == 'fileupdate':
        obj = FileUpdate
        stmt = select(obj).where(obj.timestamp.between(datetime.datetime.utcfromtimestamp(tstart),
                                                       datetime.datetime.utcfromtimestamp(tend)))
        stmt = stmt.order_by(desc(obj.timestamp))
        res = session.scalars(stmt).all()
        head = [_("User"), _("Date"), _("Library Name"), _("Library ID"),
                _("Library Owner"), _("Action")]
        data_list = []

        for ev in res:

            repo_id = ev.repo_id
            repo = seafile_api.get_repo(repo_id)
            if repo:
                repo_name = repo.name
                repo_owner = seafile_api.get_repo_owner(repo_id) or \
                             seafile_api.get_org_repo_owner(repo_id)
            else:
                repo_name = _('Deleted')
                repo_owner = '--'

            username = ev.user if ev.user else _('Anonymous User')
            date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if \
                ev.timestamp else ''

            row = [username, date, repo_name, ev.repo_id, repo_owner, ev.file_oper.strip()]
            data_list.append(row)

        wb = write_xls('file-update-logs', head, data_list)
        if not wb:
            logger.error('Failed to export excel')
            raise RuntimeError('Failed to export excel')

        target_dir = os.path.join('/tmp/seafile_events/', task_id)
        os.makedirs(target_dir, exist_ok=True)
        excel_name = 'file-update-logs.xlsx'
        target_path = os.path.join(target_dir, excel_name)
        wb.save(target_path)

    elif log_type == 'permaudit':
        obj = PermAudit
        stmt = select(obj).where(obj.timestamp.between(datetime.datetime.utcfromtimestamp(tstart),
                                                       datetime.datetime.utcfromtimestamp(tend)))
        stmt = stmt.order_by(desc(obj.timestamp))
        res = session.scalars(stmt).all()
        head = [_("From"), _("To"), _("Action"), _("Permission"), _("Library"),
                _("Folder Path"), _("Date")]
        data_list = []

        for ev in res:
            repo = seafile_api.get_repo(ev.repo_id)
            repo_name = repo.repo_name if repo else _('Deleted')

            if '@' in ev.to:
                to = ev.to
            elif ev.to.isdigit():
                group = ccnet_api.get_group(int(ev.to))
                to = group.group_name if group else _('Deleted')
            elif 'all' in ev.to:
                to = _('Organization')
            else:
                to = '--'

            if 'add' in ev.etype:
                action = _('Add')
            elif 'modify' in ev.etype:
                action = _('Modify')
            elif 'delete' in ev.etype:
                action = _('Delete')
            else:
                action = '--'

            if ev.permission == 'rw':
                permission = _('Read-Write')
            elif ev.permission == 'r':
                permission = _('Read-Only')
            else:
                permission = '--'

            date = utc_to_local(ev.timestamp).strftime('%Y-%m-%d %H:%M:%S') if \
                ev.timestamp else ''

            row = [ev.from_user, to, action, permission, repo_name,
                   ev.file_path, date]
            data_list.append(row)

        wb = write_xls('perm-audit-logs', head, data_list)
        if not wb:
            logger.error('Failed to export excel')
            raise RuntimeError('Failed to export excel')

        target_dir = os.path.join('/tmp/seafile_events/', task_id)
        os.makedirs(target_dir, exist_ok=True)
        excel_name = 'perm-audit-logs.xlsx'
        target_path = os.path.join(target_dir, excel_name)
        wb.save(target_path)
    elif log_type == 'loginadmin':
        obj = UserLogin
        stmt = select(obj).where(obj.login_date.between(datetime.datetime.utcfromtimestamp(tstart),
                                                       datetime.datetime.utcfromtimestamp(tend)))
        stmt = stmt.order_by(desc(obj.login_date))
        res = session.scalars(stmt).all()

        head = [_("Name"), _("IP"), _("Status"), _("Time")]
        data_list = []
        for log in res:
            login_time = log.login_date.strftime("%Y-%m-%d %H:%M:%S")
            status = _('Success') if log.login_success else _('Failed')
            row = [log.username, log.login_ip, status, login_time]
            data_list.append(row)

        wb = write_xls('login-logs', head, data_list)
        if not wb:
            logger.error('Failed to export excel')
            raise RuntimeError('Failed to export excel')

        target_dir = os.path.join('/tmp/seafile_events/', task_id)
        os.makedirs(target_dir, exist_ok=True)
        excel_name = 'login-logs.xlsx'
        target_path = os.path.join(target_dir, excel_name)
        wb.save(target_path)
    else:
        logger.error('Invalid log_type parameter')
        raise RuntimeError('Invalid log_type parameter')


