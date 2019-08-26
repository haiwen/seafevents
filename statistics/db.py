import logging
from sqlalchemy import desc
from sqlalchemy import func
from sqlalchemy import distinct
from datetime import datetime

from .models import UserActivityStat, UserTraffic, SysTraffic, \
                   FileOpsStat, TotalStorageStat, MonthlyUserTraffic, MonthlySysTraffic

from seaserv import seafile_api, get_org_id_by_repo_id
from seafevents.app.config import appconfig

repo_org = {}
is_org = -1

def get_org_id(repo_id):
    global is_org
    if is_org == -1:
        org_conf = seafile_api.get_server_config_string('general', 'multi_tenancy')
        if not org_conf:
            is_org = 0
        elif org_conf.lower() == 'true':
            is_org = 1
        else:
            is_org = 0
    if not is_org:
        return -1

    if repo_id not in repo_org:
        org_id = get_org_id_by_repo_id(repo_id)
        repo_org[repo_id] = org_id
    else:
        org_id = repo_org[repo_id]

    return org_id

# sqlalchemy session.query(func.date(timestamp)) returns datetime.date,
# we return datetime.datetime in our apis, convert datetime.date to datetime.datetime:
# date->str->datetime: datetime.strptime(str(row.timestamp),'%Y-%m-%d')

def get_user_activity_stats_by_day(session, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    # offset is not supported for now
    offset='+00:00'

    q = session.query(func.date(func.convert_tz(UserActivityStat.timestamp, '+00:00', offset)).label("timestamp"),
                      func.count(distinct(UserActivityStat.username)).label("number")).filter(
                      UserActivityStat.timestamp.between(
                      func.convert_tz(start_at_0, offset, '+00:00'),
                      func.convert_tz(end_at_23, offset, '+00:00'))).group_by(
                      func.date(func.convert_tz(UserActivityStat.timestamp, '+00:00', offset))).order_by("timestamp")
    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp), '%Y-%m-%d'), row.number))
    return ret

def get_org_user_activity_stats_by_day(org_id, start, end):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
    ret = []
    try:
        session = appconfig.session_cls()
        q = session.query(UserActivityStat.timestamp.label("timestamp"),
                          func.count(UserActivityStat.username).label("number"))
        q = q.filter(UserActivityStat.timestamp.between(start_at_0, end_at_23),
                     UserActivityStat.org_id==org_id).group_by(
                     "timestamp").order_by("timestamp")
        rows = q.all()

        for row in rows:
            timestamp = row.timestamp
            num = row.number
            ret.append({"timestamp":timestamp, "number":num})
    except Exception as e:
        logging.warning('Failed to get org-user activities by day: %s.', e)
    finally:
        session.close()

    return ret

def _get_total_storage_stats(start, end, offset='+00:00', org_id=0):
    ret = []
    try:
        session = appconfig.session_cls()
        q = session.query(func.convert_tz(TotalStorageStat.timestamp, '+00:00', offset).label("timestamp"),
                          func.sum(TotalStorageStat.total_size).label("total_size"))
        if org_id == 0:
            q = q.filter(TotalStorageStat.timestamp.between(
                         func.convert_tz(start, offset, '+00:00'),
                         func.convert_tz(end, offset, '+00:00')))
        else:
            q = q.filter(TotalStorageStat.timestamp.between(
                         func.convert_tz(start, offset, '+00:00'),
                         func.convert_tz(end, offset, '+00:00')),
                         TotalStorageStat.org_id==org_id)
        q = q.group_by("timestamp").order_by("timestamp")
        rows = q.all()

        for row in rows:
            ret.append((row.timestamp, row.total_size))
    except Exception as e:
        logging.warning('Failed to get total storage: %s.', e)
    finally:
        session.close()

    return ret

def get_total_storage_stats_by_day(session, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    results = _get_total_storage_stats (start_at_0, end_at_23, offset)
    results.reverse()

    '''
    Traverse data from end to start,
    record the last piece of data in each day.
    '''

    last_date = None
    ret = []
    for result in results:
        cur_time = result[0]
        cur_num = result[1]
        cur_date = datetime.date (cur_time)
        if cur_date != last_date or last_date == None:
            ret.append((datetime.strptime(str(cur_date), '%Y-%m-%d'), cur_num))
            last_date = cur_date

    ret.reverse()
    return ret

def get_org_storage_stats_by_day(org_id, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    results = _get_total_storage_stats (start_at_0, end_at_23, offset, org_id)
    results.reverse()

    '''
    Traverse data from end to start,
    record the last piece of data in each day.
    '''

    last_date = None
    ret = []
    for result in results:
        cur_time = result[0]
        cur_num = result[1]
        cur_date = datetime.date (cur_time)
        if cur_date != last_date or last_date == None:
            ret.append({"timestamp":datetime.strptime(str(cur_date), '%Y-%m-%d'),\
                        "number" : cur_num})
            last_date = cur_date
    ret.reverse()

    return ret

def get_file_ops_stats_by_day(session, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    q = session.query(func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)).label("timestamp"),
                      func.sum(FileOpsStat.number).label("number"),
                      FileOpsStat.op_type).filter(FileOpsStat.timestamp.between(
                      func.convert_tz(start_at_0, offset, '+00:00'),
                      func.convert_tz(end_at_23, offset, '+00:00'))).group_by(
                      func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)),
                      FileOpsStat.op_type).order_by("timestamp")

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp), '%Y-%m-%d'), row.op_type, int(row.number)))
    return ret

def get_org_file_ops_stats_by_day(org_id, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
    ret = []

    try:
        session = appconfig.session_cls()
        q = session.query(func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)).label("timestamp"),
                          func.sum(FileOpsStat.number).label("number"),
                          FileOpsStat.op_type)
        q = q.filter(FileOpsStat.timestamp.between(
                     func.convert_tz(start_at_0, offset, '+00:00'),
                     func.convert_tz(end_at_23, offset, '+00:00')),
                     FileOpsStat.org_id==org_id)
        q = q.group_by(func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)),
                       FileOpsStat.op_type).order_by("timestamp")

        rows = q.all()

        for row in rows:
            timestamp = datetime.strptime(str(row.timestamp), '%Y-%m-%d')
            op_type = row.op_type
            num = int(row.number)
            ret.append({"timestamp":timestamp, "op_type":op_type, "number":num})
    except Exception as e:
        logging.warning('Failed to get org-file operations data: %s.', e)
    finally:
        session.close()

    return ret

def get_org_user_traffic_by_day(session, org_id, user, start, end, offset='+00:00', op_type='all'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    # offset is not supported for now
    offset='+00:00'

    if op_type == 'web-file-upload' or op_type == 'web-file-download' or op_type == 'sync-file-download' \
       or op_type == 'sync-file-upload' or op_type == 'link-file-upload' or op_type == 'link-file-download':
        q = session.query(func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                          func.sum(UserTraffic.size).label("size"),
                          UserTraffic.op_type).filter(UserTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          UserTraffic.user==user,
                          UserTraffic.op_type==op_type,
                          UserTraffic.org_id==org_id).group_by(
                          func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)),
                          UserTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        q = session.query(func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                          func.sum(UserTraffic.size).label("size"),
                          UserTraffic.op_type).filter(UserTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          UserTraffic.user==user,
                          UserTraffic.org_id==org_id).group_by(
                          func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)),
                          UserTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp), '%Y-%m-%d'), row.op_type, int(row.size)))
    return ret

def get_user_traffic_by_day(session, user, start, end, offset='+00:00', op_type='all'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    # offset is not supported for now
    offset='+00:00'

    if op_type == 'web-file-upload' or op_type == 'web-file-download' or op_type == 'sync-file-download' \
       or op_type == 'sync-file-upload' or op_type == 'link-file-upload' or op_type == 'link-file-download':
        q = session.query(func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                          func.sum(UserTraffic.size).label("size"),
                          UserTraffic.op_type).filter(UserTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          UserTraffic.user==user,
                          UserTraffic.op_type==op_type).group_by(
                          func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)),
                          UserTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        q = session.query(func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                          func.sum(UserTraffic.size).label("size"),
                          UserTraffic.op_type).filter(UserTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          UserTraffic.user==user).group_by(
                          func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)),
                          UserTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp), '%Y-%m-%d'), row.op_type, int(row.size)))
    return ret

def get_org_traffic_by_day(session, org_id, start, end, offset='+00:00', op_type='all'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    # offset is not supported for now
    offset='+00:00'

    if op_type == 'web-file-upload' or op_type == 'web-file-download' or op_type == 'sync-file-download' \
       or op_type == 'sync-file-upload' or op_type == 'link-file-upload' or op_type == 'link-file-download':
        q = session.query(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                          func.sum(SysTraffic.size).label("size"),
                          SysTraffic.op_type).filter(SysTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          SysTraffic.org_id==org_id,
                          SysTraffic.op_type==op_type).group_by(
                          SysTraffic.org_id,
                          func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
                          SysTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        q = session.query(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                          func.sum(SysTraffic.size).label("size"),
                          SysTraffic.op_type).filter(SysTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          SysTraffic.org_id==org_id).group_by(
                          SysTraffic.org_id,
                          func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
                          SysTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp), '%Y-%m-%d'), row.op_type, int(row.size)))
    return ret

def get_system_traffic_by_day(session, start, end, offset='+00:00', op_type='all'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    # offset is not supported for now
    offset='+00:00'

    if op_type == 'web-file-upload' or op_type == 'web-file-download' or op_type == 'sync-file-download' \
       or op_type == 'sync-file-upload' or op_type == 'link-file-upload' or op_type == 'link-file-download':
        q = session.query(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                          func.sum(SysTraffic.size).label("size"),
                          SysTraffic.op_type).filter(SysTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          SysTraffic.op_type==op_type).group_by(
                          func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
                          SysTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        q = session.query(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                          func.sum(SysTraffic.size).label("size"),
                          SysTraffic.op_type).filter(SysTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00'))).group_by(
                          func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
                          SysTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp), '%Y-%m-%d'), row.op_type, int(row.size)))
    return ret

"""
def get_user_traffic_by_month(user, start, end):
    start_str = start.strftime('%Y-%m-01 00:00:00')
    end_str = end.strftime('%Y-%m-01 00:00:00')
    start_date = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    ret = []
    try:
        session = appconfig.session_cls()
        q = session.query(MonthlyUserTraffic).filter(MonthlyUserTraffic.timestamp.between(
                          start_date, end_date),
                          MonthlyUserTraffic.user==user).order_by(
                          MonthlyUserTraffic.timestamp, MonthlyUserTraffic.user)

        rows = q.all()

        for row in rows:
            d = row.__dict__
            d.pop('_sa_instance_state')
            ret.append(d)

    except Exception as e:
        logging.warning('Failed to get user traffic by month: %s.', e)
    finally:
        session.close()

    return ret

def get_system_traffic_by_month(start, end):
    start_str = start.strftime('%Y-%m-01 00:00:00')
    end_str = end.strftime('%Y-%m-01 00:00:00')
    start_date = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    ret = []
    try:
        session = appconfig.session_cls()
        q = session.query(MonthlySysTraffic.timestamp,
                          func.sum(MonthlySysTraffic.web_file_upload).label('web_file_upload'),
                          func.sum(MonthlySysTraffic.web_file_download).label('web_file_download'),
                          func.sum(MonthlySysTraffic.link_file_upload).label('link_file_upload'),
                          func.sum(MonthlySysTraffic.link_file_download).label('link_file_download'),
                          func.sum(MonthlySysTraffic.sync_file_upload).label('sync_file_upload'),
                          func.sum(MonthlySysTraffic.sync_file_download).label('sync_file_download')
                          ).filter(MonthlySysTraffic.timestamp.between(
                          start_date, end_date)).group_by(MonthlySysTraffic.timestamp).order_by(
                          MonthlySysTraffic.timestamp)

        rows = q.all()

        for row in rows:
            d = {"timestamp": row.timestamp,
                 "web_file_upload": long(row.web_file_upload),
                 "web_file_download": long(row.web_file_download),
                 "link_file_upload": long(row.link_file_upload),
                 "link_file_download": long(row.link_file_download),
                 "sync_file_upload": long(row.sync_file_upload),
                 "sync_file_download": long(row.sync_file_download)}
            ret.append(d)

    except Exception as e:
        logging.warning('Failed to get system traffic by month: %s.', e)
    finally:
        session.close()

    return ret

def get_org_traffic_by_month(org_id, start, end):
    start_str = start.strftime('%Y-%m-01 00:00:00')
    end_str = end.strftime('%Y-%m-01 00:00:00')
    start_date = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    ret = []
    try:
        session = appconfig.session_cls()
        q = session.query(MonthlySysTraffic).filter(
                          MonthlySysTraffic.timestamp.between(
                          start_date, end_date),
                          MonthlySysTraffic.org_id==org_id).order_by(
                          MonthlySysTraffic.timestamp)

        rows = q.all()

        for row in rows:
            d = row.__dict__
            d.pop('_sa_instance_state')
            ret.append(d)

    except Exception as e:
        logging.warning('Failed to get org traffic by month: %s.', e)
    finally:
        session.close()

    return ret
"""

def get_all_users_traffic_by_month(month, start=-1, limit=-1, order_by='user', org_id=-1):
    month_str = month.strftime('%Y-%m-01 00:00:00')
    _month = datetime.strptime(month_str, '%Y-%m-%d %H:%M:%S')

    ret = []
    try:
        session = appconfig.session_cls()
        q = session.query(MonthlyUserTraffic).filter(
                          MonthlyUserTraffic.timestamp==_month,
                          MonthlyUserTraffic.org_id==org_id)
        if order_by == 'user':
            q = q.order_by(MonthlyUserTraffic.user)
        elif order_by == 'user_desc':
            q = q.order_by(desc(MonthlyUserTraffic.user))
        elif order_by == 'web_file_upload':
            q = q.order_by(MonthlyUserTraffic.web_file_upload)
        elif order_by == 'web_file_upload_desc':
            q = q.order_by(desc(MonthlyUserTraffic.web_file_upload))
        elif order_by == 'web_file_download':
            q = q.order_by(MonthlyUserTraffic.web_file_download)
        elif order_by == 'web_file_download_desc':
            q = q.order_by(desc(MonthlyUserTraffic.web_file_download))
        elif order_by == 'link_file_upload':
            q = q.order_by(MonthlyUserTraffic.link_file_upload)
        elif order_by == 'link_file_upload_desc':
            q = q.order_by(desc(MonthlyUserTraffic.link_file_upload))
        elif order_by == 'link_file_download':
            q = q.order_by(MonthlyUserTraffic.link_file_download)
        elif order_by == 'link_file_download_desc':
            q = q.order_by(desc(MonthlyUserTraffic.link_file_download))
        elif order_by == 'sync_file_upload':
            q = q.order_by(MonthlyUserTraffic.sync_file_upload)
        elif order_by == 'sync_file_upload_desc':
            q = q.order_by(desc(MonthlyUserTraffic.sync_file_upload))
        elif order_by == 'sync_file_download':
            q = q.order_by(MonthlyUserTraffic.sync_file_download)
        elif order_by == 'sync_file_download_desc':
            q = q.order_by(desc(MonthlyUserTraffic.sync_file_download))
        else:
            logging.warning("Failed to get all users traffic by month, unkown order_by '%s'.", order_by)
            session.close()
            return []

        if start>=0 and limit>0:
            q = q.slice(start, start + limit)
        rows = q.all()

        for row in rows:
            d = row.__dict__
            d.pop('_sa_instance_state')
            d.pop('id')
            ret.append(d)

    except Exception as e:
        logging.warning('Failed to get all users traffic by month: %s.', e)
    finally:
        session.close()

    return ret

def get_all_orgs_traffic_by_month(month, start=-1, limit=-1, order_by='org_id'):
    month_str = month.strftime('%Y-%m-01 00:00:00')
    _month = datetime.strptime(month_str, '%Y-%m-%d %H:%M:%S')

    ret = []
    try:
        session = appconfig.session_cls()
        q = session.query(MonthlySysTraffic).filter(MonthlySysTraffic.timestamp==_month,
                                                    MonthlySysTraffic.org_id>0)

        if order_by == 'org_id':
            q = q.order_by(MonthlySysTraffic.org_id)
        elif order_by == 'org_id_desc':
            q = q.order_by(desc(MonthlySysTraffic.org_id))
        elif order_by == 'web_file_upload':
            q = q.order_by(MonthlySysTraffic.web_file_upload)
        elif order_by == 'web_file_upload_desc':
            q = q.order_by(desc(MonthlySysTraffic.web_file_upload))
        elif order_by == 'web_file_download':
            q = q.order_by(MonthlySysTraffic.web_file_download)
        elif order_by == 'web_file_download_desc':
            q = q.order_by(desc(MonthlySysTraffic.web_file_download))
        elif order_by == 'link_file_upload':
            q = q.order_by(MonthlySysTraffic.link_file_upload)
        elif order_by == 'link_file_upload_desc':
            q = q.order_by(desc(MonthlySysTraffic.link_file_upload))
        elif order_by == 'link_file_download':
            q = q.order_by(MonthlySysTraffic.link_file_download)
        elif order_by == 'link_file_download_desc':
            q = q.order_by(desc(MonthlySysTraffic.link_file_download))
        elif order_by == 'sync_file_upload':
            q = q.order_by(MonthlySysTraffic.sync_file_upload)
        elif order_by == 'sync_file_upload_desc':
            q = q.order_by(desc(MonthlySysTraffic.sync_file_upload))
        elif order_by == 'sync_file_download':
            q = q.order_by(MonthlySysTraffic.sync_file_download)
        elif order_by == 'sync_file_download_desc':
            q = q.order_by(desc(MonthlySysTraffic.sync_file_download))
        else:
            logging.warning("Failed to get all orgs traffic by month, unkown order_by '%s'.", order_by)
            return []

        if start>=0 and limit>0:
            q = q.slice(start, start + limit)
        rows = q.all()

        for row in rows:
            d = row.__dict__
            d.pop('_sa_instance_state')
            d.pop('id')
            ret.append(d)

    except Exception as e:
        logging.warning('Failed to get all users traffic by month: %s.', e)
    finally:
        session.close()

    return ret

def get_user_traffic_by_month (user, month):
    month_str = month.strftime('%Y-%m-01 00:00:00')
    _month = datetime.strptime(month_str, '%Y-%m-%d %H:%M:%S')

    ret = {}
    try:
        session = appconfig.session_cls()
        q = session.query(MonthlyUserTraffic).filter(MonthlyUserTraffic.timestamp==_month,
                                                     MonthlyUserTraffic.user==user)
        result = q.first()
        if result:
            d = result.__dict__
            d.pop('_sa_instance_state')
            d.pop('id')
            ret = d
    except Exception as e:
        logging.warning('Failed to get user traffic by month: %s.', e)
    finally:
        session.close()

    return ret
