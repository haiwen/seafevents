from sqlalchemy import desc
from sqlalchemy import func
from sqlalchemy import distinct
from datetime import datetime

from models import UserActivityStat, UserTraffic, SysTraffic, \
                   FileOpsStat, TotalStorageStat, MonthlyUserTraffic, MonthlySysTraffic

from seaserv import seafile_api, get_org_id_by_repo_id
from seafevents.app.config import appconfig

repo_org = {}
is_org = -1

def get_org_id(repo_id):
    global is_org
    if is_org == -1:
        org_conf = seafile_api.get_server_config_string('general', 'multi_tenancy')
        if org_conf.lower() == 'true':
            is_org = 1
        else:
            is_org = 0
    if not is_org:
        return 0

    if not repo_org.has_key(repo_id):
        org_id = get_org_id_by_repo_id(repo_id)
        if org_id == -1:
            org_id = 0
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
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

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
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.number))
    return ret

def _get_total_storage_stats(session, start, end, offset='+00:00'):
    q = session.query(func.convert_tz(TotalStorageStat.timestamp, '+00:00', offset).label("timestamp"),
                      TotalStorageStat.total_size).filter(
                      TotalStorageStat.timestamp.between(
                      func.convert_tz(start, offset, '+00:00'),
                      func.convert_tz(end, offset, '+00:00'))).order_by("timestamp")

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((row.timestamp, row.total_size))
    return ret

def get_total_storage_stats_by_day(session, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    rets = _get_total_storage_stats (session, start_at_0, end_at_23, offset)
    rets.reverse()

    '''
    Traverse data from end to start,
    record the last piece of data in each day.
    '''

    last_date = None
    res = []
    for ret in rets:
        cur_time = ret[0]
        cur_num = ret[1]
        cur_date = datetime.date (cur_time)
        if cur_date != last_date or last_date == None:
            res.append((datetime.strptime(str(cur_date),'%Y-%m-%d'), cur_num))
            last_date = cur_date
        else:
            last_date = cur_date

    res.reverse()
    return res

def get_file_ops_stats_by_day(session, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

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
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.op_type, long(row.number)))
    return ret

def get_org_user_traffic_by_day(session, org_id, user, start, end, offset='+00:00', op_type='all'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

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
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.op_type, long(row.size)))
    return ret

def get_user_traffic_by_day(session, user, start, end, offset='+00:00', op_type='all'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

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
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.op_type, long(row.size)))
    return ret

def get_org_traffic_by_day(session, org_id, start, end, offset='+00:00', op_type='all'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

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
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.op_type, long(row.size)))
    return ret

def get_system_traffic_by_day(session, start, end, offset='+00:00', op_type='all'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

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
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.op_type, long(row.size)))
    return ret

def get_user_traffic_by_month(session, user, start, end, offset='+00:00', op_type='all'):
    # offset is not supported for now

    start_str = start.strftime('%Y-%m-01 00:00:00')
    end_str = end.strftime('%Y-%m-01 00:00:00')
    start_date = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    if op_type == 'web-file-upload' or op_type == 'web-file-download' or op_type == 'sync-file-download' \
       or op_type == 'sync-file-upload' or op_type == 'link-file-upload' or op_type == 'link-file-download':
        q = session.query(func.date(MonthlyUserTraffic.timestamp).label("timestamp"),
                          func.sum(MonthlyUserTraffic.size).label("size"),
                          MonthlyUserTraffic.op_type).filter(MonthlyUserTraffic.timestamp.between(
                          start_date, end_date),
                          MonthlyUserTraffic.user==user,
                          MonthlyUserTraffic.op_type==op_type).group_by(
                          func.date(MonthlyUserTraffic.timestamp),
                          MonthlyUserTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        q = session.query(func.date(MonthlyUserTraffic.timestamp).label("timestamp"),
                          func.sum(MonthlyUserTraffic.size).label("size"),
                          MonthlyUserTraffic.op_type).filter(MonthlyUserTraffic.timestamp.between(
                          start_date, end_date),
                          MonthlyUserTraffic.user==user).group_by(
                          func.date(MonthlyUserTraffic.timestamp),
                          MonthlyUserTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.op_type, long(row.size)))
    return ret

def get_org_user_traffic_by_month(session, org_id, user, start, end, offset='+00:00', op_type='all'):
    # offset is not supported for now

    start_str = start.strftime('%Y-%m-01 00:00:00')
    end_str = end.strftime('%Y-%m-01 00:00:00')
    start_date = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    if op_type == 'web-file-upload' or op_type == 'web-file-download' or op_type == 'sync-file-download' \
       or op_type == 'sync-file-upload' or op_type == 'link-file-upload' or op_type == 'link-file-download':
        q = session.query(func.date(MonthlyUserTraffic.timestamp).label("timestamp"),
                          func.sum(MonthlyUserTraffic.size).label("size"),
                          MonthlyUserTraffic.op_type).filter(MonthlyUserTraffic.timestamp.between(
                          start_date, end_date),
                          MonthlyUserTraffic.user==user,
                          MonthlyUserTraffic.org_id==org_id,
                          MonthlyUserTraffic.op_type==op_type).group_by(
                          func.date(MonthlyUserTraffic.timestamp),
                          MonthlyUserTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        q = session.query(func.date(MonthlyUserTraffic.timestamp).label("timestamp"),
                          func.sum(MonthlyUserTraffic.size).label("size"),
                          MonthlyUserTraffic.op_type).filter(MonthlyUserTraffic.timestamp.between(
                          start_date, end_date),
                          MonthlyUserTraffic.user==user,
                          MonthlyUserTraffic.org_id==org_id).group_by(
                          func.date(MonthlyUserTraffic.timestamp),
                          MonthlyUserTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.op_type, long(row.size)))
    return ret

def get_system_traffic_by_month(session, start, end, offset='+00:00', op_type='all'):
    # offset is not supported for now

    start_str = start.strftime('%Y-%m-01 00:00:00')
    end_str = end.strftime('%Y-%m-01 00:00:00')
    start_date = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    if op_type == 'web-file-upload' or op_type == 'web-file-download' or op_type == 'sync-file-download' \
       or op_type == 'sync-file-upload' or op_type == 'link-file-upload' or op_type == 'link-file-download':
        q = session.query(func.date(MonthlySysTraffic.timestamp).label("timestamp"),
                          func.sum(MonthlySysTraffic.size).label("size"),
                          MonthlySysTraffic.op_type).filter(MonthlySysTraffic.timestamp.between(
                          start_date, end_date),
                          MonthlySysTraffic.op_type==op_type).group_by(
                          func.date(MonthlySysTraffic.timestamp),
                          MonthlySysTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        q = session.query(func.date(MonthlySysTraffic.timestamp).label("timestamp"),
                          func.sum(MonthlySysTraffic.size).label("size"),
                          MonthlySysTraffic.op_type).filter(MonthlySysTraffic.timestamp.between(
                          start_date, end_date)).group_by(
                          func.date(MonthlySysTraffic.timestamp),
                          MonthlySysTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.op_type, long(row.size)))
    return ret

def get_org_traffic_by_month(session, org_id, start, end, offset='+00:00', op_type='all'):
    # offset is not supported for now

    start_str = start.strftime('%Y-%m-01 00:00:00')
    end_str = end.strftime('%Y-%m-01 00:00:00')
    start_date = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    if op_type == 'web-file-upload' or op_type == 'web-file-download' or op_type == 'sync-file-download' \
       or op_type == 'sync-file-upload' or op_type == 'link-file-upload' or op_type == 'link-file-download':
        q = session.query(func.date(MonthlySysTraffic.timestamp).label("timestamp"),
                          func.sum(MonthlySysTraffic.size).label("size"),
                          MonthlySysTraffic.op_type).filter(MonthlySysTraffic.timestamp.between(
                          start_date, end_date),
                          MonthlySysTraffic.org_id==org_id,
                          MonthlySysTraffic.op_type==op_type).group_by(
                          func.date(MonthlySysTraffic.timestamp),
                          MonthlySysTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        q = session.query(func.date(MonthlySysTraffic.timestamp).label("timestamp"),
                          func.sum(MonthlySysTraffic.size).label("size"),
                          MonthlySysTraffic.op_type).filter(MonthlySysTraffic.timestamp.between(
                          start_date, end_date),
                          MonthlySysTraffic.org_id==org_id).group_by(
                          func.date(MonthlySysTraffic.timestamp),
                          MonthlySysTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.op_type, long(row.size)))
    return ret

