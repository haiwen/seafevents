import logging
from sqlalchemy import desc, func, distinct, select
from datetime import datetime

from .models import UserActivityStat, UserTraffic, SysTraffic, \
                   FileOpsStat, TotalStorageStat, MonthlyUserTraffic, MonthlySysTraffic

from seaserv import seafile_api, get_org_id_by_repo_id

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


def get_user_activity_stats_by_day(session, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    # offset is not supported for now
    offset='+00:00'

    stmt = select(func.date(func.convert_tz(UserActivityStat.timestamp, '+00:00', offset)).label("timestamp"),
                  func.count(distinct(UserActivityStat.username)).label("number")).where(
                  UserActivityStat.timestamp.between(
                      func.convert_tz(start_at_0, offset, '+00:00'),
                      func.convert_tz(end_at_23, offset, '+00:00'))).group_by(
                      func.date(func.convert_tz(UserActivityStat.timestamp, '+00:00', offset))).order_by("timestamp")
    rows = session.execute(stmt).all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row[0]), '%Y-%m-%d'), row[1]))
    return ret

def get_org_user_activity_stats_by_day(session, org_id, start, end):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
    ret = []

    stmt = select(UserActivityStat.timestamp.label("timestamp"),
                  func.count(UserActivityStat.username).label("number")).where(
                  UserActivityStat.timestamp.between(start_at_0, end_at_23),
                  UserActivityStat.org_id == org_id).group_by("timestamp").order_by("timestamp")
    try:
        rows = session.execute(stmt).all()

        for row in rows:
            timestamp = row[0]
            num = row[1]
            ret.append({"timestamp":timestamp, "number":num})
    except Exception as e:
        logging.warning('Failed to get org-user activities by day: %s.', e)

    return ret

def _get_total_storage_stats(session, start, end, offset='+00:00', org_id=0):
    ret = []
    try:
        stmt = select(func.convert_tz(TotalStorageStat.timestamp, '+00:00', offset).label("timestamp"),
                      func.sum(TotalStorageStat.total_size).label("total_size"))
        if org_id == 0:
            stmt = stmt.where(TotalStorageStat.timestamp.between(
                         func.convert_tz(start, offset, '+00:00'),
                         func.convert_tz(end, offset, '+00:00')))
        else:
            stmt = stmt.where(TotalStorageStat.timestamp.between(
                         func.convert_tz(start, offset, '+00:00'),
                         func.convert_tz(end, offset, '+00:00')),
                         TotalStorageStat.org_id == org_id)
        stmt = stmt.group_by("timestamp").order_by("timestamp")
        rows = session.execute(stmt).all()

        for row in rows:
            ret.append((row[0], row[1]))
    except Exception as e:
        logging.warning('Failed to get total storage: %s.', e)

    return ret

def get_total_storage_stats_by_day(session, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    results = _get_total_storage_stats(session, start_at_0, end_at_23, offset)
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
        cur_date = datetime.date(cur_time)
        if cur_date != last_date or last_date is None:
            ret.append((datetime.strptime(str(cur_date), '%Y-%m-%d'), cur_num))
            last_date = cur_date

    ret.reverse()
    return ret

def get_org_storage_stats_by_day(session, org_id, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    results = _get_total_storage_stats(session, start_at_0, end_at_23, offset, org_id)
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
        cur_date = datetime.date(cur_time)
        if cur_date != last_date or last_date is None:
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

    stmt = select(func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)).label("timestamp"),
                  func.sum(FileOpsStat.number).label("number"),
                  FileOpsStat.op_type).where(FileOpsStat.timestamp.between(
                      func.convert_tz(start_at_0, offset, '+00:00'),
                      func.convert_tz(end_at_23, offset, '+00:00'))).group_by(
                      func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)),
                      FileOpsStat.op_type).order_by("timestamp")

    rows = session.execute(stmt).all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row[0]), '%Y-%m-%d'), row[2], int(row[1])))
    return ret

def get_org_file_ops_stats_by_day(session, org_id, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
    ret = []

    try:
        stmt = select(func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)).label("timestamp"),
                      func.sum(FileOpsStat.number).label("number"), FileOpsStat.op_type).where(
                      FileOpsStat.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                      FileOpsStat.org_id == org_id).group_by(
                          func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)),
                          FileOpsStat.op_type).order_by("timestamp")

        rows = session.execute(stmt).all()

        for row in rows:
            timestamp = datetime.strptime(str(row[0]), '%Y-%m-%d')
            op_type = row[2]
            num = int(row[1])
            ret.append({"timestamp":timestamp, "op_type":op_type, "number":num})
    except Exception as e:
        logging.warning('Failed to get org-file operations data: %s.', e)

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
        stmt = select(func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                      func.sum(UserTraffic.size).label("size"),
                      UserTraffic.op_type).where(UserTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          UserTraffic.user == user,
                          UserTraffic.op_type == op_type,
                          UserTraffic.org_id == org_id).group_by(
                          func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)),
                          UserTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        stmt = select(func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                      func.sum(UserTraffic.size).label("size"),
                      UserTraffic.op_type).where(UserTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          UserTraffic.user == user,
                          UserTraffic.org_id == org_id).group_by(
                          func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)),
                          UserTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = session.execute(stmt).all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row[0]), '%Y-%m-%d'), row[2], int(row[1])))
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
        stmt = select(func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                      func.sum(UserTraffic.size).label("size"),
                      UserTraffic.op_type).where(UserTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          UserTraffic.user == user,
                          UserTraffic.op_type == op_type).group_by(
                          func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)),
                          UserTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        stmt = select(func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                      func.sum(UserTraffic.size).label("size"),
                      UserTraffic.op_type).where(UserTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          UserTraffic.user == user).group_by(
                          func.date(func.convert_tz(UserTraffic.timestamp, '+00:00', offset)),
                          UserTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = session.execute(stmt).all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row[0]), '%Y-%m-%d'), row[2], int(row[1])))
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
        stmt = select(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                      func.sum(SysTraffic.size).label("size"),
                      SysTraffic.op_type).where(SysTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          SysTraffic.org_id == org_id,
                          SysTraffic.op_type == op_type).group_by(
                          SysTraffic.org_id,
                          func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
                          SysTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        stmt = select(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                      func.sum(SysTraffic.size).label("size"),
                      SysTraffic.op_type).where(SysTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          SysTraffic.org_id == org_id).group_by(
                          SysTraffic.org_id,
                          func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
                          SysTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = session.execute(stmt).all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row[0]), '%Y-%m-%d'), row[2], int(row[1])))
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
        stmt = select(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                      func.sum(SysTraffic.size).label("size"),
                      SysTraffic.op_type).where(SysTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00')),
                          SysTraffic.op_type == op_type).group_by(
                          func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
                          SysTraffic.op_type).order_by("timestamp")
    elif op_type == 'all':
        stmt = select(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
                      func.sum(SysTraffic.size).label("size"),
                      SysTraffic.op_type).where(SysTraffic.timestamp.between(
                          func.convert_tz(start_at_0, offset, '+00:00'),
                          func.convert_tz(end_at_23, offset, '+00:00'))).group_by(
                          func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
                          SysTraffic.op_type).order_by("timestamp")
    else:
        return []

    rows = session.execute(stmt).all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row[0]), '%Y-%m-%d'), row[2], int(row[1])))
    return ret


def get_all_users_traffic_by_month(session, month, start=-1, limit=-1, order_by='user', org_id=-1):
    month_str = month.strftime('%Y-%m-01 00:00:00')
    _month = datetime.strptime(month_str, '%Y-%m-%d %H:%M:%S')

    ret = []
    try:
        stmt = select(MonthlyUserTraffic).where(
                      MonthlyUserTraffic.timestamp == _month,
                      MonthlyUserTraffic.org_id == org_id)
        if order_by == 'user':
            stmt = stmt.order_by(MonthlyUserTraffic.user)
        elif order_by == 'user_desc':
            stmt = stmt.order_by(desc(MonthlyUserTraffic.user))
        elif order_by == 'web_file_upload':
            stmt = stmt.order_by(MonthlyUserTraffic.web_file_upload)
        elif order_by == 'web_file_upload_desc':
            stmt = stmt.order_by(desc(MonthlyUserTraffic.web_file_upload))
        elif order_by == 'web_file_download':
            stmt = stmt.order_by(MonthlyUserTraffic.web_file_download)
        elif order_by == 'web_file_download_desc':
            stmt = stmt.order_by(desc(MonthlyUserTraffic.web_file_download))
        elif order_by == 'link_file_upload':
            stmt = stmt.order_by(MonthlyUserTraffic.link_file_upload)
        elif order_by == 'link_file_upload_desc':
            stmt = stmt.order_by(desc(MonthlyUserTraffic.link_file_upload))
        elif order_by == 'link_file_download':
            stmt = stmt.order_by(MonthlyUserTraffic.link_file_download)
        elif order_by == 'link_file_download_desc':
            stmt = stmt.order_by(desc(MonthlyUserTraffic.link_file_download))
        elif order_by == 'sync_file_upload':
            stmt = stmt.order_by(MonthlyUserTraffic.sync_file_upload)
        elif order_by == 'sync_file_upload_desc':
            stmt = stmt.order_by(desc(MonthlyUserTraffic.sync_file_upload))
        elif order_by == 'sync_file_download':
            stmt = stmt.order_by(MonthlyUserTraffic.sync_file_download)
        elif order_by == 'sync_file_download_desc':
            stmt = stmt.order_by(desc(MonthlyUserTraffic.sync_file_download))
        else:
            logging.warning("Failed to get all users traffic by month, unkown order_by '%s'.", order_by)
            return []

        if start>=0 and limit>0:
            stmt = stmt.slice(start, start + limit)
        rows = session.scalars(stmt).all()

        for row in rows:
            d = row.__dict__
            d.pop('_sa_instance_state')
            d.pop('id')
            ret.append(d)

    except Exception as e:
        logging.warning('Failed to get all users traffic by month: %s.', e)

    return ret

def get_all_orgs_traffic_by_month(session, month, start=-1, limit=-1, order_by='org_id'):
    month_str = month.strftime('%Y-%m-01 00:00:00')
    _month = datetime.strptime(month_str, '%Y-%m-%d %H:%M:%S')

    ret = []
    try:
        stmt = select(MonthlySysTraffic).where(MonthlySysTraffic.timestamp == _month,
                                               MonthlySysTraffic.org_id > 0)

        if order_by == 'org_id':
            stmt = stmt.order_by(MonthlySysTraffic.org_id)
        elif order_by == 'org_id_desc':
            stmt = stmt.order_by(desc(MonthlySysTraffic.org_id))
        elif order_by == 'web_file_upload':
            stmt = stmt.order_by(MonthlySysTraffic.web_file_upload)
        elif order_by == 'web_file_upload_desc':
            stmt = stmt.order_by(desc(MonthlySysTraffic.web_file_upload))
        elif order_by == 'web_file_download':
            stmt = stmt.order_by(MonthlySysTraffic.web_file_download)
        elif order_by == 'web_file_download_desc':
            stmt = stmt.order_by(desc(MonthlySysTraffic.web_file_download))
        elif order_by == 'link_file_upload':
            stmt = stmt.order_by(MonthlySysTraffic.link_file_upload)
        elif order_by == 'link_file_upload_desc':
            stmt = stmt.order_by(desc(MonthlySysTraffic.link_file_upload))
        elif order_by == 'link_file_download':
            stmt = stmt.order_by(MonthlySysTraffic.link_file_download)
        elif order_by == 'link_file_download_desc':
            stmt = stmt.order_by(desc(MonthlySysTraffic.link_file_download))
        elif order_by == 'sync_file_upload':
            stmt = stmt.order_by(MonthlySysTraffic.sync_file_upload)
        elif order_by == 'sync_file_upload_desc':
            stmt = stmt.order_by(desc(MonthlySysTraffic.sync_file_upload))
        elif order_by == 'sync_file_download':
            stmt = stmt.order_by(MonthlySysTraffic.sync_file_download)
        elif order_by == 'sync_file_download_desc':
            stmt = stmt.order_by(desc(MonthlySysTraffic.sync_file_download))
        else:
            logging.warning("Failed to get all orgs traffic by month, unkown order_by '%s'.", order_by)
            return []

        if start>=0 and limit>0:
            stmt = stmt.slice(start, start + limit)
        rows = session.scalars(stmt).all()

        for row in rows:
            d = row.__dict__
            d.pop('_sa_instance_state')
            d.pop('id')
            ret.append(d)

    except Exception as e:
        logging.warning('Failed to get all users traffic by month: %s.', e)

    return ret

def get_user_traffic_by_month(session, user, month):
    month_str = month.strftime('%Y-%m-01 00:00:00')
    _month = datetime.strptime(month_str, '%Y-%m-%d %H:%M:%S')

    ret = {}
    try:
        stmt = select(MonthlyUserTraffic).where(MonthlyUserTraffic.timestamp == _month,
                                                MonthlyUserTraffic.user == user).limit(1)
        result = session.scalars(stmt).first()
        if result:
            d = result.__dict__
            d.pop('_sa_instance_state')
            d.pop('id')
            ret = d
    except Exception as e:
        logging.warning('Failed to get user traffic by month: %s.', e)

    return ret


def get_org_traffic_by_month(session, org_id, month):
    month_str = month.strftime('%Y-%m-01 00:00:00')
    _month = datetime.strptime(month_str, '%Y-%m-%d %H:%M:%S')

    ret = {}
    try:
        stmt = select(MonthlySysTraffic).where(MonthlySysTraffic.timestamp == _month,
                                               MonthlySysTraffic.org_id == org_id).limit(1)
        result = session.scalars(stmt).first()
        if result:
            d = result.__dict__
            d.pop('_sa_instance_state')
            d.pop('id')
            ret = d
    except Exception as e:
        logging.warning('Failed to get org traffic by month: %s.', e)

    return ret
