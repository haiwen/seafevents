import logging
from sqlalchemy import desc, func, distinct, select, text
from datetime import datetime

from .models import UserActivityStat, UserTraffic, SysTraffic, \
                   FileOpsStat, TotalStorageStat, MonthlyUserTraffic, MonthlySysTraffic

from seaserv import seafile_api, get_org_id_by_repo_id

repo_org = {}
is_org = -1

import pytz
from seafevents.app.config import TIME_ZONE

def convert_timezone(dt, from_tz, to_tz):
    if not isinstance(dt, datetime):
        raise TypeError('Expected a datetime object')

    if from_tz is None:
        from_tz = pytz.timezone('UTC')
    if to_tz is None:
        to_tz = pytz.timezone('UTC')

    aware_datetime = from_tz.normalize(dt.astimezone(pytz.UTC))
    return aware_datetime.astimezone(to_tz)


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

    # stmt = select(func.date(func.convert_tz(UserActivityStat.timestamp, '+00:00', offset)).label("timestamp"),
    #               func.count(distinct(UserActivityStat.username)).label("number")).where(
    #               UserActivityStat.timestamp.between(
    #                   func.convert_tz(start_at_0, offset, '+00:00'),
    #                   func.convert_tz(end_at_23, offset, '+00:00'))).group_by(
    #                   func.date(func.convert_tz(UserActivityStat.timestamp, '+00:00', offset))).order_by("timestamp")

    stmt = select(func.TO_DATE(UserActivityStat.timestamp).label("timestamp"),
                  func.count(distinct(UserActivityStat.username)).label("number")).where(
        UserActivityStat.timestamp.between(start_at_0, end_at_23)).group_by(
        func.TO_DATE(UserActivityStat.timestamp)).order_by("timestamp")

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

    off_hour = int(offset[0:3])
    try:
        # stmt = select(func.convert_tz(TotalStorageStat.timestamp, '+00:00', offset).label("timestamp"),
        #               func.sum(TotalStorageStat.total_size).label("total_size"))
        # if org_id == 0:
        #     stmt = stmt.where(TotalStorageStat.timestamp.between(
        #                  func.convert_tz(start, offset, '+00:00'),
        #                  func.convert_tz(end, offset, '+00:00')))
        # else:
        #     stmt = stmt.where(TotalStorageStat.timestamp.between(
        #                  func.convert_tz(start, offset, '+00:00'),
        #                  func.convert_tz(end, offset, '+00:00')),
        #                  TotalStorageStat.org_id == org_id)
        # stmt = stmt.group_by("timestamp").order_by("timestamp")

        sql = f"""SELECT DATEADD(HH, {off_hour}, timestamp) as timestamp, sum(total_size) as total_size FROM TotalStorageStat 
        WHERE timestamp between DATEADD(HH, {-off_hour}, '{start}') AND DATEADD(HH, {-off_hour}, '{end}')
        """

        if org_id != 0:
            sql += f" AND org_id={org_id}"

        sql += "GROUP BY timestamp ORDER BY timestamp"
        rows = session.execute(text(sql)).all()

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

    # # 不支持 func.convert_tz 这种语法，需要调整
    # stmt = select(func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)).label("timestamp"),
    #               func.sum(FileOpsStat.number).label("number"),
    #               FileOpsStat.op_type).where(FileOpsStat.timestamp.between(
    #                   func.convert_tz(start_at_0, offset, '+00:00'),
    #                   func.convert_tz(end_at_23, offset, '+00:00'))).group_by(
    #                   func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)),
    #                   FileOpsStat.op_type).order_by("timestamp")
    #

    # 按下面的方式改就不需要在Python中进行时区转化了
    # from_tz = pytz.timezone(TIME_ZONE)
    # end_at_23 = convert_timezone(end_at_23, from_tz=from_tz, to_tz=pytz.UTC)
    # start_at_0 = convert_timezone(start_at_0, from_tz=from_tz, to_tz=pytz.UTC)
    # off_hour = int(offset[0:3])
    # sql = f"""SELECT TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS') as timestamp, sum("number") as "number", op_type
    # FROM FileOpsStat WHERE timestamp between '{start_at_0}' AND '{end_at_23}'
    # GROUP BY YEAR(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')),
    # MONTH(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')),
    # DAY(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), op_type """

    off_hour = int(offset[0:3])
    sql = f"""SELECT TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS') as timestamp, sum("number") as "number", op_type 
        FROM FileOpsStat WHERE timestamp between DATEADD(HH, {-off_hour}, '{start_at_0}') AND DATEADD(HH, {-off_hour}, '{end_at_23}')
        GROUP BY YEAR(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')),
        MONTH(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), 
        DAY(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), op_type """

    rows = session.execute(text(sql)).fetchall()
    ret = []
    for row in rows:
        ret.append((datetime.strptime(str(datetime.date(row[0])), '%Y-%m-%d'), row[2], int(row[1])))

    return ret

# def get_file_ops_stats_by_day(session, start, end, offset='+00:00'):
#     start_str = start.strftime('%Y-%m-%d 00:00:00')
#     end_str = end.strftime('%Y-%m-%d 23:59:59')
#     start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
#     end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
#
#     from_tz = pytz.timezone(TIME_ZONE)
#     end_at_23 = convert_timezone(end_at_23, from_tz=from_tz, to_tz=pytz.UTC)
#     start_at_0 = convert_timezone(start_at_0, from_tz=from_tz, to_tz=pytz.UTC)
#
#     # # 不支持 func.convert_tz 这种语法，需要调整
#     # stmt = select(func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)).label("timestamp"),
#     #               func.sum(FileOpsStat.number).label("number"),
#     #               FileOpsStat.op_type).where(FileOpsStat.timestamp.between(
#     #                   func.convert_tz(start_at_0, offset, '+00:00'),
#     #                   func.convert_tz(end_at_23, offset, '+00:00'))).group_by(
#     #                   func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)),
#     #                   FileOpsStat.op_type).order_by("timestamp")
#
#     # 它这里分组的时候实际上不需要转换时区，转换和不转换都是一样的结果，所以直接按照timestamp分组就可以了
#     # 数据库中的数据是这样的 2025-01-15 06:00:00.000000，分组的话不会受时区的影响
#     print('start_at_0')
#     print(start_at_0)
#     print(end_at_23)
#     # SELECT DATEADD(HH, 4, '2022-09-19 16:09:35');
#     sql = f"""SELECT timestamp, sum("number") as "number", op_type FROM FileOpsStat WHERE timestamp between '{start_at_0}' AND '{end_at_23}' GROUP BY DAY(DATEADD(HH, 8, timestamp)), op_type """
#     # sql = f"""SELECT timestamp, "number", op_type FROM FileOpsStat WHERE timestamp between '{start_at_0}' AND '{end_at_23}'"""
#     # sql = """SELECT FROM_TZ(timestamp, '+09:00'), sum("number") as "number", op_type FROM FileOpsStat"""
#     # sql = """SELECT timestamp, sum("number") as "number", op_type FROM FileOpsStat WHERE timestamp between '2025-01-13' AND '2025-01-16';"""
#     # sql = """SELECT timestamp, sum("number") as "number", op_type FROM FileOpsStat WHERE timestamp between '2025-01-13' AND '2025-01-16 06:00:00.000000';"""
#     print('sql')
#     print(sql)
#     print(text(sql).compile(compile_kwargs={"literal_binds": True}))
#     rows = session.execute(text(sql)).fetchall()
#     ret = []
#     # SELECT timestamp, "number", op_type FROM FileOpsStat WHERE timestamp between '2025-01-13 16:00:00+00:00' AND '2025-01-14 23:59:59+00:00';
#
#     # 下午需要看一下，这个的group后的数据不是安装 timestamp 分组的吗，如果是，那么seahub中为什么只取了最后一个呢？
#     print('len(rows)')
#     print(len(rows))
#     for row in rows:
#         # 数据库中的数据是这样的 2025-01-15 06:00:00.000000， 所以返回的时间有可能是两个不同的时间
#         print('row')
#         print(row)
#         timestamp = row[0]
#         number = row[1]
#         op_type = row[2]
#         print(str(timestamp))
#         print(timestamp.tzinfo)
#
#         # from_tz = pytz.timezone('UTC')
#         # aware_datetime = from_tz.normalize(timestamp.astimezone(pytz.UTC))
#         # print('aware_datetime')
#         # print(aware_datetime)
#
#         # 2025-01-15 06:00:00
#
#         # start_at_0 = datetime.strptime(timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
#         timestamp = convert_timezone(timestamp, from_tz=pytz.UTC, to_tz=from_tz)
#         print('timestamp')
#         print(timestamp)
#         tz_offset = timestamp.utcoffset()
#
#         print(timestamp.utcoffset())
#         print(timestamp.utcoffset() + timestamp)
#         print(timestamp.strftime('%Y-%m-%d %H:%M:%S'))
#
#         # 2025-01-15 06:00:00+08:00
#
#
#         t = str(row[0])[:10]
#         # ret.append((datetime.strptime(str(row[0]), '%Y-%m-%d'), row[2], int(row[1])))
#         # 第一个数据只是为了统计，返回值也只是精确到天，所以没必要转化 2025-01-15T00:00:00+08:00
#         ret.append((datetime.strptime(t, '%Y-%m-%d'), row[2], int(row[1])))
#
#     print('ret')
#     print(ret)
#     return ret

def get_org_file_ops_stats_by_day(session, org_id, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
    ret = []

    try:
        # stmt = select(func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)).label("timestamp"),
        #               func.sum(FileOpsStat.number).label("number"), FileOpsStat.op_type).where(
        #               FileOpsStat.timestamp.between(
        #                   func.convert_tz(start_at_0, offset, '+00:00'),
        #                   func.convert_tz(end_at_23, offset, '+00:00')),
        #               FileOpsStat.org_id == org_id).group_by(
        #                   func.date(func.convert_tz(FileOpsStat.timestamp, '+00:00', offset)),
        #                   FileOpsStat.op_type).order_by("timestamp")
        #
        # rows = session.execute(stmt).all()

        off_hour = int(offset[0:3])
        sql = f"""SELECT TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS') as timestamp, sum("number") as "number", op_type 
                FROM FileOpsStat WHERE timestamp between DATEADD(HH, {-off_hour}, '{start_at_0}') AND DATEADD(HH, {-off_hour}, '{end_at_23}') AND org_id={org_id}
                GROUP BY YEAR(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')),
                MONTH(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), 
                DAY(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), op_type 
                ORDER BY timestamp"""

        rows = session.execute(text(sql)).fetchall()

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
    off_hour = int(offset[0:3])

    if op_type == 'web-file-upload' or op_type == 'web-file-download' or op_type == 'sync-file-download' \
       or op_type == 'sync-file-upload' or op_type == 'link-file-upload' or op_type == 'link-file-download':
        # stmt = select(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
        #               func.sum(SysTraffic.size).label("size"),
        #               SysTraffic.op_type).where(SysTraffic.timestamp.between(
        #                   func.convert_tz(start_at_0, offset, '+00:00'),
        #                   func.convert_tz(end_at_23, offset, '+00:00')),
        #                   SysTraffic.org_id == org_id,
        #                   SysTraffic.op_type == op_type).group_by(
        #                   SysTraffic.org_id,
        #                   func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
        #                   SysTraffic.op_type).order_by("timestamp")

        sql = f"""SELECT TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS') as timestamp, sum("size") as "size", op_type 
                        FROM SysTraffic WHERE timestamp between DATEADD(HH, {-off_hour}, '{start_at_0}') AND DATEADD(HH, {-off_hour}, '{end_at_23}') 
                        AND org_id={org_id} AND op_type={op_type}
                        GROUP BY org_id, YEAR(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')),
                        MONTH(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), 
                        DAY(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), op_type 
                        ORDER BY timestamp """

    elif op_type == 'all':
        # stmt = select(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
        #               func.sum(SysTraffic.size).label("size"),
        #               SysTraffic.op_type).where(SysTraffic.timestamp.between(
        #                   func.convert_tz(start_at_0, offset, '+00:00'),
        #                   func.convert_tz(end_at_23, offset, '+00:00')),
        #                   SysTraffic.org_id == org_id).group_by(
        #                   SysTraffic.org_id,
        #                   func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
        #                   SysTraffic.op_type).order_by("timestamp")

        sql = f"""SELECT TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS') as timestamp, sum("size") as "size", op_type 
                                FROM SysTraffic WHERE timestamp between DATEADD(HH, {-off_hour}, '{start_at_0}') AND DATEADD(HH, {-off_hour}, '{end_at_23}') 
                                AND org_id={org_id}
                                GROUP BY org_id, YEAR(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')),
                                MONTH(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), 
                                DAY(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), op_type 
                                ORDER BY timestamp """
    else:
        return []

    # rows = session.execute(stmt).all()
    rows = session.execute(text(sql)).fetchall()
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
    off_hour = int(offset[0:3])

    if op_type == 'web-file-upload' or op_type == 'web-file-download' or op_type == 'sync-file-download' \
       or op_type == 'sync-file-upload' or op_type == 'link-file-upload' or op_type == 'link-file-download':
        # stmt = select(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
        #               func.sum(SysTraffic.size).label("size"),
        #               SysTraffic.op_type).where(SysTraffic.timestamp.between(
        #                   func.convert_tz(start_at_0, offset, '+00:00'),
        #                   func.convert_tz(end_at_23, offset, '+00:00')),
        #                   SysTraffic.op_type == op_type).group_by(
        #                   func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
        #                   SysTraffic.op_type).order_by("timestamp")
        sql = f"""SELECT TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS') as timestamp, sum("size") as "size", op_type 
                                FROM SysTraffic WHERE timestamp between DATEADD(HH, {-off_hour}, '{start_at_0}') AND DATEADD(HH, {-off_hour}, '{end_at_23}') 
                                AND op_type={op_type}
                                GROUP BY YEAR(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')),
                                MONTH(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), 
                                DAY(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), op_type 
                                ORDER BY timestamp """
    elif op_type == 'all':
        # stmt = select(func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)).label("timestamp"),
        #               func.sum(SysTraffic.size).label("size"),
        #               SysTraffic.op_type).where(SysTraffic.timestamp.between(
        #                   func.convert_tz(start_at_0, offset, '+00:00'),
        #                   func.convert_tz(end_at_23, offset, '+00:00'))).group_by(
        #                   func.date(func.convert_tz(SysTraffic.timestamp, '+00:00', offset)),
        #                   SysTraffic.op_type).order_by("timestamp")

        sql = f"""SELECT TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS') as timestamp, sum("size") as "size", op_type 
                                        FROM SysTraffic WHERE timestamp between DATEADD(HH, {-off_hour}, '{start_at_0}') AND DATEADD(HH, {-off_hour}, '{end_at_23}') 
                                        GROUP BY org_id, YEAR(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')),
                                        MONTH(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), 
                                        DAY(TO_DATE(DATEADD(HH, {off_hour}, timestamp), 'YYYY-MM-DD HH24:MI:SS')), op_type 
                                        ORDER BY timestamp """
    else:
        return []

    # rows = session.execute(stmt).all()
    rows = session.execute(text(sql)).fetchall()
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
