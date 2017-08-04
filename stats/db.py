import os
import ConfigParser
import datetime
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import desc
from sqlalchemy import func
from sqlalchemy import distinct
from datetime import datetime

from .models import Base, UserTrafficStat, UserActivityStat
from seafevents.statistic import FileOpsStat, TotalStorageStat

logger = logging.getLogger(__name__)

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

def update_user_last_login_info(session, login_name, login_time):
    time_str = login_time.strftime('%Y-%m-%d %H:00:00')
    time_by_hour = datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S')
    q = session.query(UserActivityStat).filter_by(username = login_name,
                                                timestamp = time_by_hour)
    r = q.first()
    if not r:
        stat = UserActivityStat(login_name, time_by_hour)
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

class UserTrafficDetail(object):
    def __init__(self, username, traffic):
        self.username = username
        self.traffic = traffic

def get_user_traffic_list(session, month, start, limit):
    q = session.query(UserTrafficStat).filter(UserTrafficStat.month==month)
    q = q.order_by(desc(UserTrafficStat.file_download + UserTrafficStat.dir_download + UserTrafficStat.file_view))
    q = q.slice(start, start + limit)
    rows = q.all()

    if not rows:
        return []
    else:
        ret = [ row.as_dict() for row in rows ]
        return ret

def get_user_activity_stats(session, start, end):
    q = session.query(UserActivityStat.timestamp, func.count(distinct(UserActivityStat.username)).label("number")
                     ).filter(UserActivityStat.timestamp.between(start, end)
                     ).order_by(UserActivityStat.timestamp
                     ).group_by(UserActivityStat.timestamp)

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((row.timestamp, row.number))
    return ret

def get_user_activity_stats_by_day(session, start, end):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    q = session.query(func.date(UserActivityStat.timestamp).label("timestamp"),
                      func.count(distinct(UserActivityStat.username)).label("number")).filter(
                      UserActivityStat.timestamp.between(start_at_0, end_at_23)).group_by(
                      func.date(UserActivityStat.timestamp)).order_by("timestamp")
    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.number))
    return ret

def get_total_storage_stats(session, start, end):
    q = session.query(TotalStorageStat).filter(TotalStorageStat.timestamp.between(
                                               start, end)).order_by(TotalStorageStat.timestamp)

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((row.timestamp, row.total_size))
    return ret

def get_total_storage_stats_by_day(session, start, end):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    rets = get_total_storage_stats (session, start_at_0, end_at_23)
    rets.reverse()

    last_date = None
    last_num = 0
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

def get_file_ops_stats(session, start, end):
    q = session.query(FileOpsStat).filter(FileOpsStat.timestamp.between(
                                            start, end)).order_by(FileOpsStat.timestamp)

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((row.timestamp, row.op_type, row.number))
    return ret

def get_file_ops_stats_by_day(session, start, end):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

    q = session.query(func.date(FileOpsStat.timestamp).label("timestamp"),
                      func.sum(FileOpsStat.number).label("number"),
                      FileOpsStat.op_type).filter(FileOpsStat.timestamp.between(
                      start_at_0, end_at_23)).group_by(func.date(FileOpsStat.timestamp),
                      FileOpsStat.op_type).order_by("timestamp")

    rows = q.all()
    ret = []

    for row in rows:
        ret.append((datetime.strptime(str(row.timestamp),'%Y-%m-%d'), row.op_type, long(row.number)))
    return ret

