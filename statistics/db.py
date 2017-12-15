import hashlib
import logging

from sqlalchemy import desc
from sqlalchemy import func
from sqlalchemy import distinct
from datetime import datetime

from seafevents.statistics import FileOpsStat, TotalStorageStat
from seafevents.statistics.models import UserTrafficStat, UserActivityStat

logger = logging.getLogger(__name__)
login_records = {}

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

    month = datetime.now().strftime('%Y%m')

    q = session.query(UserTrafficStat).filter_by(email=email, month=month)
    n = q.update({ type: type + size })
    if n != 1:
        stat = UserTrafficStat(email, month, **{name:size})
        session.add(stat)

    session.commit()

def update_hash_record(session, login_name, login_time):
    time_str = login_time.strftime('%Y-%m-%d 01:01:01')
    time_by_day = datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S')
    md5_key = hashlib.md5((login_name + time_str).encode('utf-8')).hexdigest()
    login_records[md5_key] = (login_name, time_by_day)

def get_user_traffic_stat(session, email, month=None):
    '''Return the total traffic of a user in the given month. If month is not
    supplied, defaults to the current month

    '''
    if month == None:
        month = datetime.now().strftime('%Y%m')

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

def get_user_activity_stats_by_day(session, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str,'%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str,'%Y-%m-%d %H:%M:%S')

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
