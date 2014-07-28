import os
import ConfigParser
import datetime
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import desc

from .models import Base, UserTrafficStat

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
