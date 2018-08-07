import os
import logging
import hashlib
from ConfigParser import ConfigParser
from datetime import timedelta
from datetime import datetime
from sqlalchemy import func
from models import FileOpsStat, TotalStorageStat, UserTraffic
from seafevents.events.models import FileUpdate
from seafevents.events.models import FileAudit
from seafevents.app.config import appconfig
from seafevents.db import SeafBase
from db import get_org_id

login_records = {}
traffic_info = {}

def update_hash_record(session, login_name, login_time):
    time_str = login_time.strftime('%Y-%m-%d 01:01:01')
    time_by_day = datetime.strptime(time_str,'%Y-%m-%d %H:%M:%S')
    md5_key = hashlib.md5((login_name + time_str).encode('utf-8')).hexdigest()
    login_records[md5_key] = (login_name, time_by_day)

def save_traffic_info(session, timestamp, user_name, repo_id, oper, size):
    if not appconfig.enable_statistics:
        return
    org_id = get_org_id(repo_id)
    time_str = timestamp.strftime('%Y-%m-%d %H:00:00')
    if not traffic_info.has_key(time_str):
        traffic_info[time_str] = {}
    if not traffic_info[time_str].has_key((org_id, user_name, oper)):
        traffic_info[time_str][(org_id, user_name, oper)] = size
    else:
        traffic_info[time_str][(org_id, user_name, oper)] += size

class FileOpsCounter(object):
    def __init__(self, session):
        self.edb_session = session

    def start_count(self):
        added = 0
        deleted = 0
        visited = 0

        dt = datetime.utcnow()
        delta = timedelta(hours=1)
        _start = (dt - delta)

        start = _start.strftime('%Y-%m-%d %H:00:00')
        end = _start.strftime('%Y-%m-%d %H:59:59')

        s_timestamp = datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
        e_timestamp = datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
        try:
            q = self.edb_session.query(FileOpsStat.timestamp).filter(
                                       FileOpsStat.timestamp==s_timestamp)
            if q.first():
                self.edb_session.rollback()
                return

            q = self.edb_session.query(FileUpdate.timestamp, FileUpdate.file_oper).filter(
                                       FileUpdate.timestamp.between(
                                       s_timestamp, e_timestamp))
        except Exception as e:
            self.edb_session.rollback()
            logging.warning('query error : %s.', e)
            return

        rows = q.all()
        for row in rows:
            if 'Added' in row.file_oper:
                added += 1
            elif 'Deleted' in row.file_oper or 'Removed' in row.file_oper:
                deleted += 1
        try:
            q = self.edb_session.query(func.count(FileAudit.eid)).filter(
                                       FileAudit.timestamp.between(
                                       s_timestamp, e_timestamp))
        except Exception as e:
            self.edb_session.rollback()
            logging.warning('query error : %s.', e)
            return
    
        visited = q.first()[0]

        if added==0 and deleted==0 and visited ==0:
            self.edb_session.rollback()
            return

        if added:
            new_record = FileOpsStat(s_timestamp, 'Added', added)
            self.edb_session.add(new_record)
        if deleted:
            new_record = FileOpsStat(s_timestamp, 'Deleted', deleted)
            self.edb_session.add(new_record)
        if visited:
            new_record = FileOpsStat(s_timestamp, 'Visited', visited)
            self.edb_session.add(new_record)

        self.edb_session.commit()

class TotalStorageCounter(object):
    def __init__(self, session, seaf_session):
        self.edb_session = session
        self.seafdb_session = seaf_session

    def start_count(self):
        try:
            RepoSize = SeafBase.classes.RepoSize
            VirtualRepo= SeafBase.classes.VirtualRepo

            q = self.seafdb_session.query(func.sum(RepoSize.size).label("size")).outerjoin(VirtualRepo,\
                                          RepoSize.repo_id==VirtualRepo.repo_id).filter(VirtualRepo.repo_id == None)
            size = q.first()[0]
        except Exception as e:
            self.seafdb_session.rollback()
            logging.warning('Failed to get total storage occupation')
            return

        dt = datetime.utcnow()
        _timestamp = dt.strftime('%Y-%m-%d %H:00:00')
        timestamp = datetime.strptime(_timestamp,'%Y-%m-%d %H:%M:%S')

        try:
            q = self.edb_session.query(TotalStorageStat).filter(TotalStorageStat.timestamp==timestamp)
        except Exception as e:
            self.seafdb_session.rollback()
            self.edb_session.rollback()
            logging.warning('query error : %s.', e)

        try:
            r = q.first()
            if not r:
                newrecord = TotalStorageStat(timestamp, size)
                self.edb_session.add(newrecord)
                self.edb_session.commit()
        except Exception as e:
            logging.warning('Failed to add record to TotalStorageStat: %s.', e)
        self.seafdb_session.rollback()
        self.edb_session.rollback()

class TrafficInfoCounter(object):
    def __init__(self, session):
        self.edb_session = session

    def start_count(self):
        dt = datetime.utcnow()
        delta = timedelta(hours=1)
        last_hour = (dt - delta)
        last_hour_str = last_hour.strftime('%Y-%m-%d %H:00:00')
        #last_hour_str = dt.strftime('%Y-%m-%d %H:00:00')
        last_hour = datetime.strptime(last_hour_str, '%Y-%m-%d %H:%M:%S')

        if not traffic_info.has_key(last_hour_str):
            return

        for row in traffic_info[last_hour_str]:
            org_id = row[0]
            user = row[1]
            oper = row[2]
            size = traffic_info[last_hour_str][row]

            try:
                q = self.edb_session.query(UserTraffic.timestamp).filter(
                                           UserTraffic.timestamp==last_hour,
                                           UserTraffic.org_id==org_id,
                                           UserTraffic.user==user,
                                           UserTraffic.op_type==oper)
                if q.first():
                    continue
            except Exception as e:
                self.edb_session.rollback()
                logging.warning('query error : %s.', e)
                return

            new_record = UserTraffic(user, last_hour, oper, size, org_id)
            self.edb_session.add(new_record)

        del traffic_info[last_hour_str]
        self.edb_session.commit()
