import os
import logging
from ConfigParser import ConfigParser
from datetime import timedelta
from datetime import datetime
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy import func
from models import FileOpsStat
from seafevents.events.models import FileUpdate
from seafevents.events.models import FileAudit

class FileOpsCounter(object):
    def __init__(self, settings):
        self.settings = settings
        self.edb_session = scoped_session(settings.session_cls)

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
                return
        except Exception as e:
            logging.warning('query error : %s.', e)

        try:
            q = self.edb_session.query(FileUpdate.timestamp, FileUpdate.file_oper).filter(
                                       FileUpdate.timestamp.between(
                                       s_timestamp, e_timestamp))
        except Exception as e:
            logging.warning('query error : %s.', e)
        
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
            ogging.warning('query error : %s.', e)

        visited = q.first()[0]
        
        if added==0 and deleted==0 and visited ==0:
            self.edb_session.remove()
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
        self.edb_session.remove()

