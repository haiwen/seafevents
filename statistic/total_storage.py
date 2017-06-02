import os
import logging
from ConfigParser import ConfigParser
from datetime import timedelta
from datetime import datetime
from sqlalchemy.orm.scoping import scoped_session
from models import TotalStorageStat

class TotalStorageCounter(object):
    def __init__(self, settings):
        self.settings = settings
        self.edb_session = scoped_session(settings.session_cls)
        settings.init_seafile_db()
        self.cursor = settings.seafile_cursor

    def start_count(self):
        try:
            self.cursor.execute('''SELECT SUM(size) FROM RepoSize s
                                   LEFT JOIN VirtualRepo v
                                   ON s.repo_id=v.repo_id
                                   WHERE v.repo_id IS NULL''')
            size = self.cursor.fetchone()[0]
        except Exception as e:
            logging.warning('Failed to get total storage occupation')

        dt = datetime.utcnow()
        _timestamp = dt.strftime('%Y-%m-%d %H:00:00')
        timestamp = datetime.strptime(_timestamp,'%Y-%m-%d %H:%M:%S')

        try:
            q = self.edb_session.query(TotalStorageStat).filter(TotalStorageStat.timestamp==timestamp)
        except Exception as e:
            logging.warning('query error : %s.', e)
        
        try:
            r = q.first()
            if not r:
                newrecord = TotalStorageStat(timestamp, size)
                self.edb_session.add(newrecord)
                self.edb_session.commit()
                self.edb_session.remove()
            else:
                self.edb_session.remove()
        except Exception as e:
            logging.warning('Failed to add record to TotalStorageStat: %s.', e)

