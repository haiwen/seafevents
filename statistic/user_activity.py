import logging
from datetime import timedelta
from datetime import datetime
from sqlalchemy.orm.scoping import scoped_session
from models import UserActivityStat

class UserActivityCounter(object):
    
    def __init__(self, settings):
        self.settings = settings
        self.edb_session = scoped_session(settings.session_cls)
        settings.init_seahub_db()
        self.cursor = settings.seahub_cursor

    def start_count(self):

        dt = datetime.utcnow()
        delta = timedelta(hours=1)
        _start = (dt - delta)
            
        start = _start.strftime('%Y-%m-%d %H:00:00')
        end = _start.strftime('%Y-%m-%d %H:59:59')
        number = 0

        try:
            self.cursor.execute('select count(1) from base_userlastlogin where last_login between %s and %s',
                                (start, end))
            number = self.cursor.fetchone()[0]
        except Exception as e:
            logging.warning('Failed to get the number of users who logged in last hour : %s.', e)

        timestamp = datetime.strptime(start,'%Y-%m-%d %H:%M:%S')

        try:
            q = self.edb_session.query(UserActivityStat).filter(UserActivityStat.timestamp==timestamp)
        except Exception as e:
            logging.warning('query error : %s.', e)
        try:
            r = q.first()
            if (not r) and (number > 0):
                newrecord = UserActivityStat(timestamp, number)
                self.edb_session.add(newrecord)
                self.edb_session.commit()
                self.edb_session.remove()
            else:
                self.edb_session.remove()
        except Exception as e:
            logging.warning('Failed to add record to UserActivityStat : %s.', e)
        
