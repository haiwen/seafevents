import logging
import sched, time
from threading import Thread
from sqlalchemy.sql import text
from sqlalchemy.orm.scoping import scoped_session

from seafevents.utils import retry
from seafevents.stats.db import login_records
from seafevents.app.config import appconfig


class TimerTasks(object):
    @retry
    def run(self):
        ULITask = UpdateLoginRecordTask()
        ULITask.start()



class UpdateLoginRecordTask(Thread):
    """ Run every ten minutes, Handle 1000 tasks at a time. 
    """
    def __init__(self):
        self.session = scoped_session(appconfig.statistic_session)
        super(UpdateLoginRecordTask, self).__init__()

    def _update_login_record(self):
        """ example:
                cmd: 'REPLACE INTO UserActivityStat values (:key1, :name1, :tim1), (:key2, :name2, :time2)'
                data: {key1: xxx, name1: xxx, time1: xxx, key2: xxx, name2: xxx, time2: xxx}
        """
        l = len(self.keys)
        if l > 0:
            try:
                cmd = "REPLACE INTO UserActivityStat values"
                cmd_extend = ''.join([' (:key' + str(i) +', :name'+ str(i) +', :time'+ str(i) +'),' for i in xrange(l)])[:-1]
                cmd += cmd_extend
                data = {}
                for key in self.keys:
                    pop_data = login_records.pop(key)
                    i = str(self.keys.index(key))
                    data['key'+i] = key
                    data['name'+i] = pop_data[0]
                    data['time'+i] = pop_data[1]
                self.session.execute(text(cmd), data)
                self.session.commit()
            except Exception as e:
                logging.error(e)
            finally:
                self.session.remove()

    def update_login_record(self):
        while True:
            all_keys = login_records.keys()
            if len(all_keys) > 1000:
                self.keys = all_keys[:1000]
                self._update_login_record()
            else:
                self.keys = all_keys
                self._update_login_record()
                break
        self.s.enter(600, 0, self.update_login_record, ())

    def run(self):
        self.s = sched.scheduler(time.time, time.sleep)

        # makesure always run at (10, 20, 30, 40, 50, 60) minutes
        minutes = time.gmtime().tm_min
        interval = 10 - minutes % 10
        self.s.enter(interval * 60, 0, self.update_login_record, ())
        self.s.run()
