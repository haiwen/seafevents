#coding: utf-8

import logging
import sched, time

from sqlalchemy.sql import text
from sqlalchemy.orm.scoping import scoped_session
from threading import Thread, Event
from seafevents.statistics import Settings, TotalStorageCounter, FileOpsCounter
from seafevents.statistics.db import login_records
from seafevents.app.config import appconfig


class Statistics(object):
    def __init__(self, config_file):
        self.settings = Settings(config_file)

    def is_enabled(self):
        return self.settings.statistics_enabled

    def start(self):
        logging.info("Starting data statistics.")
        if self.settings.statistics_enabled:
            CountTotalStorage(self.settings).start()
            CountFileOps(self.settings).start()

class CountTotalStorage(Thread):
    def __init__(self, settings):
        Thread.__init__(self)
        self.settings = settings
        self.fininsh = Event()

    def run(self):
        while not self.fininsh.is_set():
            if not self.fininsh.is_set():
                TotalStorageCounter(self.settings).start_count()
            self.fininsh.wait(3600)

    def cancel(self):
        self.fininsh.set()

class CountFileOps(Thread):
    def __init__(self, settings):
        Thread.__init__(self)
        self.settings = settings
        self.fininsh = Event()

    def run(self):
        while not self.fininsh.is_set():
            if not self.fininsh.is_set():
                FileOpsCounter(self.settings).start_count()
            self.fininsh.wait(3600)

    def cancel(self):
        self.fininsh.set()


class UpdateLoginRecordTask(Thread):
    """ Run every thirty minutes, Handle 1000 tasks at a time. 
    """
    def __init__(self):
        self.session = appconfig.event_session
        super(UpdateLoginRecordTask, self).__init__()
        # time.time is timefunc, as the scheduling standard for the scheduler.
        # time.sleep is delayfunc, used to delay time until time up
        self.s = sched.scheduler(time.time, time.sleep)

    def _scoped_session(self):
        try:
            return scoped_session(self.session)
        except Exception as e:
            logging.error(e)
            return scoped_session(self.session)


    def _update_login_record(self):
        """ example:
                cmd: 'REPLACE INTO UserActivityStat values (:key1, :name1, :tim1), (:key2, :name2, :time2)'
                data: {key1: xxx, name1: xxx, time1: xxx, key2: xxx, name2: xxx, time2: xxx}
        """
        l = len(self.keys)
        if l > 0:
            try:
                session = self._scoped_session()
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
                try:
                    session.execute(text(cmd), data)
                    session.commit()
                except Exception as e:
                    logging.error(e)
                    session.execute(text(cmd), data)
                    session.commit()

            except Exception as e:
                logging.error(e)
            else:
                logging.info('%s records has beend updated' % l)
            finally:
                session.remove()

    def update_login_record(self):
        logging.info("start to update user login record")
        while True:
            all_keys = login_records.keys()
            if len(all_keys) > 300:
                self.keys = all_keys[:300]
                self._update_login_record()
            else:
                self.keys = all_keys
                self._update_login_record()
                break
        logging.info("total %s items has been updated")
        # add new event to queue before finish this event.
        self.s.enter(20 * 60, 0, self.update_login_record, ())

    def run(self):
        logging.info("Starting user login statistics.")
        # Add an event with a priority of 0 and a delay of 20 * 60 seconds.
        self.s.enter(20 * 60, 0, self.update_login_record, ())
        # run until there is no event in the queue.
        self.s.run()
