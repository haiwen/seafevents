# coding: utf-8
import logging
from threading import Thread, Event

from seafevents.statistics import TotalStorageCounter, FileOpsCounter, MonthlyTrafficCounter


def exception_catch(module):
    def func_wrapper(func):
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                logging.info('[Statistics] %s task is failed: %s' % (module, e))
        return wrapper
    return func_wrapper


class Statistics(Thread):
    def __init__(self, config, seafile_config):
        Thread.__init__(self)
        self.config = config
        self.seafile_config = seafile_config

    def is_enabled(self):
        enabled = False
        if self.config.has_option('STATISTICS', 'enabled'):
            enabled = self.config.getboolean('STATISTICS', 'enabled')
        return enabled

    def run(self):
        # These tasks should run at backend node server.
        if self.is_enabled():
            logging.info("Start data statistics..")
            CountTotalStorage().start()
            CountFileOps().start()
            CountMonthlyTrafficInfo().start()
        else:
            logging.info('Can not start data statistics: it is not enabled!')
            return


class CountTotalStorage(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.finished = Event()

    @exception_catch('CountTotalStorage')
    def run(self):
        while not self.finished.is_set():
            TotalStorageCounter().start_count()
            self.finished.wait(3600)

    def cancel(self):
        self.finished.set()


class CountFileOps(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.finished = Event()

    @exception_catch('CountFileOps')
    def run(self):
        while not self.finished.is_set():
            FileOpsCounter().start_count()
            self.finished.wait(3600)

    def cancel(self):
        self.finished.set()


class CountMonthlyTrafficInfo(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.finished = Event()

    @exception_catch('CountMonthlyTrafficInfo')
    def run(self):
        while not self.finished.is_set():
            MonthlyTrafficCounter().start_count()
            self.finished.wait(3600)

    def cancel(self):
        self.finished.set()
