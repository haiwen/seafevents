# coding: utf-8
import logging

from threading import Thread, Event
from seafevents.statistics import TotalStorageCounter, FileOpsCounter, TrafficInfoCounter,\
                                  MonthlyTrafficCounter, UserActivityCounter
from seafevents.app.config import appconfig


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
    def __init__(self):
        Thread.__init__(self)

    def is_enabled(self):
        return appconfig.enable_statistics

    def run(self):
        # These tasks should run at backend node server.
        if self.is_enabled():
            logging.info("Starting data statistics.")
            CountTotalStorage().start()
            CountFileOps().start()
            CountMonthlyTrafficInfo().start()


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


class CountTrafficInfo(Thread):
    # This should run at frontend node server.
    def __init__(self):
        Thread.__init__(self)
        self.finished = Event()

    @exception_catch('CountTrafficInfo')
    def run(self):
        while not self.finished.is_set():
            TrafficInfoCounter().start_count()
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


class CountUserActivity(Thread):
    # This should run at frontend node server.
    def __init__(self):
        Thread.__init__(self)
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            UserActivityCounter().start_count()
            self.finished.wait(3600)

    def cancel(self):
        self.finished.set()
