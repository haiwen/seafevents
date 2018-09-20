#coding: utf-8

import logging
import sched, time

from threading import Thread, Event
from seafevents.statistics import TotalStorageCounter, FileOpsCounter, TrafficInfoCounter,\
                                  MonthlyTrafficCounter, UserActivityCounter, FileTypesCounter
from seafevents.statistics.counter import login_records
from seafevents.app.config import appconfig


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
            CountFileTypes().start()

class CountTotalStorage(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.fininsh = Event()

    def run(self):
        while not self.fininsh.is_set():
            TotalStorageCounter().start_count()
            self.fininsh.wait(3600)

    def cancel(self):
        self.fininsh.set()

class CountFileOps(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.fininsh = Event()

    def run(self):
        while not self.fininsh.is_set():
            FileOpsCounter().start_count()
            self.fininsh.wait(3600)

    def cancel(self):
        self.fininsh.set()

class CountTrafficInfo(Thread):
    # This should run at frontend node server.
    def __init__(self):
        Thread.__init__(self)
        self.fininsh = Event()

    def run(self):
        while not self.fininsh.is_set():
            TrafficInfoCounter().start_count()
            self.fininsh.wait(3600)

    def cancel(self):
        self.fininsh.set()

class CountMonthlyTrafficInfo(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.fininsh = Event()

    def run(self):
        while not self.fininsh.is_set():
            MonthlyTrafficCounter().start_count()
            self.fininsh.wait(3600)
    def cancel(self):
        self.fininsh.set()

class CountFileTypes(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.fininsh = Event()

    def run(self):
        while not self.fininsh.is_set():
            FileTypesCounter().start_count()
            self.fininsh.wait(appconfig.file_types_interval)

    def cancel(self):
        self.fininsh.set()

class CountUserActivity(Thread):
    # This should run at frontend node server.
    def __init__(self):
        Thread.__init__(self)
        self.fininsh = Event()

    def run(self):
        while not self.fininsh.is_set():
            UserActivityCounter().start_count()
            self.fininsh.wait(3600)

    def cancel(self):
        self.fininsh.set()
