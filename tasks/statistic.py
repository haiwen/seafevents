#coding: utf-8

import logging
from threading import Thread, Event
from seafevents.statistic import Settings
from seafevents.statistic import TotalStorageCounter, FileOpsCounter

class DataCounter(object):
    def __init__(self, config_file):
        self.settings = Settings(config_file)

    def is_enabled(self):
        return self.settings.enable_count

    def start(self):
        if self.settings.enable_storage_count:
            CountTotalStorage(self.settings).start()
        if self.settings.enable_audit_count:
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
