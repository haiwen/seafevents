# coding: utf-8

import logging
from threading import Thread, Event
from seafevents.virus_scanner import Settings
from seafevents.virus_scanner import VirusScan


class VirusScanner(object):
    def __init__(self, config_file):
        self.settings = Settings(config_file)

    def is_enabled(self):
        return self.settings.is_enabled()

    def start(self):
        logging.info("Start virus scanner, interval = %s sec", self.settings.scan_interval*60)
        VirusScanTimer(self.settings).start()


class VirusScanTimer(Thread):
    def __init__(self, settings):
        Thread.__init__(self)
        self.settings = settings
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.settings.scan_interval*60)
            if not self.finished.is_set():
                VirusScan(self.settings).start()

    def cancel(self):
        self.finished.set()
