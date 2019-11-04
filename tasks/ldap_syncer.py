#coding: utf-8

import logging
from threading import Thread, Event
from seafevents.ldap_syncer import Settings
import time

class LdapSyncer(object):
    def __init__(self):
        self.settings = Settings()

    def enable_sync(self):
        return self.settings.enable_sync()

    def start(self):
        logging.info("Starting ldap sync.")
        LdapSyncTimer(self.settings).start()

class LdapSyncTimer(Thread):
    def __init__(self, settings):
        Thread.__init__(self)
        self.settings = settings
        self.sync_user_finish = Event()
        self.sync_group_finish = Event()

    def run(self):
        from seafevents.ldap_syncer.run_ldap_sync import run_ldap_sync
        self.sync_group_finish.set()
        self.sync_user_finish.set()
        while True:
            time.sleep (self.settings.sync_interval*60)
            if self.sync_group_finish.is_set() and self.sync_user_finish.is_set():
                self.sync_group_finish.clear()
                self.sync_user_finish.clear()
                run_ldap_sync(self.settings, self.sync_group_finish, self.sync_user_finish)
