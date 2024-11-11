# coding: utf-8

import logging
from threading import Thread, Event
from seafevents.ldap_syncer import Settings


class LdapSyncer(object):
    def __init__(self, config):
        self.settings = Settings(config)

    def enable_sync(self):
        return self.settings.enable_sync()

    def start(self):
        if not self.enable_sync():
            logging.warning('Can not start ldap syncer: it is not enabled!')
            return
        logging.info("Start ldap syncer..")
        LdapSyncTimer(self.settings).start()


class LdapSyncTimer(Thread):
    def __init__(self, settings):
        Thread.__init__(self)
        self.settings = settings
        self.finished = Event()

    def run(self):
        from seafevents.ldap_syncer.run_ldap_sync import run_ldap_sync
        while not self.finished.is_set():
            self.finished.wait(self.settings.sync_interval*60)
            if not self.finished.is_set():
                run_ldap_sync(self.settings)

    def cancel(self):
        self.finished.set()
