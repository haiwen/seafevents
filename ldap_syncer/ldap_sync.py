#coding: utf-8

import logging
from threading import Thread

from ldap_conn import LdapConn

class LdapSync(Thread):
    def __init__(self, settings):
        Thread.__init__(self)
        self.settings = settings

    def run(self):
        self.start_sync()
        self.show_sync_result()

    def show_sync_result(self):
        pass

    def start_sync(self):
        data_ldap = self.get_data_from_ldap()
        if data_ldap is None:
            return

        data_db = self.get_data_from_db()
        if data_db is None:
            return

        self.sync_data(data_db, data_ldap)

    def get_data_from_db(self):
        return None

    def get_data_from_ldap(self):
        ret = {}

        for config in self.settings.ldap_configs:
            cur_ret = self.get_data_from_ldap_by_server(config)
            # If get data from one server failed, then the result is failed
            if cur_ret is None:
                return None
            for key in cur_ret.iterkeys():
                if not ret.has_key(key):
                    ret[key] = cur_ret[key]

        return ret

    def get_data_from_ldap_by_server(self, config):
        return None

    def sync_data(self, data_db, data_ldap):
        pass
