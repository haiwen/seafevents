#coding: utf-8

import logging
from threading import Thread

from ldap_conn import LdapConn

class LdapSync(Thread):
    def __init__(self, settings):
        Thread.__init__(self)
        self.settings = settings
        self.ldap_conn = LdapConn(settings.host, settings.user_dn, settings.passwd)

    def run(self):
        self.start_sync()
        self.show_sync_result()

    def show_sync_result(self):
        pass

    def start_sync(self):
        self.ldap_conn.create_conn()
        if not self.ldap_conn.conn:
            return

        data_ldap = self.get_data_from_ldap()
        self.ldap_conn.unbind_conn()

        data_db = self.get_data_from_db()
        if data_db is None:
            return

        self.sync_data(data_db, data_ldap)

    def get_data_from_db(self):
        return None

    def get_grp_data_from_ldap(self):
        return {}

    def sync_data(self, data_db, data_ldap):
        pass
