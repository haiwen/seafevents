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
        email_to_uid = self.get_uid_from_profile()

        data_ldap, uid_to_ldap_user = self.get_data_from_ldap()
        if data_ldap is None:
            return

        data_db, uid_to_users  = self.get_data_from_db(email_to_uid)
        if data_db is None:
            return

        self.sync_data(data_db, email_to_uid, data_ldap, uid_to_ldap_user)

    def get_uid_from_profile(self):
        return None

    def get_data_from_db(self, email_to_uid):
        return None

    def get_data_from_ldap(self):
        ret = {}
        uid_to_ldap_user={}

        for config in self.settings.ldap_configs:
            cur_ret = self.get_data_from_ldap_by_server(config, uid_to_ldap_user)
            # If get data from one server failed, then the result is failed
            if cur_ret is None:
                return None
            for key in cur_ret.iterkeys():
                if not ret.has_key(key):
                    ret[key] = cur_ret[key]
                    ret[key].config = config

        return ret, uid_to_ldap_user

    def get_data_from_ldap_by_server(self, config, uid_to_ldap_user):
        return None

    def sync_data(self, data_db, data_ldap):
        pass
