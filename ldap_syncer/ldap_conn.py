#coding: utf-8

import logging
import ldap

class LdapConn(object):
    def __init__(self, host, user_dn, passwd):
        self.host = host
        self.user_dn = user_dn
        self.passwd = passwd
        self.conn = None

    def create_conn(self):
        self.conn = ldap.initialize(self.host)
        try:
            self.conn.simple_bind_s(self.user_dn, self.passwd)
        except ldap.INVALID_CREDENTIALS:
            logging.warning('Invalid user or password for connect ldap')
        except ldap.LDAPError as e:
            logging.warning('Connect ldap failed, error: %s' % e.message)

    def search(self, base_dn, scope, search_filter, attr_list):
        result = None
        if not self.conn:
            return result

        try:
            result = self.conn.search_s(base_dn, scope, search_filter, attr_list)
        except ldap.LDAPError as e:
            if type(e.message) == dict and e.message['desc'] == 'No such object':
                pass
            else:
                logging.warning('search failed error: %s' % e.message)

        return result

    def unbind_conn(self):
        if self.conn:
            self.conn.unbind()
