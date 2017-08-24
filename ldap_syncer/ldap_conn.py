#coding: utf-8

import logging
import ldap
from ldap.controls.libldap import SimplePagedResultsControl

class LdapConn(object):

    PAGE_SIZE = 100

    def __init__(self, host, user_dn, passwd, follow_referrals):
        self.host = host
        self.user_dn = user_dn
        self.passwd = passwd
        self.follow_referrals = follow_referrals
        self.conn = None

    def create_conn(self):
        self.conn = ldap.initialize(self.host)
        try:
            self.conn.set_option(ldap.OPT_REFERRALS, 1 if self.follow_referrals else 0)
        except ldap.LDAPError as e:
            logging.warning('Failed to set follow_referrals option, error: %s' % e.message)

        try:
            self.conn.simple_bind_s(self.user_dn, self.passwd)
        except ldap.INVALID_CREDENTIALS:
            self.conn = None
            logging.warning('Invalid credential %s:***** to connect ldap server %s' %
                            (self.user_dn, self.host))
        except ldap.LDAPError as e:
            self.conn = None
            logging.warning('Connect ldap server %s failed, error: %s' %
                            (self.host, e.message))

    def search(self, base_dn, scope, search_filter, attr_list):
        if not self.conn:
            return None

        result = None
        try:
            result = self.conn.search_s(base_dn, scope, search_filter, attr_list)
        except ldap.LDAPError as e:
            logging.warning('Search failed for base dn(%s), filter(%s) '
                            'on server %s error: %s' % (base_dn, search_filter,
                                                        self.host, e.message))

        return result

    def paged_search(self, base_dn, scope, search_filter, attr_list):
        if not self.conn:
            return None

        total_result = []
        ctrl = SimplePagedResultsControl(True, size=LdapConn.PAGE_SIZE,
                                         cookie='')
        while True:
            try:
                result = self.conn.search_ext(base_dn, scope, search_filter,
                                              attr_list, serverctrls=[ctrl])
                rtype, rdata, rmsgid, ctrls = self.conn.result3(result)
            except ldap.LDAPError as e:
                if type(e.message) == dict and e.message['desc'] == 'No such object':
                    pass
                else:
                    logging.warning('Search failed for base dn(%s), filter(%s) '
                                    'on server %s error: %s' % (base_dn, search_filter,
                                                                self.host, e.message))
                return None

            total_result.extend(rdata)

            page_ctrls = [c for c in ctrls
                          if c.controlType == SimplePagedResultsControl.controlType]
            if not page_ctrls or not page_ctrls[0].cookie:
                break

            ctrl.cookie = page_ctrls[0].cookie

        return total_result

    def unbind_conn(self):
        if self.conn:
            self.conn.unbind()
