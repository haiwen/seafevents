import os
import logging
import copy

import ldap
from ldap import SCOPE_SUBTREE
import ldap.modlist as modlist

class LDAPSyncTestHelper:
    def __init__(self, config):
        os.environ['PYTHONPATH'] = '/usr/lib/python3/dist-packages:/usr/lib/python3.7/dist-packages:/usr/lib/python3.7/site-packages:/usr/local/lib/python3.7/dist-packages:/usr/local/lib/python3.7/site-packages:/data/dev/seahub/thirdpart:/data/dev/pyes/pyes:/data/dev/seahub-extra::/data/dev/portable-python-libevent/libevent:/data/dev/seafobj:/data/dev/seahub/seahub/:/data/dev/'
        self.host = config.get('LDAP', 'host')
        self.base_dn = config.get('LDAP', 'base')
        self.user_dn = config.get('LDAP', 'user_dn')
        self.passwd = config.get('LDAP', 'password')
        self.follow_referrals = None
        self.test_user_dn = ''

        self.conn = ldap.initialize(self.host, bytes_mode=False)
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

    def __del__(self):
        if self.conn:
            self.conn.unbind_s()

    def add_test_user(self, cn='', email=''):

        if not cn or not email:
            return
        attrs = {}
        attrs['givenName'] = [b'default_test_firstname']
        attrs['sn'] = [b'default_test_lastname']
        attrs['title'] = [b'Default']
        attrs['mail'] = [b'default_test_contact_email']
        attrs['department'] = [b'default_department_name']
        attrs['userPrincipalName'] = bytes(email, encoding='utf-8')  # necessary
        attrs['objectclass'] = [b'top', b'person', b'organizationalPerson', b'user']

        test_user_dn = 'CN=' + cn + ',OU=ceshiyi,OU=ceshi,DC=seafile,DC=ren'

        # Convert our dict to nice syntax for the add-function using modlist-module
        ldif = modlist.addModlist(attrs)
        self.conn.add_s(test_user_dn, ldif)

    def update_test_user(self, cn, first_name='', last_name='', role='', contact_email='', department=''):
        dn = 'CN=' + cn + ',OU=ceshiyi,OU=ceshi,DC=seafile,DC=ren'
        user = self.conn.search_s(dn, SCOPE_SUBTREE)
        old_attrs = user[0][1]

        attrs = copy.copy(old_attrs)
        if first_name:
            attrs['givenName'] = [bytes(first_name, encoding="utf8")]
        if last_name:
            attrs['sn'] = [bytes(last_name, encoding="utf8")]
        if role:
            attrs['title'] = [bytes(role, encoding="utf8")]
        if contact_email:
            attrs['mail'] = [bytes(contact_email, encoding="utf8")]
        if department:
            attrs['department'] = [bytes(department, encoding="utf8")]

        ldif = modlist.modifyModlist(old_attrs, attrs)
        self.conn.modify_s(dn, ldif)

    def delete_test_user(self, cn):
        if not cn:
            return
        self.conn.delete_s('CN=' + cn + ',OU=ceshiyi,OU=ceshi,DC=seafile,DC=ren')

    def is_user_exist(self, cn):
        try:
            self.conn.search_s('CN=' + cn + ',OU=ceshiyi,OU=ceshi,DC=seafile,DC=ren', SCOPE_SUBTREE)
        except ldap.NO_SUCH_OBJECT:
            return False
        return True