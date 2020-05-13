import logging

import ldap
import ldap.modlist as modlist
import sys
import uuid

from ldap_test_config import *


class LDAPConn:
    def __init__(self, ):

        self.conn = ldap.initialize(HOST, bytes_mode=False)
        try:
            self.conn.set_option(ldap.OPT_REFERRALS, 0)
        except ldap.LDAPError as e:
            logging.warning('Failed to set follow_referrals option, error: %s' % e.message)

        try:
            self.conn.simple_bind_s(USER_DN, PASSWORD)
        except ldap.INVALID_CREDENTIALS:
            self.conn = None
            logging.warning('Invalid credential %s:***** to connect ldap server %s' %
                    (USER_DN, HOST))
        except ldap.LDAPError as e:
            self.conn = None
            logging.warning('Connect ldap server %s failed, error: %s' %
                    (PASSWORD, e.message))

    def __del__(self):
        if self.conn:
            self.conn.unbind_s()

    def add_user(self, dn='', email=''):

        attrs = {}
        attrs['userPrincipalName'] = bytes(email, encoding='utf-8')  # necessary
        attrs['objectclass'] = [b'top', b'person', b'organizationalPerson', b'user']
        ldif = modlist.addModlist(attrs)
        try:
            self.conn.add_s(dn, ldif)
        except ldap.ALREADY_EXISTS:
            self.conn.delete_s(dn)
            self.conn.add_s(dn, ldif)

    def delete_user(self, dn):
        if not dn:
            return
        self.conn.delete_s(dn)

if __name__ == '__main__':
    conn = LDAPConn()

    dn_email_dict = {}

    if len(sys.argv) < 2:
        print('arg error, must be create or delete')
        exit(1)

    action = sys.argv[1]
    if action not in ['create', 'delete']:
        print('arg error, must be create or delete')
        exit(1)
    print('wait a few seconds.')

    for i in range(BATCH_USER_COUNT):
        dn_email_dict[f'cn={i},'+BASE] = uuid.uuid4().hex + '@seafile.ren'

    if action == 'create':
        for dn, email in dn_email_dict.items():
            conn.add_user(dn, email)
    elif action == 'delete':
        for dn in dn_email_dict.keys():
            conn.delete_user(dn)
    print('done.')





