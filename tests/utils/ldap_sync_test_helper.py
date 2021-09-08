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
        #self.base_dn = config.get('LDAP', 'base')
        self.user_dn = config.get('LDAP', 'user_dn')
        self.passwd = config.get('LDAP', 'password')
        self.follow_referrals = None

        self.conn = ldap.initialize(self.host, bytes_mode=False)
        try:
            self.conn.set_option(ldap.OPT_REFERRALS, 1 if self.follow_referrals else 0)
        except ldap.LDAPError as e:
            logging.warning('Failed to set follow_referrals option, error: %s' % e)

        try:
            self.conn.simple_bind_s(self.user_dn, self.passwd)
        except ldap.INVALID_CREDENTIALS:
            self.conn = None
            logging.warning('Invalid credential %s:***** to connect ldap server %s' %
                            (self.user_dn, self.host))
        except ldap.LDAPError as e:
            self.conn = None
            logging.warning('Connect ldap server %s failed, error: %s' %
                            (self.host, e))

    def __del__(self):
        if self.conn:
            self.conn.unbind_s()


    def add_user(self, dn='', email=''):
        if not dn or not email:
            return
        attrs = {}
        attrs['userPrincipalName'] = bytes(email, encoding='utf-8')  # necessary
        attrs['objectclass'] = [b'top', b'person', b'organizationalPerson', b'user']
        ldif = modlist.addModlist(attrs)
        try:
            self.conn.add_s(dn, ldif)
        except ldap.ALREADY_EXISTS:
            self.conn.delete_s(dn)
            self.conn.add_s(dn, ldif)

    def update_user(self, dn, first_name='', last_name='', role='', contact_email='', department=''):
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

    def delete_user(self, dn):
        if not dn:
            return
        self.conn.delete_s(dn)

    def is_user_exist(self, cn):
        try:
            self.conn.search_s('CN=' + cn + ',OU=ceshiyi,OU=ceshi,DC=seafile,DC=ren', SCOPE_SUBTREE)
        except ldap.NO_SUCH_OBJECT:
            return False
        return True

    def rename_grp(self, old_grp_dn, new_grp_cn, new_parent_dn):
        if not old_grp_dn:
            return
        try:
            self.conn.rename_s(old_grp_dn, 'CN=' + new_grp_cn, new_parent_dn)
        except ldap.NO_SUCH_OBJECT:
            return

    def add_grp(self, grp_dn):
        if not grp_dn:
            return
        attrs = {}
        attrs['objectClass'] = [b'group', b'top']
        ldif = modlist.addModlist(attrs)
        self.conn.add_s(grp_dn, ldif)

    def delete_grp(self, grp_dn):
        try:
            self.conn.delete_s(grp_dn)
        except ldap.NO_SUCH_OBJECT:
            return

    def add_grp_to_grp(self, sub_grp_dn, parent_grp_dn):
        parent_grp = self.conn.search_s(parent_grp_dn, SCOPE_SUBTREE)
        sub_grp = self.conn.search_s(sub_grp_dn, SCOPE_SUBTREE)

        parent_attrs, old_parent_attrs = parent_grp[0][1], copy.copy(parent_grp[0][1])

        if 'member' in parent_attrs.keys():
            parent_attrs['member'] += [bytes(sub_grp_dn, encoding="utf8")]
        else:
            parent_attrs['member'] = [bytes(sub_grp_dn, encoding="utf8")]

        ldif = modlist.modifyModlist(old_parent_attrs, parent_attrs)
        self.conn.modify_s(parent_grp_dn, ldif)

    def add_user_to_grp(self, user_dn, grp_dn):
        grp = self.conn.search_s(grp_dn, SCOPE_SUBTREE)
        parent_attrs, old_parent_attrs = grp[0][1], copy.copy(grp[0][1])
        if 'member' in parent_attrs.keys():
            parent_attrs['member'] += [bytes(user_dn, encoding="utf8")]
        else:
            parent_attrs['member'] = [bytes(user_dn, encoding="utf8")]

        ldif = modlist.modifyModlist(old_parent_attrs, parent_attrs)
        self.conn.modify_s(grp_dn, ldif)

    def add_ou(self, dn):
        if not dn:
            return
        attrs = {}
        attrs['objectClass'] = [b'top', b'organizationalUnit']
        ldif = modlist.addModlist(attrs)
        self.conn.add_s(dn, ldif)

        return dn

    def delete_ou(self, dn):
        try:
            self.conn.delete_s(dn)
        except ldap.NO_SUCH_OBJECT:
            return

