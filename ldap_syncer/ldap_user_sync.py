#coding: utf-8

import logging

from seaserv import get_ldap_users, add_ldap_user, update_ldap_user, \
        del_ldap_user
from ldap_sync import LdapSync
from ldap import SCOPE_SUBTREE

class LdapUser(object):
    def __init__(self, user_id, password):
        self.user_id = user_id
        self.password = password

class LdapUserSync(LdapSync):
    def __init__(self, settings):
        LdapSync.__init__(self, settings)
        self.auser = 0
        self.uuser = 0
        self.duser = 0

    def show_sync_result(self):
        logging.info('LDAP user sync result: add [%d]user, update [%d]user, delete [%d]user' %
                     (self.auser, self.uuser, self.duser))

    def get_data_from_db(self):
        # user_id <-> LdapUser
        user_data_db = None
        users = get_ldap_users(-1, -1)
        if users is None:
            logging.warning('get ldap users from db failed.')
            return user_data_db

        user_data_db = {}
        for user in users:
            user_data_db[user.email] = LdapUser(user.id, user.password)
        return user_data_db

    def get_data_from_ldap(self):
        #  dn <-> LdapUser
        user_data_ldap = {}
        # search all users on base dn
        if self.settings.user_filter != '':
            search_filter = '(&(objectClass=%s)(%s))' % \
                             (self.settings.user_object_class,
                              self.settings.user_filter)
        else:
            search_filter = '(objectClass=%s)' % self.settings.user_object_class

        base_dns = self.settings.base_dn.split(';')
        for base_dn in base_dns:
            if base_dn == '':
                continue
            data = self.get_data_by_base_dn(base_dn, search_filter)
            if data is None:
                return None
            user_data_ldap.update(data)

        return user_data_ldap

    def get_data_by_base_dn(self, base_dn, search_filter):
        user_data_ldap = {}

        if self.settings.use_page_result:
            users = self.ldap_conn.paged_search(base_dn, SCOPE_SUBTREE,
                                                search_filter,
                                                [self.settings.login_attr,
                                                 self.settings.pwd_change_attr])
        else:
            users = self.ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                          search_filter,
                                          [self.settings.login_attr,
                                           self.settings.pwd_change_attr])
        if users is None:
            return None

        for pair in users:
            user_dn, attrs = pair
            if type(attrs) != dict:
                continue
            if not attrs.has_key(self.settings.login_attr):
                continue
            if not attrs.has_key(self.settings.pwd_change_attr):
                password = ''
            else:
                password = attrs[self.settings.pwd_change_attr][0]
            user_data_ldap[attrs[self.settings.login_attr][0].lower()] = password

        return user_data_ldap

    def sync_data(self, data_db, data_ldap):
        # sync deleted user in ldap to db
        for k in data_db.iterkeys():
            if not data_ldap.has_key(k):
                ret = del_ldap_user(data_db[k].user_id)
                if ret < 0:
                    logging.warning('delete user [%s] failed.' % k)
                    return
                logging.debug('delete user [%s] success.' % k)
                self.duser += 1

        # sync undeleted user in ldap to db
        for k, v in data_ldap.iteritems():
            if data_db.has_key(k):
                if v != data_db[k].password:
                    rc = update_ldap_user(data_db[k].user_id, k, v, 0, 1)
                    if rc < 0:
                        logging.warning('update user [%s] failed.' % k)
                        return
                    logging.debug('update user [%s] success.' % k)
                    self.uuser += 1
            else:
                # add user to db
                user_id = add_ldap_user(k, v, 0, 1)
                if user_id <= 0:
                    logging.warning('add user [%s] failed.' % k)
                    return
                self.auser += 1
                logging.debug('add user [%s] success.' % k)
