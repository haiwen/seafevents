#coding: utf-8

import logging

from seaserv import get_ldap_users, add_ldap_user, update_ldap_user, \
        del_ldap_user
from ldap_sync import LdapSync
from ldap import SCOPE_SUBTREE

class LdapUser(object):
    def __init__(self, user_id, password, nickname):
        self.user_id = user_id
        self.password = password
        self.nickname = nickname

class LdapUserSync(LdapSync):
    def __init__(self, settings):
        LdapSync.__init__(self, settings)
        self.auser = 0
        self.uuser = 0
        self.duser = 0
        self.anickname = 0
        self.unickname = 0
        self.dnickname = 0
        if self.settings.enable_nickname_sync:
            self.init_seahub_db()

    def init_seahub_db(self):
        try:
            import MySQLdb
            import seahub_settings
        except ImportError as e:
            logging.info('Failed to import MySQLdb or seahub_settings module: %s, '
                         'disable nickname sync.' % e)
            self.settings.enable_nickname_sync = False
            return

        try:
            db_infos = seahub_settings.DATABASES['default']
        except KeyError as e:
            logging.info('Can not find db info in seahub settings, disable nickname sync.')
            self.settings.enable_nickname_sync = False
            return

        if db_infos.get('ENGINE') != 'django.db.backends.mysql':
            logging.info('Nickname sync feature only mysql db supported, '
                         'disable nickname sync.')
            self.settings.enable_nickname_sync = False
            return

        db_host = db_infos.get('HOST', '127.0.0.1')
        db_port = db_infos.get('PORT', 3306)
        db_name = db_infos.get('NAME')
        if not db_name:
            logging.info('DB name is not setted, disable nickname sync.')
            self.settings.enable_nickname_sync = False
            return
        db_user = db_infos.get('USER')
        if not db_user:
            logging.info('DB user is not setted, disable nickname sync.')
            self.settings.enable_nickname_sync = False
            return
        db_passwd = db_infos.get('PASSWORD')

        try:
            self.db_conn = MySQLdb.connect(host=db_host, port=db_port,
                                           user=db_user, passwd=db_passwd,
                                           db=db_name, charset='utf8')
            self.db_conn.autocommit(True)
            self.cursor = self.db_conn.cursor()
        except Exception as e:
            logging.info('Failed to connect mysql: %s, disable nick name sync.' %  e)
            self.settings.enable_nickname_sync = False

    def close_seahub_db(self):
        if self.settings.enable_nickname_sync:
            self.cursor.close()
            self.db_conn.close()

    def show_sync_result(self):
        logging.info('LDAP user sync result: add [%d]user, update [%d]user, delete [%d]user' %
                     (self.auser, self.uuser, self.duser))
        if self.settings.enable_nickname_sync:
            logging.info('LDAP nickname sync result: add [%d]nickname, update [%d]nickname, '
                         'delete [%d]nickname' % (self.anickname, self.unickname, self.dnickname))

    def get_nickname(self, email):
        try:
            self.cursor.excute('select nickname from profile_profile where user=?',
                               email)
            r = self.cursor.fetchone()
            if r:
                nickname = r[0]
            else:
                nickname = None
        except Exception as e:
            nickname = None
        return nickname

    def add_nickname(self, email, nickname):
        try:
            self.cursor.execute('insert into profile_profile (user,nickname,intro) values '
                                '(%s,%s,%s)', (email, nickname, ''))
            if self.cursor.rowcount == 1:
                logging.debug('Add nickname %s to user %s successs.' %
                              (nickname, email))
                self.anickname += 1
        except Exception as e:
            logging.warning('Failed to add nickname %s to user %s: %s.' %
                            (nickname, email, e))

    def update_nickname(self, email, nickname):
        try:
            self.cursor.execute('select 1 from profile_profile where user=%s', email)
            if self.cursor.rowcount == 0:
                self.cursor.execute('insert into profile_profile (user,nickname,intro) '
                                    'values (%s,%s,%s)', (email, nickname, ''))
            else:
                self.cursor.execute('update profile_profile set nickname=%s where user=%s',
                                    (nickname, email))
            if self.cursor.rowcount == 1:
                logging.debug('Update user %s nickname to %s success.' %
                              (email, nickname))
                self.unickname += 1
        except Exception as e:
            logging.warning('Failed to update user %s nickname to %s: %s.' %
                            (email, nickname, e))

    def del_nickname(self, email):
        try:
            self.cursor.execute('delete from profile_profile where user=%s', email)
            if self.cursor.rowcount == 1:
                logging.debug('Delete profile info for user %s success.' % email)
                self.dnickname += 1
        except Exception as e:
            logging.warning('Failed to delete profile info for user %s: %s.' %
                            (email, e))

    def get_data_from_db(self):
        # user_id <-> LdapUser
        user_data_db = None
        users = get_ldap_users(-1, -1)
        if users is None:
            logging.warning('get ldap users from db failed.')
            return user_data_db

        user_data_db = {}
        if self.settings.enable_nickname_sync:
            for user in users:
                nickname = self.get_nickname(user.email)
                user_data_db[user.email] = LdapUser(user.id, user.password, nickname)
        else:
            for user in users:
                user_data_db[user.email] = LdapUser(user.id, user.password, None)

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
                                                 self.settings.pwd_change_attr,
                                                 self.settings.first_name_attr,
                                                 self.settings.last_name_attr])
        else:
            users = self.ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                          search_filter,
                                          [self.settings.login_attr,
                                           self.settings.pwd_change_attr,
                                           self.settings.first_name_attr,
                                           self.settings.last_name_attr])
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

            if not attrs.has_key(self.settings.first_name_attr):
                first_name = ''
            else:
                first_name = attrs[self.settings.first_name_attr][0]

            if not attrs.has_key(self.settings.last_name_attr):
                last_name = ''
            else:
                last_name = attrs[self.settings.last_name_attr][0]

            if self.settings.name_reverse:
                user_name = last_name + ' ' + first_name
            else:
                user_name = first_name + ' ' + last_name

            email = attrs[self.settings.login_attr][0].lower()
            user_data_ldap[email] = LdapUser(None, password, user_name.strip())

        return user_data_ldap

    def sync_add_user(self, ldap_user, email):
        user_id = add_ldap_user(email, ldap_user.password, 0, 1)
        if user_id <= 0:
            logging.warning('Add user [%s] failed.' % email)
            return
        self.auser += 1
        logging.debug('Add user [%s] success.' % email)

        if self.settings.enable_nickname_sync:
            self.add_nickname(email, ldap_user.nickname)

    def sync_update_user(self, ldap_user, db_user, email):
        if ldap_user.password != db_user.password:
            rc = update_ldap_user(db_user.user_id, email, ldap_user.password, 0, 1)
            if rc < 0:
                logging.warning('Update user [%s] failed.' % email)
            else:
                logging.debug('Update user [%s] success.' % email)
                self.uuser += 1

        if self.settings.enable_nickname_sync:
            if ldap_user.nickname != db_user.nickname:
                self.update_nickname(email, ldap_user.nickname)

    def sync_del_user(self, db_user, email):
        ret = del_ldap_user(db_user.user_id)
        if ret < 0:
            logging.warning('Delete user [%s] failed.' % email)
            return
        logging.debug('Delete user [%s] success.' % email)
        self.duser += 1

        if self.settings.enable_nickname_sync:
            self.del_nickname(email)

    def sync_data(self, data_db, data_ldap):
        # sync deleted user in ldap to db
        for k in data_db.iterkeys():
            if not data_ldap.has_key(k):
                self.sync_del_user(data_db[k], k)

        # sync undeleted user in ldap to db
        for k, v in data_ldap.iteritems():
            if data_db.has_key(k):
                self.sync_update_user(v, data_db[k], k)
            else:
                # add user to db
                self.sync_add_user(v, k)

        self.close_seahub_db()
