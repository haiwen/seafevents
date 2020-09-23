#coding: utf-8

import logging
logger = logging.getLogger('ldap_sync')
logger.setLevel(logging.DEBUG)

from seaserv import get_ldap_users, add_ldap_user, update_ldap_user, \
        seafile_api, ccnet_api
from .ldap_conn import LdapConn
from .ldap_sync import LdapSync
from .utils import bytes2str
from ldap import SCOPE_SUBTREE

def default_ldap_role_mapping(role):
    return role

role_mapping = None
try:
    from custom_functions import ldap_role_mapping
    role_mapping = ldap_role_mapping
except:
    role_mapping = default_ldap_role_mapping

class LdapUser(object):
    def __init__(self, user_id, password, name, dept, uid, cemail,
                 is_staff=0, is_active=1, role = '', is_manual_set = False):
        self.user_id = user_id
        self.password = password
        self.name = name
        self.dept = dept
        self.uid = uid
        self.cemail = cemail
        self.is_staff = is_staff
        self.is_active = is_active
        self.role = role
        self.is_manual_set = is_manual_set

class LdapUserSync(LdapSync):
    def __init__(self, settings):
        LdapSync.__init__(self, settings)
        self.auser = 0
        self.uuser = 0
        self.duser = 0

        self.arole = 0
        self.urole = 0

        self.aprofile = 0
        self.uprofile = 0
        self.dprofile = 0

        self.adept = 0
        self.udept = 0
        self.ddept = 0

        self.db_conn = None
        self.cursor = None

        self.init_seahub_db()

        if self.cursor is None and settings.load_extra_user_info_sync:
            logger.debug('Failed to init seahub db, disable sync user extra info.')
            for config in self.settings.ldap_configs:
                config.enable_extra_user_info_sync = False

    def init_seahub_db(self):
        try:
            import pymysql
            pymysql.install_as_MySQLdb()
            import seahub_settings
        except ImportError as e:
            logger.warning('Failed to init seahub db: %s.' %  e)
            return

        try:
            db_infos = seahub_settings.DATABASES['default']
        except KeyError as e:
            logger.warning('Failed to init seahub db, can not find db info in seahub settings.')
            return

        if db_infos.get('ENGINE') != 'django.db.backends.mysql':
            logger.warning('Failed to init seahub db, only mysql db supported.')
            return

        db_host = db_infos.get('HOST', '127.0.0.1')
        db_port = int(db_infos.get('PORT', '3306'))
        db_name = db_infos.get('NAME')
        if not db_name:
            logger.warning('Failed to init seahub db, db name is not setted.')
            return
        db_user = db_infos.get('USER')
        if not db_user:
            logger.warning('Failed to init seahub db, db user is not setted.')
            return
        db_passwd = db_infos.get('PASSWORD')

        try:
            self.db_conn = pymysql.connect(host=db_host, port=db_port,
                                           user=db_user, passwd=db_passwd,
                                           db=db_name, charset='utf8')
            self.db_conn.autocommit(True)
            self.cursor = self.db_conn.cursor()
        except Exception as e:
            logger.warning('Failed to init seahub db: %s.' %  e)

    def close_seahub_db(self):
        if self.cursor:
            self.cursor.close()
        if self.db_conn:
            self.db_conn.close()

    def show_sync_result(self):
        logger.info('''LDAP user sync result: add [%d]user, update [%d]user, deactive [%d]user, add [%d]role, update [%d]role''' %
                     (self.auser, self.uuser, self.duser, self.arole, self.urole))

        if self.settings.load_extra_user_info_sync:
            logger.info('LDAP profile sync result: add [%d]profile, update [%d]profile, '
                         'delete [%d]profile' % (self.aprofile, self.uprofile, self.dprofile))
            logger.info('LDAP dept sync result: add [%d]dept, update [%d]dept, '
                         'delete [%d]dept' % (self.adept, self.udept, self.ddept))


    def add_profile(self, email, ldap_user):
        # list_in_address_book: django will not apply default value to mysql. it will be processed in ORM.
        field = 'user, nickname, intro, list_in_address_book'
        qmark = '%s, %s, %s, %s'
        val = [email, ldap_user.name, '', False]
        if ldap_user.uid:
            field += ', login_id'
            qmark += ', %s'
            val.append(ldap_user.uid)
        if ldap_user.cemail:
            field += ', contact_email'
            qmark += ', %s'
            val.append(ldap_user.cemail)
        sql = 'insert into profile_profile (%s) values (%s)' % (field, qmark)
        try:
            self.cursor.execute(sql, val)
            if self.cursor.rowcount == 1:
                logger.debug('Add profile %s to user %s successs.' %
                              (val, email))
                self.aprofile += 1
        except Exception as e:
            logger.warning('Failed to add profile %s to user %s: %s.' %
                            (val, email, e))

    def add_dept(self, email, dept):
        try:
            self.cursor.execute('insert into profile_detailedprofile (user,department,telephone) '
                                'values (%s,%s,%s)', (email, dept, ''))
            if self.cursor.rowcount == 1:
                logger.debug('Add dept %s to user %s successs.' %
                              (dept, email))
                self.adept += 1
        except Exception as e:
            logger.warning('Failed to add dept %s to user %s: %s.' %
                            (dept, email, e))

    def update_profile(self, email, db_user, ldap_user):
        try:
            self.cursor.execute('select 1 from profile_profile where user=%s', [email])
            if self.cursor.rowcount == 0:
                self.add_profile(email, ldap_user)
                return
            else:
                field = ''
                val = []
                if db_user.name != ldap_user.name:
                    field += 'nickname=%s'
                    val.append(ldap_user.name)
                if ldap_user.uid is not None and db_user.uid != ldap_user.uid:
                    if field == '':
                        field += 'login_id=%s'
                    else:
                        field += ', login_id=%s'
                    val.append(ldap_user.uid)
                if ldap_user.cemail is not None and db_user.cemail != ldap_user.cemail:
                    if field == '':
                        field += 'contact_email=%s'
                    else:
                        field += ', contact_email=%s'
                    val.append(ldap_user.cemail)
                if field == '':
                    # no change
                    return
                val.append(email)
                sql = 'update profile_profile set %s where user=%%s' % field
                self.cursor.execute(sql, val)
                if self.cursor.rowcount == 1:
                    logger.debug('Update user %s profile to %s success.' %
                                  (email, val))
                    self.uprofile += 1
        except Exception as e:
            logger.warning('Failed to update user %s profile: %s.' %
                            (email, e))

    def update_dept(self, email, dept):
        try:
            self.cursor.execute('select 1 from profile_detailedprofile where user=%s', [email])
            if self.cursor.rowcount == 0:
                self.add_dept(email, dept)
                return
            else:
                self.cursor.execute('update profile_detailedprofile set department=%s where user=%s',
                                    (dept, email))
            if self.cursor.rowcount == 1:
                logger.debug('Update user %s dept to %s success.' %
                              (email, dept))
                self.udept += 1
        except Exception as e:
            logger.warning('Failed to update user %s dept to %s: %s.' %
                            (email, dept, e))

    def del_profile(self, email):
        try:
            self.cursor.execute('delete from profile_profile where user=%s', [email])
            if self.cursor.rowcount == 1:
                logger.debug('Delete profile info for user %s success.' % email)
                self.dprofile += 1
        except Exception as e:
            logger.warning('Failed to delete profile info for user %s: %s.' %
                            (email, e))

    def del_dept(self, email):
        try:
            self.cursor.execute('delete from profile_detailedprofile where user=%s', [email])
            if self.cursor.rowcount == 1:
                logger.debug('Delete dept info for user %s success.' % email)
                self.ddept += 1
        except Exception as e:
            logger.warning('Failed to delete dept info for user %s: %s.' %
                            (email, e))

    def del_token(self, tab, email):
        try:
            sql = 'delete from {0} where user = %s'.format(tab)
            self.cursor.execute(sql, [email])
            if self.cursor.rowcount > 0:
                logger.debug('Delete token from %s for user %s success.' %
                              (tab, email))
        except Exception as e:
            logger.warning('Failed to delete token from %s for user %s: %s.' %
                            (tab, email, e))

    def get_data_from_db(self):
        # user_id <-> LdapUser
        user_data_db = None
        users = get_ldap_users(-1, -1)
        if users is None:
            logger.warning('get ldap users from db failed.')
            return user_data_db

        # select all users attrs from profile_profile and profile_detailedprofile in one query
        email2attrs = {}  # is like: { 'some_one@seafile': {'name': 'leo', 'dept': 'dev', ...} ...}
        if self.settings.load_extra_user_info_sync:
            profile_sql = "SELECT user, nickname, contact_email, login_id FROM profile_profile"
            detailed_profile_sql = "SELECT user, department FROM profile_detailedprofile"
            try:
                self.cursor.execute(profile_sql)
                profile_res = self.cursor.fetchall()
                self.cursor.execute(detailed_profile_sql)
                detailed_profile_res = self.cursor.fetchall()
            except Exception as e:
                logger.warning('Failed to get profile info for db users %s.'.format(e))

            email2dept = {}

            for row in detailed_profile_res:
                email2dept[row[0]] = row[1]

            for row in profile_res:
                email = row[0]
                name = row[1]
                cemail = row[2]
                uid = row[3]
                attr_dict = {
                    'name': name,
                    'dept': email2dept.get(email, ''),
                }
                email2attrs[email] = attr_dict
                if self.settings.load_uid_attr != '':
                    email2attrs[email]['uid'] = uid
                if self.settings.load_cemail_attr != '':
                    email2attrs[email]['cemail'] = cemail

        name = None
        dept = None
        uid = None
        cemail = None
        user_data_db = {}
        for user in users:
            if not user:
                continue
            user_attrs = email2attrs.get(user.email, {})
            if user_attrs and self.settings.load_extra_user_info_sync:
                name = user_attrs.get('name', '')
                dept = user_attrs.get('dept', '')
                uid = user_attrs.get('uid', '')
                cemail = user_attrs.get('email', '')

            user_data_db[user.email] = LdapUser(user.id, user.password, name, dept,
                                                uid, cemail,
                                                1 if user.is_staff else 0,
                                                1 if user.is_active else 0,
                                                user.role,
                                                user.is_manual_set)
        return user_data_db

    def get_data_from_ldap_by_server(self, config):
        if not config.enable_user_sync:
            return {}
        ldap_conn = LdapConn(config.host, config.user_dn, config.passwd, config.follow_referrals)
        ldap_conn.create_conn()
        if not ldap_conn.conn:
            return None

        #  dn <-> LdapUser
        user_data_ldap = {}
        # search all users on base dn
        if config.user_filter != '':
            search_filter = '(&(objectClass=%s)(%s))' % \
                             (config.user_object_class,
                              config.user_filter)
        else:
            search_filter = '(objectClass=%s)' % config.user_object_class

        base_dns = config.base_dn.split(';')
        for base_dn in base_dns:
            if base_dn == '':
                continue
            data = self.get_data_by_base_dn(config, ldap_conn, base_dn, search_filter)
            if data is None:
                continue
            user_data_ldap.update(data)

        ldap_conn.unbind_conn()

        return user_data_ldap

    def get_data_by_base_dn(self, config, ldap_conn, base_dn, search_filter):
        user_data_ldap = {}
        search_attr = [config.login_attr, config.pwd_change_attr]

        if config.role_name_attr:
            search_attr.append(config.role_name_attr)

        if config.enable_extra_user_info_sync:
            search_attr.append(config.first_name_attr)
            search_attr.append(config.last_name_attr)
            search_attr.append(config.dept_attr)

            if config.uid_attr != '':
                search_attr.append(config.uid_attr)
            if config.cemail_attr != '':
                search_attr.append(config.cemail_attr)

        if config.use_page_result:
            users = ldap_conn.paged_search(base_dn, SCOPE_SUBTREE,
                                           search_filter, search_attr)
        else:
            users = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                     search_filter, search_attr)
        if users is None:
            return None
        users = bytes2str(users)

        for pair in users:
            user_dn, attrs = pair
            if not isinstance(attrs, dict):
                continue
            if config.login_attr not in attrs:
                continue
            if config.pwd_change_attr not in attrs:
                password = ''
            else:
                password = attrs[config.pwd_change_attr][0]

            user_name = None
            dept = None
            uid = None
            cemail = None
            role = None

            if config.role_name_attr not in attrs:
                role = ''
            else:
                role = attrs[config.role_name_attr][0]

            if config.enable_extra_user_info_sync:
                if config.first_name_attr not in attrs:
                    first_name = ''
                else:
                    first_name = attrs[config.first_name_attr][0]

                if config.last_name_attr not in attrs:
                    last_name = ''
                else:
                    last_name = attrs[config.last_name_attr][0]

                if config.name_reverse:
                    user_name = last_name + ' ' + first_name
                else:
                    user_name = first_name + ' ' + last_name

                if config.dept_attr not in attrs:
                    dept = ''
                else:
                    dept = attrs[config.dept_attr][0]

                if config.uid_attr != '':
                   if config.uid_attr not in attrs:
                       uid = ''
                   else:
                        uid = attrs[config.uid_attr][0]

                if config.cemail_attr != '':
                   if config.cemail_attr not in attrs:
                       cemail = ''
                   else:
                       cemail = attrs[config.cemail_attr][0]

            email = attrs[config.login_attr][0].lower()
            user_name = None if user_name is None else user_name.strip()
            user_data_ldap[email] = LdapUser(None, password, user_name, dept,
                                             uid, cemail, role = role)

        return user_data_ldap

    def sync_add_user(self, ldap_user, email):
        user_id = add_ldap_user(email, ldap_user.password, 0,
                                1 if self.settings.activate_user else 0)
        if user_id <= 0:
            logger.warning('Add user [%s] failed.' % email)
            return
        self.auser += 1
        logger.debug('Add user [%s] success.' % email)

        ret = 0
        if ldap_user.role:
            role = role_mapping(ldap_user.role)
            ret = ccnet_api.update_role_emailuser(email, role, False)

            if ret == 0:
                self.arole += 1
                logger.debug('Add role [%s] for user [%s] success.' % (role, email))

            if ret < 0:
                logger.warning('Add role [%s] for user [%s] failed.' % (role, email))

        if ldap_user.config.enable_extra_user_info_sync:
            self.add_profile(email, ldap_user)
            self.add_dept(email, ldap_user.dept)

    def sync_update_user(self, ldap_user, db_user, email):
        '''
        set_status = False
        if db_user.is_active == 0:
            set_status = True

        if ldap_user.password != db_user.password or set_status:
            rc = update_ldap_user(db_user.user_id, email, ldap_user.password,
                                  db_user.is_staff, db_user.is_active)
            if rc < 0:
                logger.warning('Update user [%s] failed.' % email)
            else:
                logger.debug('Update user [%s] success.' % email)
                self.uuser += 1
        '''
        ret = 0

        if ldap_user.role:
            role = role_mapping(ldap_user.role)
            if not db_user.is_manual_set and db_user.role != role:
                ret = ccnet_api.update_role_emailuser(email, role, False)

                if ret == 0:
                    self.urole += 1
                    #logger.debug('Update role [%s] for user [%s] success.' % (role, email))

                if ret < 0:
                    logger.warning('Update role [%s] for user [%s] failed.' % (role, email))

        if ldap_user.config.enable_extra_user_info_sync:
            self.update_profile(email, db_user, ldap_user)
            if ldap_user.dept != db_user.dept:
                self.update_dept(email, ldap_user.dept)

        if not db_user.is_active and ldap_user.config.auto_reactivate_users:
            try:
                ret = update_ldap_user(db_user.user_id, email, db_user.password,
                            db_user.is_staff, 1)
            except Exception as e:
                logger.error('Reactivate user [{}] failed. ERROR: {}'.format(email, e))
                return

            if ret < 0:
                logger.warning('Reactivate user [%s] failed.' % email)
                return
            logger.debug('Reactivate user [%s] success.' % email)

    def sync_del_user(self, db_user, email):
        ret = update_ldap_user(db_user.user_id, email, db_user.password,
                               db_user.is_staff, 0)
        if ret < 0:
            logger.warning('Deactive user [%s] failed.' % email)
            return
        logger.debug('Deactive user [%s] success.' % email)
        self.duser += 1

        if self.cursor:
            self.del_token('api2_token', email)
            self.del_token('api2_tokenv2', email)
        else:
            logger.debug('Failed to connect seahub db, omit delete api token for user [%s].' %
                          email)
        try:
            seafile_api.delete_repo_tokens_by_email(email)
            logger.debug('Delete repo tokens for user %s success.', email)
        except Exception as e:
            logger.warning("Failed to delete repo tokens for user %s: %s." % (email, e))

        if self.settings.load_extra_user_info_sync:
            self.del_profile(email)
            self.del_dept(email)

    def sync_data(self, data_db, data_ldap):
        # sync deleted user in ldap to db
        for k in data_db.keys():
            if k not in data_ldap and data_db[k].is_active == 1:
                if self.settings.enable_deactive_user:
                    self.sync_del_user(data_db[k], k)
                else:
                    logger.debug('User[%s] not found in ldap, '
                                  'DEACTIVE_USER_IF_NOTFOUND option is not set, so not deactive it.' % k)

        # sync undeleted user in ldap to db
        for k, v in data_ldap.items():
            if k in data_db:
                self.sync_update_user(v, data_db[k], k)
            else:
                # add user to db
                if self.settings.import_new_user:
                    self.sync_add_user(v, k)

        self.close_seahub_db()
