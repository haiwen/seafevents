#coding: utf-8

import logging
import sys
logger = logging.getLogger('ldap_sync')
logger.setLevel(logging.DEBUG)

from seaserv import get_ldap_users, add_ldap_user, update_ldap_user, del_ldap_user, \
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
        # PingAn customization:
        # Since login_id and contact_email are unique, we need to delete any existing duplicate entries.
        # Otherwise there may be entries in profile_profile tables that belong to deactivated user,
        # causing failure to insert new entry with the same login_id or contact_email.
        # For example, a user's email changed in LDAP. Then it will be deactivated and a new user will
        # be created during LDAP sync.
        if ldap_user.uid is not None and ldap_user.uid != '':
            sql = 'delete from profile_profile where login_id=%s'
            try:
                self.cursor.execute(sql, [ldap_user.uid])
            except Exception as e:
                logger.warning('Failed to delete duplicate profile for login_id %s: %s.',
                               ldap_user.uid, e)

        if ldap_user.cemail is not None and ldap_user.cemail != '':
            sql = 'delete from profile_profile where contact_email=%s'
            try:
                self.cursor.execute(sql, [ldap_user.cemail])
            except Exception as e:
                logger.warning('Failed to delete duplicate profile for contact_email %s: %s.',
                               ldap_user.cemail, e)

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

    def update_profile_user_login_id(self, user, uid):
        try:
            self.cursor.execute('update profile_profile set user=%s where login_id=%s',
                                (user, uid))
            if self.cursor.rowcount == 1:
                logger.debug('Update user email for login id %s to %s success.' % (uid, user))
        except Exception as e:
            logger.warning('Failed to update profile user to %s.' % user)

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

    def get_uid_from_profile(self):
        # email <-> uid
        email_to_uid = {}
        try:
            self.cursor.execute('select user, login_id from profile_profile')
            r = self.cursor.fetchall()
            for row in r:
                email = row[0].lower()
                uid = row[1]
                email_to_uid[email.encode("utf-8")] = '' if not uid else uid.encode('utf-8')
        except Exception as e:
            logger.warning('Failed to get uid from profile_profile: %s.' % e)
            sys.exit(1)

        return email_to_uid

    def get_uid_to_users(self, email_to_uid):
        # user_id <-> LdapUser
        uid_to_users = {}
        users = get_ldap_users(-1, -1)
        if users is None:
            logger.warning('get ldap users from db failed.')
            return uid_to_users

        uid = None
        for user in users:
            email = user.email.lower().encode("utf-8")
            if email in email_to_uid:
                uid = email_to_uid[email].lower()
                if uid in uid_to_users:
                    uid_users = uid_to_users[uid]
                    uid_users.append(email)
                else:
                    uid_to_users[uid] = [email]
            else:
                end = user.email.rfind('@')
                if end < 1:
                    continue
                uid =  user.email[0:end].encode("utf-8").lower()
                if uid in uid_to_users:
                    uid_users = uid_to_users[uid]
                    uid_users.append(email)
                else:
                    uid_to_users[uid] = [email]

        return uid_to_users

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

    def get_uid_to_ldap_user(self, data_ldap):
        uid_to_ldap_user = {}
        for k, v in data_ldap.iteritems():
            email = k
            uid = v.uid.lower()
            if email is not None:
                if uid in uid_to_ldap_user:
                    continue
                else:
                    uid_to_ldap_user[uid] = email

        return uid_to_ldap_user

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
                # Failed to get data by base dn
                return None
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

    def sync_update_user(self, ldap_user, db_user, email, new_email):
        # PingAn customization: reactivate user when it's added back to AD.
        if db_user.is_active == 0:
            db_user.is_active = 1

        rc = update_ldap_user(db_user.user_id, new_email, ldap_user.password,
                              db_user.is_staff, db_user.is_active)
        if rc < 0:
            logger.warning('Activate user [%s] failed.' % email)
        else:
            logger.debug('Activate user [%s] success.' % email)
            self.uuser += 1

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

    def sync_migrate_user(self, old_user, new_user):
        if seafile_api.update_email_id (old_user, new_user) < 0:
            logger.warning('Failed to update emailuser id to %s.' % new_user)
        logger.debug('$migrate$ %s $to$ %s .' % (old_user, new_user))

        try:
            self.cursor.execute('update profile_detailedprofile set user=%s where user=%s',
                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update profile_detailedprofile user to %s.' % new_user)

        try:
            self.cursor.execute('update share_fileshare set username=%s where username=%s',
                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update share_fileshare username to %s.' % new_user)

        try:
            self.cursor.execute('update share_uploadlinkshare set username=%s where username=%s',
                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update share_uploadlinkshare username to %s.' % new_user)

        try:
            self.cursor.execute('update base_userstarredfiles set email=%s where email=%s',
                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update base_userstarredfiles email to %s.' % new_user)

        try:
            self.cursor.execute('update api2_token set user=%s where user=%s',
                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update api2_token user to %s.' % new_user)

        try:
            self.cursor.execute('update api2_tokenv2 set user=%s where user=%s',
                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update api2_tokenv2 user to %s.' % new_user)

        try:
            self.cursor.execute('update admin_log_adminlog set email=%s where email=%s',
                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update admin_log_adminlog email to %s.' % new_user)

        try:
            self.cursor.execute('update avatar_avatar set emailuser=%s where emailuser=%s',
                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update avatar_avatar email to %s.' % new_user)

        try:
            self.cursor.execute('update base_clientlogintoken set username=%s where username=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update base_clientlogintoken email to %s.' % new_user)

        try:
            self.cursor.execute('update base_devicetoken set user=%s where user=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update base_devicetoken user to %s.' % new_user)

        try:
            self.cursor.execute('update base_filecomment set author=%s where author=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update base_filecomment author to %s.' % new_user)

        try:
            self.cursor.execute('update base_userlastlogin set username=%s where username=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update base_userlastlogin user to %s.' % new_user)

        try:
            self.cursor.execute('update drafts_draft set username=%s where username=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update drafts_draft user to %s.' % new_user)

        try:
            self.cursor.execute('update drafts_draftreviewer set reviewer=%s where reviewer=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update drafts_draftreviewer reviewer to %s.' % new_user)

        try:
            self.cursor.execute('update notifications_usernotification set to_user=%s where to_user=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update notifications_usernotification to_user to %s.' % new_user)

        try:
            self.cursor.execute('update options_useroptions set email=%s where email=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update options_useroptions email to %s.' % new_user)

        try:
            self.cursor.execute('update role_permissions_adminrole set email=%s where email=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update role_permissions_adminrole email to %s.' % new_user)

        try:
            self.cursor.execute('update sysadmin_extra_userloginlog set username=%s where username=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update sysadmin_extra_userloginlog email to %s.' % new_user)

        try:
            self.cursor.execute('update tags_filetag set username=%s where username=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update tags_filetag email to %s.' % new_user)

        try:
            self.cursor.execute('update wiki_wiki set username=%s where username=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update wiki_wiki email to %s.' % new_user)

        try:
            self.cursor.execute('update Activity set op_user=%s where op_user=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update Activity user to %s.' % new_user)

        try:
            self.cursor.execute('update UserActivity set username=%s where username=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update UserActivity user to %s.' % new_user)

        try:
            self.cursor.execute('update FileHistory set op_user=%s where op_user=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update FileHistory user to %s.' % new_user)

        try:
            self.cursor.execute('update FileAudit set user=%s where user=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update FileAudit user to %s.' % new_user)

        try:
            self.cursor.execute('update FileUpdate set user=%s where user=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update FileUpdate user to %s.' % new_user)

        try:
            self.cursor.execute('update PermAudit set from_user=%s where from_user=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update PermAudit from_user to %s.' % new_user)

        try:
            self.cursor.execute('update PermAudit set `to`=%s where `to`=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update PermAudit to_user to %s.' % new_user)

        try:
            self.cursor.execute('update UserTrafficStat set email=%s where email=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update UserTrafficStat email to %s.' % new_user)

        try:
            self.cursor.execute('update UserActivityStat set username=%s where username=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update UserActivityStat username to %s.' % new_user)

        try:
            self.cursor.execute('update UserTraffic set user=%s where user=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update UserTraffic user to %s.' % new_user)

        try:
            self.cursor.execute('update MonthlyUserTraffic set user=%s where user=%s',

                                (new_user, old_user))
        except Exception as e:
            logger.warning('Failed to update MonthlyUserTraffic user to %s.' % new_user)

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

    # Note: customized for PinaAn's requirements. Do not deactivate renamed users in ldap.
    # The checking of rename is based on profile_profile.login_id database field and uid attribute.
    def sync_data(self, data_db, data_ldap):
        email_to_uid = self.get_uid_from_profile()
        uid_to_users = self.get_uid_to_users(email_to_uid)
        uid_to_ldap_user = self.get_uid_to_ldap_user(data_ldap)
        # used to find renamed users

        # collect deleted users from ldap
        for k, v in uid_to_users.items():
            if uid_to_ldap_user and k not in uid_to_ldap_user:
                del_users = uid_to_users[k]
                for del_user in del_users:
                    if del_user in data_db and data_db[del_user].is_active == 1:
                        if self.settings.enable_deactive_user:
                            self.sync_del_user(data_db[del_user], del_user)
                        else:
                            logger.debug('User[%s] not found in ldap, '
                                         'DEACTIVE_USER_IF_NOTFOUND option is not set, so not deactive it.' % del_user)

        # collect migrated users
        nums = 0
        for k, v in data_ldap.items():
            if uid_to_users:
                uid = v.uid.lower()
                if uid in uid_to_users:
                    found_active = False
                    users = uid_to_users[uid]

                    for user in users:
                        if k == user:
                            found_active = True
                            break

                    if found_active:
                        for user in users:
                            if k == user:
                                continue
                            nums = nums + 1
                        continue

                    for user in users:
                        nums = nums + 1

        if nums > 0:
            logger.debug('%d users need migrate.' % nums)

        # sync new and existing users from ldap to db
        for k, v in data_ldap.items():
            if uid_to_users:
                uid = v.uid.lower()
                if uid not in uid_to_users:
                    if self.settings.import_new_user:
                        self.sync_add_user(v, k)
                else:
                    found_active = False
                    users = uid_to_users[uid]

                    for user in users:
                        if k == user:
                            if user in data_db and data_db[user].is_active == 0:
                                self.sync_update_user(v, data_db[user], user, k)
                            found_active = True
                            break

                    if found_active:
                        for user in users:
                            if k == user:
                                continue
                            self.sync_migrate_user(user, k)
                            del_ldap_user(data_db[user].user_id)
                            logger.debug('Delete user [%s] success.' % user)
                        self.update_profile_user_login_id (k, uid)
                        continue

                    email = users[0]
                    self.sync_update_user(v, data_db[email], email, k)
                    for user in users:
                        self.sync_migrate_user(user, k)
                        if email != user:
                            del_ldap_user(data_db[user].user_id)
                            logger.debug('Delete user [%s] success.' % user)
                    self.update_profile_user_login_id(k, uid)

        self.close_seahub_db()
