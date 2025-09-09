# -*- coding: utf-8 -*-
import uuid
import logging
from datetime import datetime
from seaserv import seafile_api, ccnet_api
from .ldap_conn import LdapConn
from .ldap_sync import LdapSync
from .utils import bytes2str
from ldap import SCOPE_SUBTREE

from .utils_pingan import handle_mobfile, SPECIAL_ACCOUNT_TYPE

USE_LDAP_ROLE_LIST_MAPPING = False
try:
    from seahub_custom_functions import ldap_role_mapping
except ImportError:
    def ldap_role_mapping(role):
        return role
try:
    from seahub_custom_functions import ldap_role_list_mapping
    USE_LDAP_ROLE_LIST_MAPPING = True
except ImportError:
    def ldap_role_list_mapping(role_list):
        return role_list[0] if role_list else ''


VIRTUAL_ID_EMAIL_DOMAIN = '@auth.local'


def gen_user_virtual_id():
    return uuid.uuid4().hex[:32] + VIRTUAL_ID_EMAIL_DOMAIN


logger = logging.getLogger('ldap_sync')
logger.setLevel(logging.DEBUG)


class UserObj(object):
    def __init__(self, user_id, email, ctime, is_staff, is_active, role, is_manual_set):
        self.id = user_id
        self.email = email
        self.ctime = ctime
        self.is_staff = is_staff
        self.is_active = is_active
        self.role = role
        self.is_manual_set = is_manual_set


class LdapUser(object):
    def __init__(self, user_id, name, dept, uid, cemail,
                 is_staff=0, is_active=1, role='', is_manual_set=False, role_list=None):
        self.id = user_id
        self.name = name
        self.dept = dept
        self.uid = uid
        self.cemail = cemail
        self.is_staff = is_staff
        self.is_active = is_active
        self.role = role
        self.role_list = role_list
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

        self.users_for_group_add = set()
        self.users_for_group_del = set()

        self.login_attr_email_map = dict()

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
            self.cursor.execute('select is_manually_set_contact_email from profile_profile where user=%s', [email])
            if self.cursor.rowcount == 0:
                self.add_profile(email, ldap_user)
                return
            else:
                profile_res = self.cursor.fetchall()
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
                if profile_res[0][0] == 0 and ldap_user.cemail is not None and db_user.cemail != ldap_user.cemail:
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

    def del_repo_api_token(self, email):
        """del personal repo api tokens (not department)
        """
        try:
            owned_repos = seafile_api.get_owned_repo_list(
                email, ret_corrupted=True)
            owned_repo_ids = [item.repo_id for item in owned_repos]
            if owned_repo_ids:
                sql = 'delete from repo_api_tokens where repo_id in %s'
                self.cursor.execute(sql, [owned_repo_ids])
                if self.cursor.rowcount > 0:
                    logger.debug('Delete repo_api_token from repo_api_tokens for user %s success.' %
                                ( email))
        except Exception as e:
            logger.warning('Failed to delete repo_api_token from repo_api_tokens for user %s: %s.' %
                            ( email, e))

    def get_data_from_db(self):
        # user_id <-> LdapUser
        providers = list()
        for config in self.settings.ldap_configs:
            providers.append(config.ldap_provider)
        user_data_db = None
        try:
            self.cursor.execute("SELECT username,uid FROM social_auth_usersocialauth WHERE `provider` IN %s",
                                [providers])
            ldap_users = self.cursor.fetchall()
        except Exception as e:
            logger.error('get ldap users from db failed: %s' % e)
            return user_data_db

        # get login_attr email map
        for user in ldap_users:
            self.login_attr_email_map[user[1]] = user[0]

        # get ldap users from ccnet by email_list
        email_list = list()
        for user in ldap_users:
            email_list.append(user[0])
        users = list()
        res = list()
        if email_list:
            try:
                self.ccnet_db_cursor.execute("SELECT e.id, e.email, ctime, is_staff, is_active, role, is_manual_set FROM "
                                            "`EmailUser` e LEFT JOIN UserRole r ON e.email=r.email WHERE e.email IN %s",
                                            [email_list])
                res = self.ccnet_db_cursor.fetchall()
            except Exception as e:
                logger.error('get users from ccnet failed: %s' % e)
                return user_data_db
        for user in res:
            users.append(UserObj(user[0], user[1], user[2], user[3], user[4], user[5], user[6]))

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
                return

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

            user_data_db[user.email] = LdapUser(user.id, name, dept,
                                                uid, cemail,
                                                1 if user.is_staff else 0,
                                                1 if user.is_active else 0,
                                                user.role,
                                                user.is_manual_set)
        return user_data_db

    def get_data_from_ldap_by_server(self, config, use_duplicated_user_filter=False, scope_all=False, use_external_user_filter=False):
        if not config.enable_user_sync:
            return {}
        ldap_conn = LdapConn(config.host, config.user_dn, config.passwd, config.follow_referrals)
        ldap_conn.create_conn()
        if not ldap_conn.conn:
            return None

        #  dn <-> LdapUser
        user_data_ldap = {}
        # 扫描 AD 全域用户
        if scope_all:
            search_filter = '(objectClass=%s)' % config.user_object_class
        
        # 使用 自定义 Filter 查询额外的用户
        elif use_duplicated_user_filter:
            
            if config.duplicated_user_filter:
                search_filter = '(&(objectClass=%s)(%s))' % \
                                (config.user_object_class,
                                 config.duplicated_user_filter)
            else:
                return None

        # 使用 外部权限的用户 过滤
        elif use_external_user_filter:
    
            if config.external_user_filter:
                search_filter = '(&(objectClass=%s)(%s))' % \
                                (config.user_object_class,
                                 config.external_user_filter)
            else:
                return None

        # search all users on base dn
        elif config.user_filter != '':
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
        search_attr = [config.login_attr, config.employee_type_attr]

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

            user_name = None
            dept = None
            uid = None
            cemail = None
            role = None
            role_list = None

            if config.role_name_attr not in attrs:
                role = ''
            else:
                role = attrs[config.role_name_attr][0]
                role_list = [role for role in attrs[config.role_name_attr]]

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
                       continue
                   else:
                        uid = attrs[config.uid_attr][0]

                if config.cemail_attr != '':
                   if config.cemail_attr not in attrs:
                       cemail = "%s@virtual.fcloud.com" % uid.strip()
                   else:
                       cemail = attrs[config.cemail_attr][0]

            # pingan custom: 判断用户账号的类型， 如果属于特殊账号或者黑名单，则跳过不同步， 同时移除安全组。
            if config.employee_type_attr in attrs:
               employee_type = attrs[config.employee_type_attr][0]
               if employee_type in SPECIAL_ACCOUNT_TYPE:
                   self.users_for_group_del.add(uid)
                   continue
                   
            user_data_ldap[uid] = LdapUser(None, user_name, dept, uid, cemail, role=role, role_list=role_list)

        return user_data_ldap
    
    def add_or_update_user_role_time(self, email, role):
        self.cursor.execute('select 1 from user_role_update_time where email=%s', [email])
        if self.cursor.rowcount == 0:
            sql = 'insert into user_role_update_time (email, role, update_at) values (%s,%s,%s)'
            try:
                self.cursor.execute(sql, (email, role, datetime.now()))
                if self.cursor.rowcount == 1:
                    logger.debug('Create role time to user %s successs.' % email)
            except Exception as e:
                logger.debug('Failed to create role time to user %s: %s.' % (email, e))
    
        else:
            sql = 'update user_role_update_time set role=%s, update_at=%s where email=%s'
            try:
                self.cursor.execute(sql, (role, datetime.now(), email))
                if self.cursor.rowcount == 1:
                    logger.debug('Update role time to user %s successs.' % email)
            except Exception as e:
                logger.debug('Failed to update role time to user %s: %s.' % (email, e))
    
    def set_external_user(self, email, set_external=False):
        self.cursor.execute('select 1 from ex_repo_user where email=%s', [email])
        if self.cursor.rowcount == 0:
            if set_external:
                sql = 'insert into ex_repo_user (email, created_time, created_by, updated_time, updated_by) values (%s,%s,%s,%s,%s)'
                try:
                    self.cursor.execute(sql, [email, datetime.now(), 'ldap_sync', datetime.now(), 'ladp_sync'])
                    if self.cursor.rowcount == 1:
                        logger.debug('Create external user %s successs.' % email)
                except Exception as e:
                    logger.debug('Failed to create external user %s: %s.' % (email, e))
        else:
            if not set_external:
                sql = 'delete from ex_repo_user where email=%s'
                try:
                    self.cursor.execute(sql, [email])
                    if self.cursor.rowcount == 1:
                        logger.debug('Create external user %s successs.' % email)
                except Exception as e:
                    logger.debug('Failed to create external user %s: %s.' % (email, e))

    def sync_add_user(self, ldap_user, login_attr, set_guest=False, set_external=False):
        virtual_id = gen_user_virtual_id()
        ret = ccnet_api.add_emailuser(virtual_id, '!', 0, 1 if self.settings.activate_user else 0)
        if ret < 0:
            logger.warning('Add user [%s] failed.' % login_attr)
            return
        try:
            self.cursor.execute("INSERT INTO social_auth_usersocialauth (username,provider,uid,extra_data) "
                                "VALUES (%s, %s, %s, %s)", (virtual_id, ldap_user.config.ldap_provider, login_attr, ''))
        except Exception as e:
            logger.error('Add user [%s] to social_auth_usersocialauth failed: %s' % (login_attr, e))
            return
        self.auser += 1
        logger.debug('Add user [%s] success.' % login_attr)

        #pingan custom: 新增用户维护访客角色, 加组
        if set_guest:
            role = 'guest'
            ret = ccnet_api.update_role_emailuser(virtual_id, role, False)
            if ret == 0:
                self.arole += 1
                self.add_or_update_user_role_time(virtual_id, role)
                logger.debug('Add role [%s] for user [%s] success.' % (role, virtual_id))
            if ret < 0:
                logger.warning('Add role [%s] for user [%s] failed.' % (role, virtual_id))
                
            # 加组
            self.users_for_group_add.add(ldap_user.uid)
        
        else:
            if ldap_user.role:
                if not USE_LDAP_ROLE_LIST_MAPPING:
                    role = ldap_role_mapping(ldap_user.role)
                else:
                    role = ldap_role_list_mapping(ldap_user.role_list)
                ret = ccnet_api.update_role_emailuser(virtual_id, role, False)
    
                if ret == 0:
                    self.arole += 1
                    logger.debug('Add role [%s] for user [%s] success.' % (role, login_attr))
    
                if ret < 0:
                    logger.warning('Add role [%s] for user [%s] failed.' % (role, login_attr))

        if ldap_user.config.enable_extra_user_info_sync:
            self.add_profile(virtual_id, ldap_user)
            self.add_dept(virtual_id, ldap_user.dept)

        
        self.set_external_user(virtual_id, set_external)

    def sync_update_user(self, ldap_user, db_user, email, set_guest=False, set_external=False):
        # pingan custom：更新用户维护访客角色, 加组
        if set_guest:
            role = 'guest'
            if not db_user.is_manual_set and db_user.role != role:
                ret = ccnet_api.update_role_emailuser(email, role, False)
                if ret == 0:
                    self.arole += 1
                    self.add_or_update_user_role_time(email, role)
                    logger.debug('Update role [%s] for user [%s] success.' % (role, email))
                if ret < 0:
                    logger.warning('Update role [%s] for user [%s] failed.' % (role, email))
            # 加组
            self.users_for_group_add.add(ldap_user.uid)
        
        else:
            if ldap_user.role:
                if not USE_LDAP_ROLE_LIST_MAPPING:
                    role = ldap_role_mapping(ldap_user.role)
                else:
                    role = ldap_role_list_mapping(ldap_user.role_list)
                if not db_user.is_manual_set and db_user.role != role:
                    ret = ccnet_api.update_role_emailuser(email, role, False)
    
                    if ret == 0:
                        self.urole += 1
                        logger.debug('Update role [%s] for user [%s] success.' % (role, email))
    
                    if ret < 0:
                        logger.warning('Update role [%s] for user [%s] failed.' % (role, email))

        if ldap_user.config.enable_extra_user_info_sync:
            self.update_profile(email, db_user, ldap_user)
            if ldap_user.dept != db_user.dept:
                self.update_dept(email, ldap_user.dept)

        if not db_user.is_active and ldap_user.config.auto_reactivate_users:
            try:
                ret = ccnet_api.update_emailuser('DB', db_user.id, '!', db_user.is_staff, 1)
            except Exception as e:
                logger.error('Reactivate user [{}] failed. ERROR: {}'.format(email, e))
                return

            if ret < 0:
                logger.warning('Reactivate user [%s] failed.' % email)
                return
            logger.debug('Reactivate user [%s] success.' % email)
        
        self.set_external_user(email, set_external)

    def sync_del_user(self, db_user, email):
        """Set user.is_active = False, del tokens and repo tokens
        """
        try:
            ccnet_api.update_emailuser('DB', db_user.id, '!', db_user.is_staff, 0)
        except Exception as e:
            logger.warning('Deactive user [%s] failed: %s' % (email, e))
            return
        logger.debug('Deactive user [%s] success.' % email)
        self.duser += 1

        if self.cursor:
            self.del_token('api2_token', email)
            self.del_token('api2_tokenv2', email)
            self.del_repo_api_token(email)
        else:
            logger.debug('Failed to connect seahub db, omit delete api token for user [%s].' % email)
        try:
            seafile_api.delete_repo_tokens_by_email(email)
            logger.debug('Delete repo tokens for user %s success.', email)
        except Exception as e:
            logger.warning("Failed to delete repo tokens for user %s: %s." % (email, e))

    def sync_data(self, data_db, data_ldap, data_ldap_all, data_ldap_external=None):
        # sync deleted user in ldap to db
        # data_ldap: 安全组用户
        # data_ldap_all: AD 全域用户
        # data_ldap_external: 外部权限用户
        # login_attr_email_map： db中的用户 {:uid: :email}
        for login_attr in self.login_attr_email_map.keys():
            email = self.login_attr_email_map[login_attr]
            if login_attr not in data_ldap_all and data_db[email].is_active == 1:
                if self.settings.enable_deactive_user:
                    self.sync_del_user(data_db[email], email)
                else:
                    logger.debug('User[%s] not found in ldap, '
                                 'DEACTIVE_USER_IF_NOTFOUND option is not set, so not deactive it.' % login_attr)
        # sync undeleted user in ldap to db
        for login_attr, ldap_user in data_ldap_all.items():
            set_guest = False
            set_external = False
            if login_attr not in data_ldap.keys():  # 如果用户不在设置的安全组里
                set_guest = True
            if data_ldap_external and login_attr in data_ldap_external.keys():
                set_external = True
            
            if login_attr in self.login_attr_email_map:
                email = self.login_attr_email_map[login_attr]
                self.sync_update_user(ldap_user, data_db[email], email, set_guest=set_guest, set_external=set_external)
            # if not exists, create a new user
            else:
                if self.settings.import_new_user:
                    self.sync_add_user(ldap_user, login_attr, set_guest=set_guest, set_external=set_external)

        # 调用接口，处理加组，减组
        uids_for_add = list(self.users_for_group_add)
        uids_for_del = list(self.users_for_group_del)
        
        self.add_mobfiles(uids_for_add)
        self.del_mobfiles(uids_for_del)

        self.close_seahub_db()
        self.close_ccnet_db()
    
    def add_mobfiles(self, uids):
        try:
            handle_mobfile(uids, 'checkaddum')
        except Exception as e:
            logger.debug("添加访客安全组出错：uid: %s, error: %s" % (uids[:5], e))
        return
    
    def del_mobfiles(self, uids):
        try:
            handle_mobfile(uids, 'checkdelum')
        except Exception as e:
            logger.debug("移除访客安全组出错：uid: %s, error: %s" % (uids[:5], e))
    
    def cleanup_duplicated_ldap_data(self, data_ldap):
        current_hour = datetime.today().hour
        if current_hour != 6:
            return
        uids = []
        for email, ldap_user_obj in data_ldap.items():
            uids.append(ldap_user_obj.uid)
        
        self.del_mobfiles(uids)
