#coding: utf-8

import os
import logging
import configparser
from seafevents.app.config import appconfig

MAX_LDAP_NUM = 10

class LdapConfig(object):
    def __init__(self):
        self.host = None
        self.base_dn = None
        self.user_dn = None
        self.passwd = None
        self.login_attr = None
        self.use_page_result = False
        self.follow_referrals = True

        self.enable_group_sync = False
        self.enable_user_sync = False

        self.user_filter = None
        self.import_new_user = True
        self.user_object_class = None
        self.pwd_change_attr = None
        self.enable_extra_user_info_sync = False
        self.first_name_attr = None
        self.last_name_attr = None
        self.name_reverse = False
        self.dept_attr = None
        self.uid_attr = None
        self.cemail_attr = None
        self.role_name_attr = None

        self.group_filter = None
        self.group_object_class = None
        self.group_member_attr = None
        self.group_uuid_attr = None
        self.use_group_member_range_query = False
        self.user_attr_in_memberUid = None

        self.create_department_library = False
        self.sync_department_from_ou = False
        self.default_department_quota = -2
        self.department_repo_permission = None

        self.sync_group_as_department = False
        self.department_repo_permission = None
        self.department_name_attr = None

class Settings(object):
    def __init__(self, is_test=False):
        # If any of ldap configs allows user-sync/group-sync, user-sync/group-sync task is allowed.
        self.enable_group_sync = False
        self.enable_user_sync = False
        self.sync_department_from_ou = False

        # Common configs which only take effect at [LDAP_SYNC] section.
        self.sync_interval = 0
        self.del_group_if_not_found = False
        self.del_department_if_not_found = False
        self.enable_deactive_user = False
        self.activate_user = True
        self.import_new_user = True

        # Only all server configs have base info so can we do ldap sync or test.
        self.has_base_info = False

        # Decide whether load extra_user_info from database or not.
        self.load_extra_user_info_sync = False
        self.load_uid_attr = False
        self.load_cemail_attr = False

        self.ldap_configs = []

        if appconfig.get('ccnet_conf_path'):
            ccnet_conf_path = appconfig.ccnet_conf_path
        else:
            if is_test:
                ccnet_conf_dir = os.environ.get('CCNET_CONF_DIR')
                if ccnet_conf_dir:
                    ccnet_conf_path = os.path.join(ccnet_conf_dir, 'ccnet.conf')
                else:
                    logging.warning('Environment variable CCNET_CONF_DIR and SEAFILE_CENTRAL_CONF_DIR is not define, stop ldap test.')
                    return
            else:
                logging.warning('Environment variable CCNET_CONF_DIR and SEAFILE_CENTRAL_CONF_DIR is not define, disable ldap sync.')
                return

        self.parser = configparser.ConfigParser()
        self.parser.read(ccnet_conf_path)

        if not self.parser.has_section('LDAP'):
            if is_test:
                logging.info('LDAP section is not set, stop ldap test.')
            else:
                logging.info('LDAP section is not set, disable ldap sync.')
            return

        # We can run test without [LDAP_SYNC] section
        has_sync_section = True
        if not self.parser.has_section('LDAP_SYNC'):
            if not is_test:
                logging.info('LDAP_SYNC section is not set, disable ldap sync.')
                return
            else:
                has_sync_section = False

        if has_sync_section:
            self.read_common_config(is_test)

        self.read_multi_server_configs(is_test, has_sync_section)

        # If enable_extra_user_info_sync, uid_attr, cemail_attr were configed in any of ldap configs,
        # load extra_user_info, uid_attr, cemail_attr from database to memory.
        for config in self.ldap_configs:
            if config.enable_extra_user_info_sync == True:
                self.load_extra_user_info_sync = True
            if config.uid_attr != '':
                self.load_uid_attr = True
            if config.cemail_attr != '':
                self.load_cemail_attr = True

    def read_common_config(self, is_test):
        self.sync_interval = self.get_option('LDAP_SYNC', 'SYNC_INTERVAL', int, 60)
        self.del_group_if_not_found = self.get_option('LDAP_SYNC', 'DEL_GROUP_IF_NOT_FOUND', bool, False)
        self.del_department_if_not_found = self.get_option('LDAP_SYNC', 'DEL_DEPARTMENT_IF_NOT_FOUND', bool, False)
        self.enable_deactive_user = self.get_option('LDAP_SYNC', 'DEACTIVE_USER_IF_NOTFOUND', bool, False)
        self.activate_user = self.get_option('LDAP_SYNC', 'ACTIVATE_USER_WHEN_IMPORT', bool, True)
        self.import_new_user = self.get_option('LDAP_SYNC', 'IMPORT_NEW_USER', bool, True)

    def read_multi_server_configs(self, is_test, has_sync_section=True):
        for i in range(0, MAX_LDAP_NUM):
            ldap_sec = 'LDAP' if i==0 else 'LDAP_MULTI_%d' % i
            sync_sec = 'LDAP_SYNC' if i==0 else 'LDAP_SYNC_MULTI_%d' % i
            if not self.parser.has_section(ldap_sec):
                break
            # If [LDAP_MULTI_1] was configed but no [LDAP_SYNC_MULTI_1], use [LDAP_SYNC] section for this server.
            if not self.parser.has_section(sync_sec):
                sync_sec = 'LDAP_SYNC'

            ldap_config = LdapConfig()
            if self.read_base_config(ldap_config, ldap_sec, sync_sec, is_test, has_sync_section) == -1:
                return

            if not has_sync_section:
                self.ldap_configs.append(ldap_config)
                continue

            if ldap_config.enable_user_sync:
                self.read_sync_user_config(ldap_config, ldap_sec, sync_sec)
                self.enable_user_sync = True

            if ldap_config.enable_group_sync or ldap_config.sync_department_from_ou:
                self.read_sync_group_config(ldap_config, ldap_sec, sync_sec)
                if ldap_config.enable_group_sync:
                    self.enable_group_sync = True
                if ldap_config.sync_department_from_ou:
                    self.sync_department_from_ou = True

            self.ldap_configs.append(ldap_config)

    def read_base_config(self, ldap_config, ldap_sec, sync_sec, is_test, has_sync_section=True):
        ldap_config.host = self.get_option(ldap_sec, 'HOST')
        ldap_config.base_dn = self.get_option(ldap_sec, 'BASE')
        ldap_config.user_dn = self.get_option(ldap_sec, 'USER_DN')
        ldap_config.passwd = self.get_option(ldap_sec, 'PASSWORD')
        ldap_config.login_attr = self.get_option(ldap_sec, 'LOGIN_ATTR', dval='mail')
        ldap_config.use_page_result = self.get_option(ldap_sec, 'USE_PAGED_RESULT', bool, False)
        ldap_config.follow_referrals = self.get_option(ldap_sec, 'FOLLOW_REFERRALS', bool, True)
        ldap_config.user_filter = self.get_option(ldap_sec, 'FILTER')
        ldap_config.group_filter = self.get_option(ldap_sec, 'GROUP_FILTER')

        if ldap_config.host == '' or ldap_config.user_dn == '' or ldap_config.passwd == '' or ldap_config.base_dn == '':
            if is_test:
                logging.warning('LDAP info is not set completely in [%s], stop ldap test.', ldap_sec)
            else:
                logging.warning('LDAP info is not set completely in [%s], disable ldap sync.', ldap_sec)
            self.has_base_info = False
            return -1

        self.has_base_info = True

        if ldap_config.login_attr != 'mail' and ldap_config.login_attr != 'userPrincipalName':
            if is_test:
                logging.warning("LDAP login attr is not mail or userPrincipalName")

        if not has_sync_section:
            return

        ldap_config.enable_group_sync = self.get_option(sync_sec, 'ENABLE_GROUP_SYNC',
                                                        bool, False)
        ldap_config.enable_user_sync = self.get_option(sync_sec, 'ENABLE_USER_SYNC',
                                                        bool, False)
        ldap_config.sync_department_from_ou = self.get_option(sync_sec, 'SYNC_DEPARTMENT_FROM_OU',
                                                              bool, False)
    def read_sync_group_config(self, ldap_config, ldap_sec, sync_sec):
        ldap_config.group_object_class = self.get_option(sync_sec, 'GROUP_OBJECT_CLASS', dval='group')

        # If GROUP_FILTER is not set in server level, use value of LDAP_SYNC section
        if not ldap_config.group_filter:
            ldap_config.group_filter = self.get_option(sync_sec, 'GROUP_FILTER')

        ldap_config.group_member_attr = self.get_option(sync_sec,
                                                 'GROUP_MEMBER_ATTR',
                                                 dval='member')
        ldap_config.group_uuid_attr = self.get_option(sync_sec,
                                               'GROUP_UUID_ATTR',
                                                dval='objectGUID')
        ldap_config.user_object_class = self.get_option(sync_sec, 'USER_OBJECT_CLASS',
                                                 dval='person')
        ldap_config.create_department_library = self.get_option(sync_sec,
                                                         'CREATE_DEPARTMENT_LIBRARY',
                                                         bool, False)
        ldap_config.department_repo_permission = self.get_option(sync_sec, 'DEPT_REPO_PERM' ,dval='rw')

        ldap_config.default_department_quota = self.get_option(sync_sec,
                                                        'DEFAULT_DEPARTMENT_QUOTA',
                                                        int, -2)

        ldap_config.sync_group_as_department = self.get_option(sync_sec,
                                                        'SYNC_GROUP_AS_DEPARTMENT',
                                                        bool, False)
        ldap_config.use_group_member_range_query = self.get_option(sync_sec,
                                                        'USE_GROUP_MEMBER_RANGE_QUERY',
                                                        bool, False)
        '''
        posix groups store members in atrribute 'memberUid', however, the value of memberUid may be not a 'uid',
        so we make it configurable, default value is 'uid'.
        '''
        ldap_config.user_attr_in_memberUid = self.get_option(sync_sec, 'USER_ATTR_IN_MEMBERUID',dval='uid')
        ldap_config.department_name_attr = self.get_option(sync_sec, 'DEPT_NAME_ATTR')

        ldap_config.department_repo_permission = self.get_option(sync_sec, 'DEPT_REPO_PERM' ,dval='rw')

    def read_sync_user_config(self, ldap_config, ldap_sec, sync_sec):
        ldap_config.user_object_class = self.get_option(sync_sec, 'USER_OBJECT_CLASS',
                                                 dval='person')
        # If USER_FILTER is not set in server level, use value of LDAP_SYNC section
        if not ldap_config.user_filter:
            ldap_config.user_filter = self.get_option(sync_sec, 'USER_FILTER')
        ldap_config.pwd_change_attr = self.get_option(sync_sec, 'PWD_CHANGE_ATTR',
                                               dval='pwdLastSet')

        ldap_config.enable_extra_user_info_sync = self.get_option(sync_sec, 'ENABLE_EXTRA_USER_INFO_SYNC',
                                                           bool, False)
        ldap_config.first_name_attr = self.get_option(sync_sec, 'FIRST_NAME_ATTR',
                                               dval='givenName')
        ldap_config.last_name_attr = self.get_option(sync_sec, 'LAST_NAME_ATTR',
                                              dval='sn')
        ldap_config.name_reverse = self.get_option(sync_sec, 'USER_NAME_REVERSE',
                                            bool, False)
        ldap_config.dept_attr = self.get_option(sync_sec, 'DEPT_ATTR',
                                         dval='department')
        ldap_config.uid_attr = self.get_option(sync_sec, 'UID_ATTR')
        ldap_config.cemail_attr = self.get_option(sync_sec, 'CONTACT_EMAIL_ATTR')
        ldap_config.role_name_attr = self.get_option(sync_sec, 'ROLE_NAME_ATTR', dval='')
        ldap_config.auto_reactivate_users = self.get_option(sync_sec, 'AUTO_REACTIVATE_USERS', bool, False)

    def enable_sync(self):
        return self.enable_user_sync or self.enable_group_sync or self.sync_department_from_ou

    def get_option(self, section, key, dtype=None, dval=''):
        try:
            val = self.parser.get(section, key)
            if dtype:
                val = self.parser.getboolean(section, key) \
                        if dtype == bool else dtype(val)
                return val
        except configparser.NoOptionError:
            return dval
        except ValueError:
            return dval
        return val if val != '' else dval
