# -*- coding: utf-8 -*-
import logging

from seafevents.db import init_db_session_class
from seafevents.app.config import seahub_settings

MULTI_LDAP_SETTING_PREFIX = 'MULTI_LDAP_1'


class LdapConfig(object):
    def __init__(self):
        self.host = None
        self.base_dn = None
        self.user_dn = None
        self.passwd = None
        self.login_attr = None
        self.ldap_provider = None
        self.use_page_result = False
        self.follow_referrals = True

        self.enable_group_sync = False
        self.enable_user_sync = False

        self.user_filter = None
        self.import_new_user = True
        self.user_object_class = None
        self.enable_extra_user_info_sync = False
        self.first_name_attr = None
        self.last_name_attr = None
        self.name_reverse = False
        self.dept_attr = None
        self.uid_attr = None
        self.cemail_attr = None
        self.role_name_attr = None
        self.auto_reactivate_users = False

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
        self.department_name_attr = None


class Settings(object):
    def __init__(self, config, is_test=False):
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
        self.db_session = init_db_session_class(config)
        if not self.get_option('ENABLE_LDAP', False) and not self.get_option('ENABLE_MULTI_LDAP', False):
            if is_test:
                logging.info('LDAP is not set, stop ldap test.')
            else:
                logging.info('LDAP is not set, disable ldap sync.')
            return

        self.read_common_config()
        self.read_multi_server_configs(is_test)

        # If enable_extra_user_info_sync, uid_attr, cemail_attr were configed in any of ldap configs,
        # load extra_user_info, uid_attr, cemail_attr from database to memory.
        for ldap_config in self.ldap_configs:
            if ldap_config.enable_extra_user_info_sync is True:
                self.load_extra_user_info_sync = True
            if ldap_config.uid_attr != '':
                self.load_uid_attr = True
            if ldap_config.cemail_attr != '':
                self.load_cemail_attr = True

    def read_common_config(self):
        self.sync_interval = self.get_option('LDAP_SYNC_INTERVAL', 60)
        self.del_group_if_not_found = self.get_option('DEL_GROUP_IF_NOT_FOUND', False)
        self.del_department_if_not_found = self.get_option('DEL_DEPARTMENT_IF_NOT_FOUND', False)
        self.enable_deactive_user = self.get_option('DEACTIVE_USER_IF_NOTFOUND', False)
        self.activate_user = self.get_option('ACTIVATE_USER_WHEN_IMPORT', True)
        self.import_new_user = self.get_option('IMPORT_NEW_USER', True)

    def read_multi_server_configs(self, is_test):
        for i in range(2):
            enable_multi_ldap = False
            if i == 1:
                if not self.get_option('ENABLE_MULTI_LDAP', False):
                    return
                enable_multi_ldap = True

            ldap_config = LdapConfig()
            if self.read_base_config(ldap_config, is_test, enable_multi_ldap) == -1:
                return

            if ldap_config.enable_user_sync:
                self.read_sync_user_config(ldap_config, enable_multi_ldap)
                self.enable_user_sync = True

            if ldap_config.enable_group_sync or ldap_config.sync_department_from_ou:
                self.read_sync_group_config(ldap_config, enable_multi_ldap)
                if ldap_config.enable_group_sync:
                    self.enable_group_sync = True
                if ldap_config.sync_department_from_ou:
                    self.sync_department_from_ou = True

            self.ldap_configs.append(ldap_config)

    def read_base_config(self, ldap_config, is_test, enable_multi_ldap=False):
        setting_prefix = MULTI_LDAP_SETTING_PREFIX if enable_multi_ldap else 'LDAP'
        ldap_config.host = self.get_option('LDAP_SERVER_URL'.replace('LDAP', setting_prefix, 1), '')
        ldap_config.base_dn = self.get_option('LDAP_BASE_DN'.replace('LDAP', setting_prefix, 1), '')
        ldap_config.user_dn = self.get_option('LDAP_ADMIN_DN'.replace('LDAP', setting_prefix, 1), '')
        ldap_config.passwd = self.get_option('LDAP_ADMIN_PASSWORD'.replace('LDAP', setting_prefix, 1), '')
        ldap_config.login_attr = self.get_option('LDAP_LOGIN_ATTR'.replace('LDAP', setting_prefix, 1), 'mail')
        ldap_config.ldap_provider = self.get_option('LDAP_PROVIDER'.replace('LDAP', setting_prefix, 1),
                                                    'ldap1' if enable_multi_ldap else 'ldap')
        ldap_config.user_filter = self.get_option('LDAP_FILTER'.replace('LDAP', setting_prefix, 1), '')
        ldap_config.use_page_result = self.get_option('LDAP_USE_PAGED_RESULT'.replace('LDAP', setting_prefix, 1), False)
        ldap_config.follow_referrals = self.get_option('LDAP_FOLLOW_REFERRALS'.replace('LDAP', setting_prefix, 1), True)

        if ldap_config.host == '' or ldap_config.user_dn == '' or ldap_config.passwd == '' or ldap_config.base_dn == '':
            if is_test:
                logging.warning('LDAP info is not set completely in seahub_settings.py, stop ldap test.')
            else:
                logging.warning('LDAP info is not set completely in seahub_settings.py, disable ldap sync.')
            self.has_base_info = False
            return -1

        self.has_base_info = True

        if ldap_config.login_attr != 'mail' and ldap_config.login_attr != 'userPrincipalName':
            if is_test:
                logging.warning("LDAP login attr is not mail or userPrincipalName")

        ldap_config.enable_user_sync = self.get_option(
            'ENABLE_LDAP_USER_SYNC'.replace('LDAP', setting_prefix, 1), False)
        ldap_config.enable_group_sync = self.get_option(
            'ENABLE_LDAP_GROUP_SYNC'.replace('LDAP', setting_prefix, 1), False)
        ldap_config.sync_department_from_ou = self.get_option(
            'LDAP_SYNC_DEPARTMENT_FROM_OU'.replace('LDAP', setting_prefix, 1), False)

    def read_sync_group_config(self, ldap_config, enable_multi_ldap=False):
        setting_prefix = MULTI_LDAP_SETTING_PREFIX if enable_multi_ldap else 'LDAP'
        ldap_config.group_object_class = self.get_option(
            'LDAP_GROUP_OBJECT_CLASS'.replace('LDAP', setting_prefix, 1), 'group')
        ldap_config.group_filter = self.get_option(
            'LDAP_GROUP_FILTER'.replace('LDAP', setting_prefix, 1), '')
        ldap_config.group_member_attr = self.get_option(
            'LDAP_GROUP_MEMBER_ATTR'.replace('LDAP', setting_prefix, 1), 'member')
        ldap_config.group_uuid_attr = self.get_option(
            'LDAP_GROUP_UUID_ATTR'.replace('LDAP', setting_prefix, 1), 'objectGUID')
        ldap_config.create_department_library = self.get_option(
            'LDAP_CREATE_DEPARTMENT_LIBRARY'.replace('LDAP', setting_prefix, 1), False)
        ldap_config.department_repo_permission = self.get_option(
            'LDAP_DEPT_REPO_PERM'.replace('LDAP', setting_prefix, 1), 'rw')
        ldap_config.default_department_quota = self.get_option(
            'LDAP_DEFAULT_DEPARTMENT_QUOTA'.replace('LDAP', setting_prefix, 1), -2)
        ldap_config.sync_group_as_department = self.get_option(
            'LDAP_SYNC_GROUP_AS_DEPARTMENT'.replace('LDAP', setting_prefix, 1), False)
        ldap_config.use_group_member_range_query = self.get_option(
            'LDAP_USE_GROUP_MEMBER_RANGE_QUERY'.replace('LDAP', setting_prefix, 1), False)
        '''
        posix groups store members in atrribute 'memberUid', however, the value of memberUid may be not a 'uid',
        so we make it configurable, default value is 'uid'.
        '''
        ldap_config.user_attr_in_memberUid = self.get_option(
            'LDAP_USER_ATTR_IN_MEMBERUID'.replace('LDAP', setting_prefix, 1), 'uid')
        ldap_config.department_name_attr = self.get_option(
            'LDAP_DEPT_NAME_ATTR'.replace('LDAP', setting_prefix, 1), '')

    def read_sync_user_config(self, ldap_config, enable_multi_ldap=False):
        setting_prefix = MULTI_LDAP_SETTING_PREFIX if enable_multi_ldap else 'LDAP'
        ldap_config.user_object_class = self.get_option(
            'LDAP_USER_OBJECT_CLASS'.replace('LDAP', setting_prefix, 1), 'person')
        if not enable_multi_ldap:
            ldap_config.enable_extra_user_info_sync = self.get_option(
                'ENABLE_EXTRA_USER_INFO_SYNC', True)
        else:
            ldap_config.enable_extra_user_info_sync = self.get_option(
                'ENABLE_MULTI_LDAP_1_EXTRA_USER_INFO_SYNC', True)
        ldap_config.first_name_attr = self.get_option(
            'LDAP_USER_FIRST_NAME_ATTR'.replace('LDAP', setting_prefix, 1), 'givenName')
        ldap_config.last_name_attr = self.get_option(
            'LDAP_USER_LAST_NAME_ATTR'.replace('LDAP', setting_prefix, 1), 'sn')
        ldap_config.name_reverse = self.get_option(
            'LDAP_USER_NAME_REVERSE'.replace('LDAP', setting_prefix, 1), False)
        ldap_config.dept_attr = self.get_option(
            'LDAP_DEPT_ATTR'.replace('LDAP', setting_prefix, 1), 'department')
        ldap_config.uid_attr = self.get_option(
            'LDAP_UID_ATTR'.replace('LDAP', setting_prefix, 1), '')
        ldap_config.cemail_attr = self.get_option(
            'LDAP_CONTACT_EMAIL_ATTR'.replace('LDAP', setting_prefix, 1), '')
        ldap_config.role_name_attr = self.get_option(
            'LDAP_USER_ROLE_ATTR'.replace('LDAP', setting_prefix, 1), '')
        ldap_config.auto_reactivate_users = self.get_option(
            'LDAP_AUTO_REACTIVATE_USERS'.replace('LDAP', setting_prefix, 1), False)

    def enable_sync(self):
        return self.enable_user_sync or self.enable_group_sync or self.sync_department_from_ou

    def get_option(self, name, default):
        return getattr(seahub_settings, name, default)
