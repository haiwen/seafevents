#coding: utf-8

import os
import logging
import ConfigParser

class Settings(object):
    def __init__(self, is_test=False):
        self.host = None
        self.user_dn = None
        self.passwd = None
        self.base_dn = None
        self.login_attr = None
        self.use_page_result = False

        self.sync_interval = 0
        self.enable_group_sync = False
        self.enable_user_sync = False
        self.import_new_user = True
        self.group_object_class = None
        self.user_object_class = None

        self.user_filter = None
        self.pwd_change_attr = None

        self.enable_extra_user_info_sync = False
        self.first_name_attr = None
        self.last_name_attr = None
        self.name_reverse = False
        self.dept_attr = None

        self.parser = None
        self.is_test = is_test
        self.has_base_info = False

        self.read_config()

    def read_config(self):
        if 'SEAFILE_CENTRAL_CONF_DIR' in os.environ:
            confdir = os.environ['SEAFILE_CENTRAL_CONF_DIR']
        elif 'CCNET_CONF_DIR' in os.environ:
            confdir = os.environ['CCNET_CONF_DIR']
        else:
            if self.is_test:
                logging.warning('Environment variable CCNET_CONF_DIR and SEAFILE_CENTRAL_CONF_DIR is not define, stop ldap test.')
            else:
                logging.warning('Environment variable CCNET_CONF_DIR and SEAFILE_CENTRAL_CONF_DIR is not define, disable ldap sync.')
            return
        ccnet_conf_path = os.path.join(confdir, 'ccnet.conf')

        self.parser = ConfigParser.ConfigParser()
        self.parser.read(ccnet_conf_path)

        if not self.parser.has_section('LDAP'):
            if self.is_test:
                logging.info('LDAP section is not set, stop ldap test.')
            else:
                logging.info('LDAP section is not set, disable ldap sync.')
            return

        self.host = self.get_option('LDAP', 'HOST')
        self.user_dn = self.get_option('LDAP', 'USER_DN')
        self.passwd = self.get_option('LDAP', 'PASSWORD')
        self.base_dn = self.get_option('LDAP', 'BASE')
        if self.host == '' or self.user_dn == '' or self.passwd == '' or self.base_dn == '':
            if self.is_test:
                logging.info('Ldap option is not set completely, stop ldap test.')
            else:
                logging.info('Ldap option is not set completely, disable ldap sync.')
            return
        self.login_attr = self.get_option('LDAP', 'LOGIN_ATTR', dval='mail')
        self.user_filter = self.get_option('LDAP', 'FILTER')
        self.use_page_result = self.get_option('LDAP', 'USE_PAGED_RESULT', bool, False)
        self.has_base_info = True

        if self.is_test:
            return

        if not self.parser.has_section('LDAP_SYNC'):
            logging.info('LDAP_SYNC section is not set, disable ldap sync.')
            return

        self.enable_group_sync = self.get_option('LDAP_SYNC', 'ENABLE_GROUP_SYNC',
                                                 bool, False)
        self.enable_user_sync = self.get_option('LDAP_SYNC', 'ENABLE_USER_SYNC',
                                                bool, False)
        if not self.enable_user_sync and not self.enable_group_sync:
            return

        self.sync_interval = self.get_option('LDAP_SYNC', 'SYNC_INTERVAL', int, 60)
        self.group_object_class = self.get_option('LDAP_SYNC', 'GROUP_OBJECT_CLASS',
                                                  dval='group')
        self.group_filter = self.get_option('LDAP_SYNC',
                                            'GROUP_FILTER')
        self.group_member_attr = self.get_option('LDAP_SYNC',
                                                 'GROUP_MEMBER_ATTR',
                                                 dval='member')

        self.import_new_user = self.get_option('LDAP_SYNC', 'IMPORT_NEW_USER', bool, True)
        self.user_object_class = self.get_option('LDAP_SYNC', 'USER_OBJECT_CLASS',
                                                 dval='person')
        self.pwd_change_attr = self.get_option('LDAP_SYNC', 'PWD_CHANGE_ATTR',
                                               dval='pwdLastSet')

        self.enable_extra_user_info_sync = self.get_option('LDAP_SYNC', 'ENABLE_EXTRA_USER_INFO_SYNC',
                                                           bool, False)
        self.first_name_attr = self.get_option('LDAP_SYNC', 'FIRST_NAME_ATTR',
                                               dval='givenName')
        self.last_name_attr = self.get_option('LDAP_SYNC', 'LAST_NAME_ATTR',
                                              dval='sn')
        self.name_reverse = self.get_option('LDAP_SYNC', 'USER_NAME_REVERSE',
                                            bool, False)
        self.dept_attr = self.get_option('LDAP_SYNC', 'DEPT_ATTR',
                                         dval='department')

    def enable_sync(self):
        return self.enable_user_sync or self.enable_group_sync

    def get_option(self, section, key, dtype=None, dval=''):
        try:
            val = self.parser.get(section, key)
            if dtype:
                val = self.parser.getboolean(section, key) \
                        if dtype == bool else dtype(val)
                return val
        except ConfigParser.NoOptionError:
            return dval
        except ValueError:
            return dval
        return val if val != '' else dval
