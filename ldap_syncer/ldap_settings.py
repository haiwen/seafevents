#coding: utf-8

import os
import logging
import ConfigParser

class Settings(object):
    def __init__(self):
        self.host = None
        self.user_dn = None
        self.passwd = None
        self.base_dn = None
        self.login_attr = None
        self.use_page_result = False

        self.sync_interval = 0
        self.enable_group_sync = False
        self.enable_user_sync = False
        self.group_object_class = None
        self.user_object_class = None

        self.user_filter = None
        self.pwd_change_attr = None

        self.parser = None

        self.read_config()

    def read_config(self):
        try:
            ccnet_conf_path = os.path.join(os.environ['CCNET_CONF_DIR'],
                                           'ccnet.conf')
        except KeyError as e:
            logging.warning('environment variable CCNET_CONF_DIR is not define')
            return

        self.parser = ConfigParser.ConfigParser()
        self.parser.read(ccnet_conf_path)

        if not self.parser.has_section('LDAP'):
            logging.info('LDAP section is not set, disable ldap sync')
            return
        if not self.parser.has_section('LDAP_SYNC'):
            logging.info('LDAP_SYNC section is not set, disable ldap sync')
            return

        self.enable_group_sync = self.get_option('LDAP_SYNC', 'ENABLE_GROUP_SYNC',
                                                 bool, False)
        self.enable_user_sync = self.get_option('LDAP_SYNC', 'ENABLE_USER_SYNC',
                                                bool, False)
        if not self.enable_user_sync and not self.enable_group_sync:
            return

        self.host = self.get_option('LDAP', 'HOST')
        if self.host == '':
            logging.info('ldap host option is not set, disable ldap sync')
            return

        self.user_dn = self.get_option('LDAP', 'USER_DN')
        self.passwd = self.get_option('LDAP', 'PASSWORD')
        self.base_dn = self.get_option('LDAP', 'BASE')
        self.login_attr = self.get_option('LDAP', 'LOGIN_ATTR', dval='mail')
        self.use_page_result = self.get_option('LDAP', 'USE_PAGED_RESULT', bool, False)

        self.sync_interval = self.get_option('LDAP_SYNC', 'SYNC_INTERVAL', int, 30*60)
        self.group_object_class = self.get_option('LDAP_SYNC', 'GROUP_OBJECT_CLASS',
                                                  dval='group')
        self.group_filter = self.get_option('LDAP_SYNC',
                                            'GROUP_FILTER')
        self.group_member_attr = self.get_option('LDAP_SYNC',
                                                 'GROUP_MEMBER_ATTR',
                                                 dval='member')

        self.user_object_class = self.get_option('LDAP_SYNC', 'USER_OBJECT_CLASS',
                                                 dval='person')
        self.user_filter = self.get_option('LDAP_SYNC', 'USER_FILTER')
        self.pwd_change_attr = self.get_option('LDAP_SYNC', 'PWD_CHANGE_ATTR',
                                               dval='pwdLastSet')

    def enable_sync(self):
        return self.enable_user_sync or self.enable_group_sync

    def get_option(self, section, key, dtype=None, dval=''):
        try:
            val = self.parser.get(section, key)
            if dtype:
                val = dtype(val)
                return val
        except ConfigParser.NoOptionError:
            return dval
        except ValueError:
            return dval
        return val if val != '' else dval
