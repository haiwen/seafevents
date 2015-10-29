#!/usr/bin/env python
#coding: utf-8

import logging
import sys
import argparse
from ldap import SCOPE_SUBTREE

from ldap_settings import Settings
from ldap_conn import LdapConn
from ldap_group_sync import LdapGroupSync
from ldap_user_sync import LdapUserSync

def print_search_result(records):
    if len(records) > 0:
        n = 0
        for record in records:
            dn, attrs = record
            logging.debug('%s: %s' % (dn, attrs))
            n += 1
            if n == 10:
                break
    else:
        logging.debug('No record found.')

def search_user(settings, ldap_conn):
    logging.debug('User sync is enabled, try to search users with object class [%s].' %
                  settings.user_object_class)

    if settings.user_filter != '':
        logging.debug('Using filter [%s].' % settings.user_filter)
        search_filter = '(&(objectClass=%s)(%s))' % \
                         (settings.user_object_class,
                          settings.user_filter)
    else:
        search_filter = '(objectClass=%s)' % settings.user_object_class

    base_dns = settings.base_dn.split(';')
    for base_dn in base_dns:
        if base_dn == '':
            continue
        logging.debug('Search result from dn [%s], and try to print ten records:' %  base_dn)
        users = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                 search_filter,
                                 [settings.login_attr,
                                  settings.pwd_change_attr,
                                  settings.first_name_attr,
                                  settings.last_name_attr,
                                  settings.dept_attr])
        if users is None:
            logging.debug('Search failed, please check whether dn [%s] is valid.' % base_dn)
            continue

        print_search_result(users)

def search_group(settings, ldap_conn):
    logging.debug('Group sync is enabled, try to search groups with object class [%s].' %
                  settings.group_object_class)

    if settings.group_filter != '':
        logging.debug('Using filter [%s].', settings.group_filter)
        search_filter = '(&(objectClass=%s)(%s))' % \
                         (settings.group_object_class,
                          settings.group_filter)
    else:
        search_filter = '(objectClass=%s)' % settings.group_object_class

    base_dns = settings.base_dn.split(';')
    for base_dn in base_dns:
        if base_dn == '':
            continue
        logging.debug('Search result from dn [%s], and try to print ten records:' % base_dn)
        groups = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                  search_filter,
                                  [settings.group_member_attr, 'cn'])
        if groups is None:
            logging.debug('Search failed, please check whether dn [%s] is valid.' % base_dn)
            continue

        print_search_result(groups)

def test_ldap(settings):
    logging.debug('Try to connect ldap server.')
    ldap_conn = LdapConn(settings.host, settings.user_dn, settings.passwd)
    ldap_conn.create_conn()
    if ldap_conn.conn is None:
        return
    logging.debug('Connect ldap server [%s] success with user_dn [%s] password [%s].' %
                  (settings.host, settings.user_dn, settings.passwd))

    if settings.enable_user_sync:
        search_user(settings, ldap_conn)

    if settings.enable_group_sync:
        search_group(settings, ldap_conn)

    ldap_conn.unbind_conn()

def run_ldap_sync(settings):
    if settings.enable_group_sync:
        LdapGroupSync(settings).start()

    if settings.enable_user_sync:
        LdapUserSync(settings).start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--test', action='store_true')
    arg = parser.parse_args()
    kw = {
        'format': '[%(asctime)s] [%(levelname)s] %(message)s',
        'datefmt': '%m/%d/%Y %H:%M:%S',
        'level': logging.DEBUG,
        'stream': sys.stdout
    }
    logging.basicConfig(**kw)

    setting = Settings(True if arg.test else False)
    if not setting.has_base_info:
        sys.exit()

    if arg.test:
        test_ldap(setting)
    else:
        run_ldap_sync(setting)
