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

def search_login_attr(settings, config, ldap_conn):
    logging.debug('Try to search login attribute [%s].' % settings.login_attr)
    if config.user_filter != '':
        logging.debug('Using filter [%s].' % config.user_filter)
        search_filter = '(&(%s=*)(%s))' % (settings.login_attr, config.user_filter)
    else:
        search_filter = '(%s=*)' % settings.login_attr

    base_dns = config.base_dn.split(';')
    for base_dn in base_dns:
        if base_dn == '':
            continue
        if settings.use_page_result:
            logging.debug('Search paged result from dn [%s], and try to print ten records:' %  base_dn)
            users = ldap_conn.paged_search(base_dn, SCOPE_SUBTREE,
                                           search_filter,
                                           [settings.login_attr])
        else:
            logging.debug('Search result from dn [%s], and try to print ten records:' %  base_dn)
            users = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                     search_filter,
                                     [settings.login_attr])
        if users is None:
            logging.debug('Search failed, please check whether dn [%s] is valid.' % base_dn)
            continue

        print_search_result(users)

def search_group(settings, config, ldap_conn):
    logging.debug('LDAP group sync is enabled, '
                  'try to search groups using group object class [%s].',
                  settings.group_object_class)

    if config.group_filter != '':
        logging.debug('Using filter [%s].' % config.group_filter)
        search_filter = '(&(objectClass=%s)(%s))' % \
                (settings.group_object_class,
                 config.group_filter)
    else:
        search_filter = '(objectClass=%s)' % settings.group_object_class

    base_dns = config.base_dn.split(';')
    for base_dn in base_dns:
        if base_dn == '':
            continue

        if settings.use_page_result:
            logging.debug('Search paged result from dn [%s], and try to print ten records:' %  base_dn)
            groups = ldap_conn.paged_search(base_dn, SCOPE_SUBTREE,
                                            search_filter, ['cn', settings.group_member_attr])
        else:
            logging.debug('Search result from dn [%s], and try to print ten records:' %  base_dn)
            groups = ldap_conn.search(base_dn, SCOPE_SUBTREE,
                                      search_filter, ['cn', settings.group_member_attr])

        if groups is None:
            logging.debug('Search failed, please check whether dn [%s] is valid.' % base_dn)
            continue

        print_search_result(groups)

def test_ldap(settings):
    for config in settings.ldap_configs:
        logging.debug('Try to connect ldap server %s.', config.host)
        ldap_conn = LdapConn(config.host, config.user_dn, config.passwd, settings.follow_referrals)
        ldap_conn.create_conn()
        if ldap_conn.conn is None:
            continue
        logging.debug('Connect ldap server [%s] success with user_dn [%s] password [*****].' %
                      (config.host, config.user_dn))

        search_login_attr(settings, config, ldap_conn)

        if settings.enable_group_sync:
            search_group(settings, config, ldap_conn)

        ldap_conn.unbind_conn()
        logging.debug('')

def run_ldap_sync(settings):
    if not settings.enable_sync():
        logging.debug('Both user and group sync are disabled, stop ldap sync.')
        return

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
