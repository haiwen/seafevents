#!/usr/bin/env python
#coding: utf-8

import logging
import sys

from ldap_settings import Settings
from ldap_group_sync import LdapGroupSync
from ldap_user_sync import LdapUserSync

def run_ldap_sync(settings):
    if settings.enable_group_sync:
        LdapGroupSync(settings).start()

    if settings.enable_user_sync:
        LdapUserSync(settings).start()

if __name__ == '__main__':
    kw = {
        'format': '[%(asctime)s] [%(levelname)s] %(message)s',
        'datefmt': '%m/%d/%Y %H:%M:%S',
        'level': logging.DEBUG,
        'stream': sys.stdout
    }

    logging.basicConfig(**kw)

    run_ldap_sync(Settings())
