#coding: utf-8

import logging
from threading import Thread, Event
from seafevents.ldap_syncer import Settings, LdapConn, uuid_bytes_to_str
from seaserv import get_group_dn_pairs, add_group_uuid_pair
from ldap import SCOPE_BASE

def migrate_dn_pairs(settings):
    grp_dn_pairs = get_group_dn_pairs()
    if grp_dn_pairs is None:
        logger.warning('get group dn pairs from db failed when migrate dn pairs.')
        return

    grp_dn_pairs.reverse()
    for grp_dn_pair in grp_dn_pairs:
        for config in settings.ldap_configs:
            search_filter = '(objectClass=*)'
            ldap_conn = LdapConn(config.host, config.user_dn, config.passwd, config.follow_referrals)
            ldap_conn.create_conn()
            if not ldap_conn.conn:
                logger.warning('connect ldap server [%s] failed.' % config.user_dn)
                return

            if config.use_page_result:
                results = ldap_conn.paged_search(grp_dn_pair.dn, SCOPE_BASE,
                                                 search_filter,
                                                 [config.group_uuid_attr])
            else:
                results = ldap_conn.search(grp_dn_pair.dn, SCOPE_BASE,
                                           search_filter,
                                           [config.group_uuid_attr])
            ldap_conn.unbind_conn()

            if not results:
                continue
            else:
                attrs = results[0][1]
                uuid = uuid_bytes_to_str (attrs[config.group_uuid_attr][0])
                add_group_uuid_pair (grp_dn_pair.group_id, uuid)
                break

class LdapSyncer(object):
    def __init__(self):
        self.settings = Settings()

    def enable_sync(self):
        return self.settings.enable_sync()

    def start(self):
        if self.settings.enable_group_sync:
            migrate_dn_pairs (self.settings)
        logging.info("Starting ldap sync.")
        LdapSyncTimer(self.settings).start()

class LdapSyncTimer(Thread):
    def __init__(self, settings):
        Thread.__init__(self)
        self.settings = settings
        self.fininsh = Event()

    def run(self):
        from seafevents.ldap_syncer.run_ldap_sync import run_ldap_sync
        while not self.fininsh.is_set():
            self.fininsh.wait(self.settings.sync_interval*60)
            if not self.fininsh.is_set():
                run_ldap_sync(self.settings)

    def cancel(self):
        self.fininsh.set()
