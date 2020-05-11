#coding: utf-8
import logging
from threading import Thread

from ldap import SCOPE_BASE
from seafevents.ldap_syncer.ldap_conn import  LdapConn
from seafevents.ldap_syncer.utils import bytes2str, add_group_uuid_pair

from seaserv import get_group_dn_pairs


logger = logging.getLogger(__name__)


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
            results = bytes2str(results)

            if not results:
                continue
            else:
                uuid = results[0][1][config.group_uuid_attr][0]
                add_group_uuid_pair(grp_dn_pair.group_id, uuid)


class LdapSync(Thread):
    def __init__(self, settings):
        Thread.__init__(self)
        self.settings = settings

    def run(self):
        if self.settings.enable_group_sync:
            migrate_dn_pairs(settings=self.settings)
        self.start_sync()
        self.show_sync_result()

    def show_sync_result(self):
        pass

    def start_sync(self):
        data_ldap = self.get_data_from_ldap()
        if data_ldap is None:
            return

        data_db = self.get_data_from_db()
        if data_db is None:
            return

        self.sync_data(data_db, data_ldap)

    def get_data_from_db(self):
        return None

    def get_data_from_ldap(self):
        ret = {}

        for config in self.settings.ldap_configs:
            cur_ret = self.get_data_from_ldap_by_server(config)
            # If get data from one server failed, then the result is failed
            if cur_ret is None:
                return None
            for key in cur_ret.keys():
                if key not in ret:
                    ret[key] = cur_ret[key]
                    ret[key].config = config

        return ret

    def get_data_from_ldap_by_server(self, config):
        return None

    def sync_data(self, data_db, data_ldap):
        pass
