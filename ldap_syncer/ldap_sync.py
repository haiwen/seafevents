# -*- coding: utf-8 -*-
import logging
from threading import Thread

from ldap import SCOPE_BASE
from seafevents.ldap_syncer.ldap_conn import LdapConn
from seafevents.ldap_syncer.utils import bytes2str, add_group_uuid_pair

from seaserv import get_group_dn_pairs

from seafevents.app.config import seahub_settings


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
                session = settings.db_session()
                add_group_uuid_pair(session, grp_dn_pair.group_id, uuid)


class LdapSync(Thread):
    def __init__(self, settings):
        Thread.__init__(self)
        self.settings = settings
        self.db_conn = None
        self.cursor = None
        self.init_seahub_db()

        if self.cursor is None:
            raise RuntimeError('Failed to init seahub db.')

    def init_seahub_db(self):
        try:
            import pymysql
            pymysql.install_as_MySQLdb()
        except ImportError as e:
            logger.warning('Failed to init seahub db: %s.' % e)
            return

        try:
            db_infos = seahub_settings.DATABASES['default']
        except KeyError:
            logger.warning('Failed to init seahub db, can not find db info in seahub settings.')
            return

        if db_infos.get('ENGINE') != 'django.db.backends.mysql':
            logger.warning('Failed to init seahub db, only mysql db supported.')
            return

        db_host = db_infos.get('HOST', '127.0.0.1')
        db_port = int(db_infos.get('PORT', '3306'))
        db_name = db_infos.get('NAME')
        if not db_name:
            logger.warning('Failed to init seahub db, db name is not setted.')
            return
        db_user = db_infos.get('USER')
        if not db_user:
            logger.warning('Failed to init seahub db, db user is not setted.')
            return
        db_passwd = db_infos.get('PASSWORD')

        try:
            self.db_conn = pymysql.connect(host=db_host, port=db_port,
                                           user=db_user, passwd=db_passwd,
                                           db=db_name, charset='utf8')
            self.db_conn.autocommit(True)
            self.cursor = self.db_conn.cursor()
        except Exception as e:
            self.cursor = None
            logger.warning('Failed to init seahub db: %s.' % e)
            return

    def close_seahub_db(self):
        if self.cursor:
            self.cursor.close()
        if self.db_conn:
            self.db_conn.close()

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
