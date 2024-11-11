import unittest
import os
import logging

from sqlalchemy import text

from seafevents.ldap_syncer.ldap_settings import Settings
from seafevents.tests.conftest import get_db_session
from seafevents.tests.utils.events_test_helper import ChangeFilePathHandler, save_file_history
from seafevents.app.config import get_config

logger = logging.getLogger('ldap_sync_test')
logger.setLevel(logging.DEBUG)


class EventTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def query(self, db, sql, param, oneres):
        session = None
        try:
            session = get_db_session(db)
            sqltext = text(sql)
            if oneres:
                res = session.execute(sqltext, param).fetchone()
            else:
                res = session.execute(sqltext, param).fetchall()
            return res
        finally:
            if session:
                session.close()

    def exec_sql(self, db, sql, param):
        session = None
        try:
            session = get_db_session(db)
            sqltext = text(sql)
            res = session.execute(sqltext, param)
            session.commit()
            return res
        finally:
            session.close()

    def get_session(self):
        return get_db_session('TESTDB')


class LDAPSyncerTest(unittest.TestCase):
    def setUp(self):
        # read conf file
        config_file = os.path.join(os.environ['CCNET_CONF_DIR'], 'seafevents.conf')
        config = get_config(config_file)
        self.settings = Settings(config, is_test=True)

        self.test_base_dn = 'OU=test-tmp-base-ou,dc=seafile,dc=ren'

        # connect to seahub db
        self.init_seahub_db()


    def tearDown(self):
        self.close_seahub_db()

    def init_seahub_db(self):
        try:
            import pymysql
            pymysql.install_as_MySQLdb()
            import seahub_settings
        except ImportError as e:
            logger.warning('Failed to init seahub db: %s.' %  e)
            return

        try:
            db_infos = seahub_settings.DATABASES['default']
        except KeyError as e:
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
            logger.warning('Failed to init seahub db: %s.' %  e)

    def close_seahub_db(self):
        if self.cursor:
            self.cursor.close()
        if self.db_conn:
            self.db_conn.close()