import os
import logging
import datetime
from ConfigParser import ConfigParser
from seafevents.db import init_db_session_class
from sqlalchemy.ext.declarative import declarative_base
from seafevents.utils.config import get_opt_from_conf_or_env, parse_bool

class Settings(object):
    def __init__(self, config_file):
        self.enable_count = False
        self.enable_storage_count = True
        self.enable_activity_count = True
        self.enable_audit_count = True

        self.session_cls = None

        self.parse_configs(config_file)
        # This is a throwaway variable to deal with a python bug, a dummy call
        throwaway = datetime.datetime.strptime('20110101','%Y%m%d')

    def parse_configs(self, config_file):
        
        try:
            self.session_cls = init_db_session_class(config_file)
        except Exception as e:
            logging.warning('Failed to init db session class: %s', e)
            return

        cfg = ConfigParser()
        cfg.read(config_file)
        if cfg.has_option('data_count', 'enable_count'):
            self.enable_count = cfg.get('data_count', 'enable_count')

        if cfg.has_option('data_count', 'enable_storage_count'):
            self.enable_storage_count = cfg.get('data_count', 'enable_storage_count')

        if cfg.has_option('data_count', 'enable_activity_count'):
            self.enable_activity_count = cfg.get('data_count', 'enable_activity_count')

        if cfg.has_option('data_count', 'enable_audit_count'):
            self.enable_audit_count = cfg.get('data_count', 'enable_audit_count')

    def init_seahub_db(self):
        try:
            import MySQLdb
            import seahub_settings
        except ImportError as e:
            logging.warning('Failed to init seahub db: %s.' %  e)  
            return

        try:
            db_infos = seahub_settings.DATABASES['default']
        except KeyError as e:
            logging.warning('Failed to init seahub db, can not find db info in seahub settings.')
            return

        if db_infos.get('ENGINE') != 'django.db.backends.mysql':
            logging.warning('Failed to init seahub db, only mysql db supported.')
            return

        db_host = db_infos.get('HOST', '127.0.0.1')
        db_port = int(db_infos.get('PORT', '3306'))
        db_name = db_infos.get('NAME')
        if not db_name:
            logging.warning('Failed to init seahub db, db name is not setted.')
            return
        db_user = db_infos.get('USER')
        if not db_user:
            logging.warning('Failed to init seahub db, db user is not setted.')
            return
        db_passwd = db_infos.get('PASSWORD')

        try:
            self.db_conn = MySQLdb.connect(host=db_host, port=db_port,
                                           user=db_user, passwd=db_passwd,
                                           db=db_name, charset='utf8')
            self.db_conn.autocommit(True)
            self.seahub_cursor = self.db_conn.cursor()
        except Exception as e:
            logging.warning('Failed to init seahub db: %s.' %  e)


    def close_seahub_db(self):
        if self.cursor:
            self.cursor.close()
        if self.db_conn:
            self.db_conn.close()

    def init_seafile_db(self):
        try:
            cfg = ConfigParser()
            if 'SEAFILE_CENTRAL_CONF_DIR' in os.environ:
                confdir = os.environ['SEAFILE_CENTRAL_CONF_DIR']
            else:
                confdir = os.environ['SEAFILE_CONF_DIR']
            seaf_conf = os.path.join(confdir, 'seafile.conf')
            cfg.read(seaf_conf)
        except Exception as e:
            logging.warning('Failed to read seafile config, disable statistics.')
            return

        db_type = None
        sdb_host = None
        sdb_port = None
        sdb_user = None
        sdb_passwd = None
        sdb_name = None
        sdb_charset = None
        if cfg.has_option('database', 'type'):
            db_type = cfg.get('database', 'type')
        if db_type != 'mysql':
            logging.info('Seafile does not use mysql db, disable statistics.')
            return False

        if cfg.has_option('database', 'host'):
            sdb_host = cfg.get('database', 'host')
        if not sdb_host:
            logging.info('mysql db host is not set in seafile conf, disable statistics.')
            return False

        if cfg.has_option('database', 'port'):
            try:
                sdb_port = cfg.getint('database', 'port')
            except ValueError:
                pass

        if cfg.has_option('database', 'user'):
            sdb_user = cfg.get('database', 'user')
        if not sdb_user:
            logging.info('mysql db user is not set in seafile conf, disable statistics.')
            return False

        if cfg.has_option('database', 'password'):
            sdb_passwd = cfg.get('database', 'password')
        if not sdb_passwd:
            logging.info('mysql db password is not set in seafile conf, disable statistics.')
            return False

        if cfg.has_option('database', 'db_name'):
            sdb_name = cfg.get('database', 'db_name')
        if not sdb_name:
            logging.info('mysql db name is not set in seafile conf, disable statistics.')
            return False

        if cfg.has_option('database', 'CONNECTION_CHARSET'):
            sdb_charset = cfg.get('database', 'CONNECTION_CHARSET')
        if not sdb_charset:
            sdb_charset = 'utf8'

        import MySQLdb

        self.sdb_conn = MySQLdb.connect(host=sdb_host, port=sdb_port,
                                        user=sdb_user, passwd=sdb_passwd,
                                        db=sdb_name, charset=sdb_charset)
        self.seafile_cursor = self.sdb_conn.cursor()
