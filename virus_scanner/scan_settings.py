#coding: utf-8

import os
import logging
from ConfigParser import ConfigParser
from seafevents.db import init_db_session_class
from sqlalchemy.ext.declarative import declarative_base

class Settings(object):
    def __init__(self, config_file):
        self.enable_scan = False
        self.scan_cmd = None
        self.vir_codes = None
        self.nonvir_codes = None
        self.scan_interval = 60

        # seafile db config
        self.sdb_host = None
        self.sdb_port = 3306
        self.sdb_user = None
        self.sdb_passwd = None
        self.sdb_name = None
        self.sdb_charset = 'utf8'

        self.session_cls = None

        self.parse_config(config_file)

    def parse_config(self, config_file):
        try:
            cfg = ConfigParser()
            if 'SEAFILE_CENTRAL_CONF_DIR' in os.environ:
                confdir = os.environ['SEAFILE_CENTRAL_CONF_DIR']
            else:
                confdir = os.environ['SEAFILE_CONF_DIR']
            seaf_conf = os.path.join(confdir, 'seafile.conf')
            cfg.read(seaf_conf)
        except Exception as e:
            logging.warning('Failed to read seafile config, disable virus scan.')
            return

        if not self.parse_scan_config(cfg, seaf_conf):
            return

        if not self.parse_sdb_config(cfg):
            return

        try:
            self.session_cls = init_db_session_class(config_file)
        except Exception as e:
            logging.warning('Failed to init db session class: %s', e)
            return

        self.enable_scan = True

    def parse_scan_config(self, cfg, seaf_conf):
        if cfg.has_option('virus_scan', 'scan_command'):
            self.scan_cmd = cfg.get('virus_scan', 'scan_command')
        if not self.scan_cmd:
            logging.info('[virus_scan] scan_command option is not found in %s, disable virus scan.' %
                         seaf_conf)
            return False

        vcode = None
        if cfg.has_option('virus_scan', 'virus_code'):
            vcode = cfg.get('virus_scan', 'virus_code')
        if not vcode:
            logging.info('virus_code is not set, disable virus scan.')
            return False

        nvcode = None
        if cfg.has_option('virus_scan', 'nonvirus_code'):
            nvcode = cfg.get('virus_scan', 'nonvirus_code')
        if not nvcode:
            logging.info('nonvirus_code is not set, disable virus scan.')
            return False

        vcodes = vcode.split(',')
        self.vir_codes = [code.strip() for code in vcodes if code]
        if len(self.vir_codes) == 0:
            logging.info('invalid virus_code format, disable virus scan.')
            return False

        nvcodes = nvcode.split(',')
        self.nonvir_codes = [code.strip() for code in nvcodes if code]
        if len(self.nonvir_codes) == 0:
            logging.info('invalid nonvirus_code format, disable virus scan.')
            return False

        if cfg.has_option('virus_scan', 'scan_interval'):
            try:
                self.scan_interval = cfg.getint('virus_scan', 'scan_interval')
            except ValueError:
                pass

        return True

    def parse_sdb_config(self, cfg):
        # seafile db config
        db_type = None
        if cfg.has_option('database', 'type'):
            db_type = cfg.get('database', 'type')
        if db_type != 'mysql':
            logging.info('Seafile does not use mysql db, disable virus scan.')
            return False

        if cfg.has_option('database', 'host'):
            self.sdb_host = cfg.get('database', 'host')
        if not self.sdb_host:
            logging.info('mysql db host is not set in seafile conf, disable virus scan.')
            return False

        if cfg.has_option('database', 'port'):
            try:
                self.sdb_port = cfg.getint('database', 'port')
            except ValueError:
                pass

        if cfg.has_option('database', 'user'):
            self.sdb_user = cfg.get('database', 'user')
        if not self.sdb_user:
            logging.info('mysql db user is not set in seafile conf, disable virus scan.')
            return False

        if cfg.has_option('database', 'password'):
            self.sdb_passwd = cfg.get('database', 'password')
        if not self.sdb_passwd:
            logging.info('mysql db password is not set in seafile conf, disable virus scan.')
            return False

        if cfg.has_option('database', 'db_name'):
            self.sdb_name = cfg.get('database', 'db_name')
        if not self.sdb_name:
            logging.info('mysql db name is not set in seafile conf, disable virus scan.')
            return False

        if cfg.has_option('database', 'CONNECTION_CHARSET'):
            self.sdb_charset = cfg.get('database', 'CONNECTION_CHARSET')
        if not self.sdb_charset:
            self.sdb_charset = 'utf8'

        return True

    def is_enabled(self):
        return self.enable_scan
