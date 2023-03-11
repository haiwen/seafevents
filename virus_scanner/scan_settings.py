# coding: utf-8
import os
import logging

from seafevents.utils import get_opt_from_conf_or_env, parse_bool
from seafevents.db import init_db_session_class

logger = logging.getLogger('virus_scan')
logger.setLevel(logging.INFO)


class Settings(object):
    def __init__(self, config, seafile_config):
        self.enable_scan = False
        self.scan_cmd = None
        self.vir_codes = None
        self.nonvir_codes = None
        self.scan_interval = 60
        # default 20M
        self.scan_size_limit = 20
        self.scan_skip_ext = ['.bmp', '.gif', '.ico', '.png', '.jpg',
                              '.mp3', '.mp4', '.wav', '.avi', '.rmvb',
                              '.mkv']
        self.threads = 4
        self.enable_send_mail = False

        self.session_cls = None
        self.seaf_session_cls = None

        self.parse_config(config, seafile_config)

    def parse_config(self, config, seafile_config):
        if not self.parse_scan_config(seafile_config):
            return

        try:
            self.session_cls = init_db_session_class(config)
            self.seaf_session_cls = init_db_session_class(seafile_config, db='seafile')
        except Exception as e:
            logger.warning('Failed to init db session class: %s', e)
            return

        self.enable_scan = True

        self.parse_send_mail_config(config)

    def parse_scan_config(self, seafile_config):
        if seafile_config.has_option('virus_scan', 'scan_command'):
            self.scan_cmd = seafile_config.get('virus_scan', 'scan_command')
        if not self.scan_cmd:
            logger.info('[virus_scan] scan_command option is not found in seafile.conf, disable virus scan.')
            return False

        vcode = None
        if seafile_config.has_option('virus_scan', 'virus_code'):
            vcode = seafile_config.get('virus_scan', 'virus_code')
        if not vcode:
            logger.info('virus_code is not set, disable virus scan.')
            return False

        nvcode = None
        if seafile_config.has_option('virus_scan', 'nonvirus_code'):
            nvcode = seafile_config.get('virus_scan', 'nonvirus_code')
        if not nvcode:
            logger.info('nonvirus_code is not set, disable virus scan.')
            return False

        vcodes = vcode.split(',')
        self.vir_codes = [code.strip() for code in vcodes if code]
        if len(self.vir_codes) == 0:
            logger.info('invalid virus_code format, disable virus scan.')
            return False

        nvcodes = nvcode.split(',')
        self.nonvir_codes = [code.strip() for code in nvcodes if code]
        if len(self.nonvir_codes) == 0:
            logger.info('invalid nonvirus_code format, disable virus scan.')
            return False

        if seafile_config.has_option('virus_scan', 'scan_interval'):
            try:
                self.scan_interval = seafile_config.getint('virus_scan', 'scan_interval')
            except ValueError:
                pass

        if seafile_config.has_option('virus_scan', 'scan_size_limit'):
            try:
                # in M unit
                self.scan_size_limit = seafile_config.getint('virus_scan', 'scan_size_limit')
            except ValueError:
                pass

        if seafile_config.has_option('virus_scan', 'scan_skip_ext'):
            exts = seafile_config.get('virus_scan', 'scan_skip_ext').split(',')
            # .jpg, .mp3, .mp4 format
            exts = [ext.strip() for ext in exts if ext]
            self.scan_skip_ext = [ext.lower() for ext in exts
                                  if len(ext) > 1 and ext[0] == '.']

        if seafile_config.has_option('virus_scan', 'threads'):
            try:
                self.threads = seafile_config.getint('virus_scan', 'threads')
            except ValueError:
                pass

        return True

    def parse_send_mail_config(self, config):
        section_name = 'SEAHUB EMAIL'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            return

        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)
        if not enabled:
            return

        self.enable_send_mail = True

    def is_enabled(self):
        return self.enable_scan
