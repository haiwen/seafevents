# -*- coding: utf-8 -*-
import os
import sys
import logging

from ccnet.async import Timer
from seafevents.utils import get_python_executable, run
from seafevents.utils.config import parse_bool, parse_interval, get_opt_from_conf_or_env


__all__ = [
    'WorkWinxinNoticeSender',
]


class WorkWinxinNoticeSender(object):

    def __init__(self, config):
        self._enabled = False
        self._interval = None
        self._seahub_dir = None
        self._logfile = None
        self._timer = None

        self._parse_config(config)
        self._prepare_logfile()

    def _prepare_logfile(self):
        log_dir = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''))
        self._logfile = os.path.join(log_dir, 'work_weixin_notice_sender.log')

    def _parse_config(self, config):
        """parse work weixin related options from config file
        """
        section_name = 'WORK WEIXIN'
        key_interval = 'interval'
        default_interval = 60  # 1min

        # seahub_dir
        seahub_dir = os.environ.get('SEAHUB_DIR', '')

        if not seahub_dir:
            logging.critical('seahub_dir is not set')
            raise RuntimeError('seahub_dir is not set')
        if not os.path.exists(seahub_dir):
            logging.critical('seahub_dir %s does not exist' % seahub_dir)
            raise RuntimeError('seahub_dir does not exist')

        # enabled
        sys.path.insert(0, seahub_dir)
        try:
            from seahub.settings import ENABLE_WORK_WEIXIN

            enabled = ENABLE_WORK_WEIXIN
            enabled = parse_bool(enabled)
        except ImportError as e:
            logging.warning('Can not import seahub.settings: %s.' % e)
            enabled = False

        if not enabled:
            return
        self._enabled = True

        # notice send interval
        if config.has_section(section_name):
            interval = get_opt_from_conf_or_env(config, section_name, key_interval,
                                                default=default_interval).lower()
            interval = parse_interval(interval, default_interval)
        else:
            interval = default_interval

        logging.info('work weixin notice send interval: %s sec', interval)

        self._interval = interval
        self._seahub_dir = seahub_dir

    def start(self, ev_base):
        if not self.is_enabled():
            logging.warning('Can not start work weixin notice sender: it is not enabled!')
            return

        logging.info('Start work weixin notice sender, interval = %s sec', self._interval)

        self._timer = WorkWeixinNoticeSenderTimer(ev_base, self._interval,
                                                  self._seahub_dir, self._logfile)

    def is_enabled(self):
        return self._enabled


class WorkWeixinNoticeSenderTimer(Timer):

    def __init__(self, ev_base, timeout, seahub_dir, logfile):
        Timer.__init__(self, ev_base, timeout)
        self._seahub_dir = seahub_dir
        self._logfile = logfile

    def callback(self):
        self.send_work_weixin_notifications()

    def send_work_weixin_notifications(self):
        """send user notifications to work weixin
        """
        logging.info('Start to send work weixin notifications..')

        try:
            self._send_work_weixin_notifications()
        except Exception as e:
            logging.exception(e)

    def _send_work_weixin_notifications(self):
        python_exec = get_python_executable()
        manage_py = os.path.join(self._seahub_dir, 'manage.py')

        cmd = [
            python_exec,
            manage_py,
            'send_work_weixin_notifications',
        ]

        with open(self._logfile, 'a') as fp:
            run(cmd, cwd=self._seahub_dir, output=fp)
