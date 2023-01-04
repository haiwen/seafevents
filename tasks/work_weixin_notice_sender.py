# -*- coding: utf-8 -*-
import os
import logging
from threading import Thread, Event

from seafevents.utils import get_python_executable, run, parse_bool, parse_interval, get_opt_from_conf_or_env
from seafevents.app.config import ENABLE_WORK_WEIXIN, SEAHUB_DIR


__all__ = [
    'WorkWinxinNoticeSender',
]


class WorkWinxinNoticeSender(object):

    def __init__(self, config):
        self._enabled = False
        self._interval = None
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

        # enabled
        enabled = ENABLE_WORK_WEIXIN
        enabled = parse_bool(enabled)

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

        self._interval = interval

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start work weixin notice sender: it is not enabled!')
            return

        logging.info('Start work weixin notice sender, interval = %s sec', self._interval)

        WorkWeixinNoticeSenderTimer(self._interval, self._logfile).start()

    def is_enabled(self):
        return self._enabled


class WorkWeixinNoticeSenderTimer(Thread):

    def __init__(self, interval, logfile):
        Thread.__init__(self)
        self._interval = interval
        self._logfile = logfile
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('Start to send work weixin notifications..')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(SEAHUB_DIR, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'send_notices_to_social_account',
                    ]

                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=SEAHUB_DIR, output=fp)
                except Exception as e:
                    logging.exception('send work weixin notifications error: %s', e)

    def cancel(self):
        self.finished.set()
