import os
import logging

from ccnet.async import Timer
from seafevents.utils import get_python_executable, run
from seafevents.utils.config import parse_bool, parse_interval, get_opt_from_conf_or_env

__all__ = [
    'SeahubEmailSender',
]

class SeahubEmailSender(object):
    def __init__(self, config):
        self._enabled = False

        self._interval = None
        self._seahubdir = None
        self._logfile = None

        self._timer = None

        self._parse_config(config)
        self._prepare_logdir()

    def _prepare_logdir(self):
        logdir = os.path.join(os.environ.get('SEAFEVENTS_LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'seahub_email_sender.log')

    def _parse_config(self, config):
        '''Parse send email related options from events.conf'''
        section_name = 'SEAHUB EMAIL'
        key_enabled = 'enabled'
        key_seahubdir = 'seahubdir'

        key_interval = 'interval'
        default_interval = 30 * 60  # 30min

        if not config.has_section(section_name):
            return

        # [ enabled ]
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)
        logging.debug('seahub email enabled: %s', enabled)

        if not enabled:
            return

        self._enabled = True

        # [ seahubdir ]
        seahubdir = get_opt_from_conf_or_env(config, section_name, key_seahubdir, 'SEAHUB_DIR')
        if not seahubdir:
            logging.critical('seahubdir is not set')
            raise RuntimeError('seahubdir is not set')
        if not os.path.exists(seahubdir):
            logging.critical('seahubdir %s does not exist' % seahubdir)
            raise RuntimeError('seahubdir does not exist')

        logging.debug('seahub dir: %s', seahubdir)

        # [ send email interval ]
        interval = get_opt_from_conf_or_env(config, section_name, key_interval,
                                               default=default_interval).lower()
        interval = parse_interval(interval, default_interval)

        logging.debug('send seahub email interval: %s sec', interval)

        self._interval = interval
        self._seahubdir = seahubdir

    def start(self, ev_base):
        if not self.is_enabled():
            logging.warning('Can not start seahub email sender: it is not enabled!')
            return

        logging.info('seahub email sender is started, interval = %s sec', self._interval)
        self._timer = SendSeahubEmailTimer(ev_base, self._interval,
                                           self._seahubdir, self._logfile)

    def is_enabled(self):
        return self._enabled

class SendSeahubEmailTimer(Timer):
    def __init__(self, ev_base, timeout, seahubdir, logfile):
        Timer.__init__(self, ev_base, timeout)
        self._seahubdir = seahubdir
        self._logfile = logfile

    def callback(self):
        self.send_seahub_email()

    def _send_seahub_email(self):
        python_exec = get_python_executable()
        manage_py = os.path.join(self._seahubdir, 'manage.py')
        cmd = [
            python_exec,
            manage_py,
            'send_notices',
        ]
        with open(self._logfile, 'a') as fp:
            run(cmd, cwd=self._seahubdir, output=fp)

        cmd = [
            python_exec,
            manage_py,
            'send_queued_mail',
        ]
        with open(self._logfile, 'a') as fp:
            run(cmd, cwd=self._seahubdir, output=fp)

    def send_seahub_email(self):
        '''Send seahub user notification emails'''
        logging.info('starts to send email')
        try:
            self._send_seahub_email()
        except Exception:
            logging.exception('error when send email:')
