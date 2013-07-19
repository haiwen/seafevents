import os
import logging

from ccnet.async import Timer
from .utils import get_python_executable, run

__all__ = [
    'SendSeahubEmailTimer',
]

class SendSeahubEmailTimer(Timer):
    def __init__(self, ev_base, timeout, seahub_email_conf):
        Timer.__init__(self, ev_base, timeout)
        self._seahubdir = seahub_email_conf['seahubdir']

    def callback(self):
        self.send_seahub_email()

    def _send_seahub_email(self):
        manage_py = os.path.join(self._seahubdir, 'manage.py')
        cmd = [
            get_python_executable(),
            manage_py,
            'send_user_messages',
        ]
        run(cmd, cwd=self._seahubdir)

    def send_seahub_email(self):
        '''Send seahub user notification emails'''
        logging.info('starts to send email')
        try:
            self._send_seahub_email()
        except Exception:
            logging.exception('error when send email:')
