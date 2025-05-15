import os
import logging
from threading import Thread, Event

from seafevents.app.config import SEAHUB_DIR
from seafevents.utils import get_python_executable, run

__all__ = [
    'QuotaAlertEmailSender',
]


class QuotaAlertEmailSender(object):
    def __init__(self):
        self._enabled = True
        self._interval = 60 * 60 * 24
        self._logfile = None
        self._timer = None

    def start(self):

        logging.info('seahub quota alert email sender is started, interval = %s sec', self._interval)
        SendQuotaAlertEmailTimer(self._interval).start()

    def is_enabled(self):
        return self._enabled


class SendQuotaAlertEmailTimer(Thread):

    def __init__(self, interval):
        Thread.__init__(self)
        self._interval = interval
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('starts to send quota alert email notice')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(SEAHUB_DIR, 'manage.py')

                    cmd = [
                        python_exec,
                        manage_py,
                        'check_user_quota',
                        '--auto', 'true',
                    ]
                    run(cmd, cwd=SEAHUB_DIR)
                except Exception as e:
                    logging.exception('error when send email for quota alert notice: %s', e)

    def cancel(self):
        self.finished.set()
