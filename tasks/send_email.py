import os
import gevent
import logging

from .utils import get_python_executable, run

def _send_seahub_email(seahubdir):
    manage_py = os.path.join(seahubdir, 'manage.py')
    cmd = [
        get_python_executable(),
        manage_py,
        'send_user_messages',
    ]
    run(cmd, cwd=seahubdir)

def send_seahub_email(conf):
    '''Send seahub user notification emails'''
    interval = conf['interval']
    seahubdir = conf['seahubdir']
    logging.info('seahub email sender is started, interval = %s sec', interval)
    while True:
        gevent.sleep(interval)
        logging.info('starts to send email')
        try:
            _send_seahub_email(seahubdir)
        except Exception:
            logging.exception('error when send email:')
