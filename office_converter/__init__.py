import os
import re
import logging
import subprocess

from .doctypes import DOC_TYPES, PPT_TYPES, EXCEL_TYPES
from seafevents.utils import has_office_tools
from seafevents.utils import run, get_python_executable

__all__ = [
    'OfficeConverter',
]

FILE_ID_PATTERN = re.compile(r'^[0-9a-f]{40}$')
logger = logging.getLogger(__name__)


class OfficeConverter(object):

    def __init__(self, conf):
        self._enabled = conf['enabled']

        if self._enabled:
            self._outputdir = conf['outputdir']
            self._num_workers = conf['workers']
            self._max_size = conf['max_size']
            self._max_pages = conf['max_pages']
            self._convert_host = conf['host']
            self._convert_port = conf['port']
            self._logfile = os.path.join(os.path.dirname(__file__), 'conver_server.log')
            self._convert_server_proc = None
            self._convert_server_py = os.path.join(os.path.dirname(__file__), 'convert_server.py')

    def start(self):
        convert_server_args = [
            get_python_executable(),
            self._convert_server_py,
            '--outputdir',
            self._outputdir,
            '--workers',
            self._num_workers,
            '--max_pages',
            self._max_pages,
            '--host',
            self._convert_host,
            '--port',
            self._convert_port,
        ]

        with open(self._logfile, 'a') as fp:
            self._convert_server_proc = run(
                convert_server_args, cwd=os.path.dirname(__file__), output=fp
            )

        exists_args = ["ps", "-ef"]
        result = run(exists_args, output=subprocess.PIPE)
        rows = result.stdout.read().decode('utf-8')
        if self._convert_server_py in rows:
            logging.info('http server process already start.')
        else:
            logging.warning('Failed to running http server process.')

        logging.info('office converter started')

    def stop(self):
        if self._convert_server_proc:
            try:
                self._convert_server_proc.terminate()
            except Exception as e:
                logger.error(e)
                pass
        logging.info('stop converter http server...')

    def is_enabled(self):
        if not has_office_tools():
            return False
        return self._enabled
