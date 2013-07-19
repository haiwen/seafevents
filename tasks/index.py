#coding: UTF-8

import os
import logging

from ccnet.async import Timer
from .utils import get_python_executable, run

__all__ = [
    'IndexUpdateTimer',
]

class IndexUpdateTimer(Timer):
    _script_name = 'update_repos.py'
    def __init__(self, ev_base, timeout, seafes_conf):
        Timer.__init__(self, ev_base, timeout)
        self._seafesdir = seafes_conf['seafesdir']
        self._index_office_pdf = seafes_conf['index_office_pdf']
        self._logfile = seafes_conf['logfile']
        self._loglevel = 'debug'

    def callback(self):
        self.index_files()

    def index_files(self):
        logging.info('starts to index files')
        try:
            self._update_file_index()
        except Exception:
            logging.exception('error when index files:')

    def _update_file_index(self):
        '''Invoking the update_repos.py, log to ./index.log'''
        assert os.path.exists(self._seafesdir)
        script_path = os.path.join(self._seafesdir, self._script_name)
        ##########
        # python update_repos.py --logfile ./index.log --loglevel debug update
        ##########
        cmd = [
            get_python_executable(),
            script_path,
            '--logfile', self._logfile,
            '--loglevel', self._loglevel,
            'update',
        ]

        def get_env():
            env = dict(os.environ)
            if self._index_office_pdf:
                env['SEAFES_INDEX_OFFICE_PDF'] = 'true'

            return env

        env = get_env()
        run(cmd, cwd=self._seafesdir, env=env)