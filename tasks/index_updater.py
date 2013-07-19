#coding: UTF-8

import os
import logging
import ConfigParser

from ccnet.async import Timer
from seafevents.utils import get_python_executable, run 
from seafevents.utils.config import parse_bool, parse_interval, get_opt_from_conf_or_env

__all__ = [
    'IndexUpdater',
]

class IndexUpdater(object):
    def __init__(self, config):
        self._enabled = False

        self._seafesdir = None
        self._interval = None
        self._index_office_pdf = None
        self._logfile = None

        self._timer = None

        self._parse_config(config)

    def _parse_config(self, config):
        '''Parse index update related parts of events.conf'''
        section_name = 'INDEX FILES'
        key_enabled = 'enabled'
        key_seafesdir = 'seafesdir'
        key_logfile = 'logfile'
        key_index_interval = 'interval'
        key_index_office_pdf = 'index_office_pdf'

        default_index_interval = 30 * 60 # 30 min

        if not config.has_section(section_name):
            return

        # [ enabled ]
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)
        logging.debug('seafes enabled: %s', enabled)

        if not enabled:
            return

        self._enabled = True

        # [ seafesdir ]
        seafesdir = get_opt_from_conf_or_env(config, section_name, key_seafesdir, 'SEAFES_DIR', None)
        if not seafesdir:
            logging.critical('seafesdir is not set')
            raise RuntimeError('seafesdir is not set')
        if not os.path.exists(seafesdir):
            logging.critical('seafesdir %s does not exist' % seafesdir)
            raise RuntimeError('seafesdir is not set')

        # [ index logfile ]

        # default index file is 'index.log' in the seafes dir
        default_logfile = os.path.join(seafesdir, 'index.log')
        logfile = get_opt_from_conf_or_env (config, section_name,
                                            key_logfile,
                                            'SEAFES_LOGFILE',
                                            default=default_logfile)

        # [ index interval ]
        interval = get_opt_from_conf_or_env(config, section_name, key_index_interval,
                                               default=default_index_interval).lower()
        val = parse_interval(interval, default_index_interval)

        # [ index office/pdf files  ]
        index_office_pdf = False
        try:
            index_office_pdf = config.get(section_name, key_index_office_pdf)
        except ConfigParser.NoOptionError, ConfigParser.NoSectionError:
            pass
        else:
            index_office_pdf = index_office_pdf.lower()
            if index_office_pdf == 'true' or index_office_pdf == '1':
                index_office_pdf = True

        logging.debug('seafes dir: %s', seafesdir)
        logging.debug('seafes logfile: %s', logfile)
        logging.debug('seafes index interval: %s sec', interval)
        logging.debug('seafes index office/pdf: %s', index_office_pdf)

        self._seafesdir = seafesdir
        self._interval = val
        self._index_office_pdf = index_office_pdf
        self._logfile = logfile

    def start(self, ev_base):
        if not self.is_enabled():
            logging.warning('Can not start index updater: it is not enabled!')
            return

        logging.info('search indexer is started, interval = %s sec', self._interval)
        self._timer = IndexUpdateTimer(ev_base, self._interval,
                                       self._seafesdir, self._index_office_pdf, self._logfile)

    def is_enabled(self):
        return self._enabled

class IndexUpdateTimer(Timer):
    _script_name = 'update_repos.py'
    def __init__(self, ev_base, timeout, seafesdir, index_office_pdf, logfile):
        Timer.__init__(self, ev_base, timeout)
        self._seafesdir = seafesdir
        self._index_office_pdf = index_office_pdf
        self._logfile = logfile
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
        # python update_repos.py --logfile ./index.log --loglevel debug update
        cmd = [
            get_python_executable(),
            script_path,
            '--logfile', self._logfile,
            '--loglevel', self._loglevel,
            'update',
        ]

        env = dict(os.environ)
        if self._index_office_pdf:
            env['SEAFES_INDEX_OFFICE_PDF'] = 'true'

        run(cmd, cwd=self._seafesdir, env=env)